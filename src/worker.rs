use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

use crate::{Error, Fila, Taken};

pub enum HandlerError {
    /// Fallo pasajero: el grupo espera según `retry_delays` y se reintenta el mismo mensaje.
    Retry(String),
    /// Fallo permanente: el mensaje va a la DLQ (y el grupo se detiene o sigue según `Options::on_dead`).
    Fatal(String),
}

/// Lo que recibe un manejador.
pub struct Job {
    pub group: String,
    pub id: Uuid,
    pub payload: Vec<u8>,
    pub attempt: u32,
    /// Número de turno, creciente en toda la cola. El destino guarda el último aplicado por grupo y
    /// rechaza escrituras con uno menor: así un manejador que perdió su turno no escribe fuera de orden.
    pub fence: u64,
    /// Se activa si el turno se pierde: otro trabajador ya tiene el grupo. El manejador debería parar.
    pub lost: CancellationToken,
}

pub trait Handler: Send + Sync + 'static {
    /// Puede ejecutarse más de una vez para el mismo `id` (si el proceso muere tras el efecto y antes de
    /// confirmar): los efectos deben ser idempotentes por `id` y protegidos por `fence`.
    fn handle(&self, job: Job) -> impl Future<Output = Result<(), HandlerError>> + Send;
}

/// Fallos inyectables para las pruebas. En producción no se tocan.
#[derive(Clone, Default)]
pub struct Faults {
    /// Las próximas N renovaciones de turno fallan como si Redis no respondiera.
    pub fail_extend: Arc<AtomicU32>,
}

pub struct WorkerConfig {
    /// Grupos atendidos a la vez por esta instancia.
    pub concurrency: usize,
    /// Duración de un turno. Se renueva cada tercio mientras el manejador corre.
    pub lease: Duration,
    /// Espera cuando no hay grupos libres.
    pub poll: Duration,
    pub maintain_every: Duration,
    /// Espera antes de cada reintento; agotada la lista, el mensaje va a la DLQ.
    pub retry_delays: Vec<Duration>,
    pub faults: Faults,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        let s = Duration::from_secs;
        Self {
            concurrency: 32,
            lease: s(30),
            poll: Duration::from_millis(50),
            maintain_every: Duration::from_millis(500),
            retry_delays: vec![s(1), s(10), s(30), s(60), s(300), s(900), s(3600)],
            faults: Faults::default(),
        }
    }
}

async fn sleep_or_stop(stop: &mut watch::Receiver<bool>, d: Duration) {
    tokio::select! {
        () = tokio::time::sleep(d) => {}
        _ = stop.changed() => {}
    }
}

fn panic_text(e: JoinError) -> String {
    match e.try_into_panic() {
        Ok(p) => p
            .downcast_ref::<&str>()
            .map(|s| (*s).to_string())
            .or_else(|| p.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "sin mensaje".into()),
        Err(_) => "tarea cancelada".into(),
    }
}

fn joined(r: Result<Result<(), Error>, JoinError>) -> Result<(), Error> {
    r.map_err(|e| Error::Panic(panic_text(e)))?
}

fn reap(inflight: &mut JoinSet<Result<(), Error>>) -> Result<(), Error> {
    while let Some(r) = inflight.try_join_next() {
        joined(r)?;
    }
    Ok(())
}

/// Atiende la cola hasta que `stop` valga `true`. Ante un fallo de Redis devuelve el error: el servicio
/// lo reinicia con espera; los turnos que tenía vencen y otro los retoma.
pub async fn run_worker<H: Handler>(
    fila: Fila,
    handler: H,
    cfg: WorkerConfig,
    mut stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    if cfg.concurrency == 0 {
        return Err(Error::Config("concurrency debe ser > 0"));
    }
    let slots = Arc::new(Semaphore::new(cfg.concurrency));
    let ctx = Arc::new((fila, handler, cfg));
    let mut inflight = JoinSet::new();
    let mut last_maintain = Instant::now() - ctx.2.maintain_every;
    let outcome = loop {
        if *stop.borrow() {
            break Ok(());
        }
        if let Err(e) = reap(&mut inflight) {
            break Err(e);
        }
        if last_maintain.elapsed() >= ctx.2.maintain_every {
            if let Err(e) = ctx.0.maintain(1000).await {
                break Err(e);
            }
            last_maintain = Instant::now();
        }
        let permit = tokio::select! {
            p = slots.clone().acquire_owned() => p.expect("el semáforo no se cierra"),
            _ = stop.changed() => break Ok(()),
        };
        match ctx.0.take(ctx.2.lease).await {
            Ok(Some(t)) => {
                inflight.spawn(process(ctx.clone(), t, permit));
            }
            Ok(None) => {
                drop(permit);
                sleep_or_stop(&mut stop, ctx.2.poll).await;
            }
            Err(e) => break Err(e),
        }
    };
    // Pase lo que pase, se espera a los manejadores en vuelo: cortarlos a medio efecto no aporta nada,
    // y si su confirmación falla, el mensaje solo se vuelve a entregar.
    let mut drained = Ok(());
    while let Some(r) = inflight.join_next().await {
        let r = joined(r);
        if drained.is_ok() {
            drained = r;
        }
    }
    outcome.and(drained)
}

fn take_fault(n: &AtomicU32) -> bool {
    n.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_sub(1))
        .is_ok()
}

async fn process<H: Handler>(
    ctx: Arc<(Fila, H, WorkerConfig)>,
    t: Taken,
    _permit: OwnedSemaphorePermit,
) -> Result<(), Error> {
    let (fila, _, cfg) = &*ctx;
    let max_attempts = cfg.retry_delays.len() as u32 + 1;
    // Más intentos que los previstos solo ocurre si el proceso murió con este mensaje en la mano una y
    // otra vez: probablemente es él quien lo tumba.
    if t.attempt > max_attempts {
        if !fila
            .bury(&t, "superó los intentos sin terminar (¿tumba al proceso?)")
            .await?
        {
            warn!(group = t.group, id = %t.id, "turno perdido al enterrar: otro trabajador tiene el grupo");
        }
        return Ok(());
    }

    let lost = CancellationToken::new();
    let job = Job {
        group: t.group.clone(),
        id: t.id,
        payload: t.payload.clone(),
        attempt: t.attempt,
        fence: t.fence,
        lost: lost.clone(),
    };
    // En su propia tarea: un pánico del manejador cuenta como un intento fallido, no tumba al trabajador.
    let mut work = tokio::spawn({
        let ctx = ctx.clone();
        async move { ctx.1.handle(job).await }
    });
    let mut renew = tokio::time::interval(cfg.lease / 3);
    renew.tick().await;
    let mut renewing = true;
    let joined = loop {
        tokio::select! {
            r = &mut work => break r,
            _ = renew.tick(), if renewing => {
                let r = if take_fault(&cfg.faults.fail_extend) {
                    Err(Error::Injected)
                } else {
                    fila.extend(&t, cfg.lease).await
                };
                match r {
                    Ok(true) => {}
                    // Perdido de verdad: otro trabajador tiene el grupo.
                    Ok(false) => {
                        renewing = false;
                        lost.cancel();
                        warn!(group = t.group, id = %t.id, "turno perdido durante el manejador");
                    }
                    // Pasajero: se insiste en el siguiente tick.
                    Err(e) => warn!(group = t.group, id = %t.id, error = %e, "no se pudo renovar el turno"),
                }
            }
        }
    };
    let result = joined.unwrap_or_else(|e| {
        let m = panic_text(e);
        warn!(group = t.group, id = %t.id, panic = m, "el manejador entró en pánico");
        Err(HandlerError::Retry(format!("pánico: {m}")))
    });

    let (confirmed, what) = match result {
        Ok(()) => (fila.ack(&t).await?, "confirmar"),
        Err(HandlerError::Retry(m)) => match cfg.retry_delays.get(t.attempt as usize - 1) {
            Some(&after) => (fila.retry(&t, after).await?, "reintentar"),
            None => (fila.bury(&t, &m).await?, "enterrar"),
        },
        Err(HandlerError::Fatal(m)) => (fila.bury(&t, &m).await?, "enterrar"),
    };
    if !confirmed {
        warn!(group = t.group, id = %t.id, accion = what, "turno perdido: el mensaje se procesará de nuevo");
    }
    Ok(())
}
