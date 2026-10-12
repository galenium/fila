use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::{Error, Fila, Taken};

pub enum HandlerError {
    /// Fallo pasajero: el grupo espera según `retry_delays` y se reintenta el mismo mensaje.
    Retry(String),
    /// Fallo permanente: el mensaje va a la DLQ y el grupo sigue.
    Fatal(String),
}

pub trait Handler: Send + Sync + 'static {
    /// Puede ejecutarse más de una vez para el mismo `id` (si el proceso muere tras el efecto y antes
    /// de confirmar): los efectos deben ser idempotentes por `id`.
    fn handle(
        &self,
        group: &str,
        id: Uuid,
        payload: &[u8],
        attempt: u32,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send;
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
        }
    }
}

async fn sleep_or_stop(stop: &mut watch::Receiver<bool>, d: Duration) {
    tokio::select! {
        () = tokio::time::sleep(d) => {}
        _ = stop.changed() => {}
    }
}

fn reap(inflight: &mut JoinSet<Result<(), Error>>) -> Result<(), Error> {
    while let Some(r) = inflight.try_join_next() {
        r.map_err(|_| Error::Config("una tarea del trabajador entró en pánico"))??;
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
        let r = r
            .map_err(|_| Error::Config("una tarea del trabajador entró en pánico"))
            .and_then(|r| r);
        if drained.is_ok() {
            drained = r;
        }
    }
    outcome.and(drained)
}

async fn process<H: Handler>(
    ctx: Arc<(Fila, H, WorkerConfig)>,
    t: Taken,
    _permit: OwnedSemaphorePermit,
) -> Result<(), Error> {
    let (fila, handler, cfg) = &*ctx;
    let max_attempts = cfg.retry_delays.len() as u32 + 1;
    // Más intentos que los previstos solo ocurre si el proceso murió con este mensaje en la mano una y
    // otra vez: probablemente es él quien lo tumba.
    if t.attempt > max_attempts {
        fila.bury(&t, "superó los intentos sin terminar (¿tumba al proceso?)")
            .await?;
        return Ok(());
    }

    let work = handler.handle(&t.group, t.id, &t.payload, t.attempt);
    tokio::pin!(work);
    let mut renew = tokio::time::interval(cfg.lease / 3);
    renew.tick().await;
    let mut renewing = true;
    let result = loop {
        tokio::select! {
            r = &mut work => break r,
            _ = renew.tick(), if renewing => {
                // Si el turno se perdió (o Redis no responde), el manejador termina igualmente: su
                // confirmación se rechazará y el mensaje se procesará de nuevo (al menos una vez).
                renewing = matches!(fila.extend(&t, cfg.lease).await, Ok(true));
            }
        }
    };
    match result {
        Ok(()) => {
            fila.ack(&t).await?;
        }
        Err(HandlerError::Retry(m)) => match cfg.retry_delays.get(t.attempt as usize - 1) {
            Some(&after) => {
                fila.retry(&t, after).await?;
            }
            None => {
                fila.bury(&t, &m).await?;
            }
        },
        Err(HandlerError::Fatal(m)) => {
            fila.bury(&t, &m).await?;
        }
    }
    Ok(())
}
