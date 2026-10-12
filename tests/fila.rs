//! Pruebas contra un Redis real y durable (`docker compose up -d`, puerto 56380).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use galenium_fila::*;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use uuid::Uuid;

fn client() -> redis::Client {
    let url = std::env::var("FILA_TEST_REDIS").unwrap_or("redis://localhost:56380".into());
    redis::Client::open(url).unwrap()
}

async fn fila() -> Fila {
    let prefix = format!("t{}:", Uuid::now_v7().simple());
    Fila::open(&client(), &prefix, "q")
        .await
        .expect("Redis no disponible o no durable: ejecuta `docker compose up -d`")
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

async fn push_n(f: &Fila, group: &str, n: u32) -> Vec<Uuid> {
    let mut ids = Vec::new();
    for i in 0..n {
        let id = Uuid::now_v7();
        f.push(group, id, &i.to_be_bytes()).await.unwrap();
        ids.push(id);
    }
    ids
}

/// Manejador de prueba: anota lo procesado y detecta si dos trabajadores tocan el mismo grupo a la vez.
#[derive(Clone, Default)]
struct Rec {
    log: Arc<Mutex<Vec<(String, u32, Uuid)>>>,
    busy: Arc<Mutex<HashSet<String>>>,
    overlap: Arc<AtomicBool>,
    /// Fallos pasajeros pendientes por (grupo, n).
    retry: Arc<Mutex<HashMap<(String, u32), u32>>>,
    fatal: Arc<Mutex<HashSet<(String, u32)>>>,
    /// Pausa por (grupo, n).
    slow: Arc<Mutex<HashMap<(String, u32), Duration>>>,
    calls: Arc<AtomicU32>,
}

impl Handler for Rec {
    async fn handle(
        &self,
        group: &str,
        id: Uuid,
        payload: &[u8],
        _attempt: u32,
    ) -> Result<(), HandlerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let n = u32::from_be_bytes(payload.try_into().unwrap());
        if !self.busy.lock().unwrap().insert(group.to_string()) {
            self.overlap.store(true, Ordering::SeqCst);
        }
        let pause = self
            .slow
            .lock()
            .unwrap()
            .get(&(group.to_string(), n))
            .copied();
        // Pausa pequeña y variable para mezclar los tiempos entre grupos.
        tokio::time::sleep(
            pause.unwrap_or(Duration::from_micros(u64::from(id.as_bytes()[15]) * 10)),
        )
        .await;
        let key = (group.to_string(), n);
        let result = if self.fatal.lock().unwrap().contains(&key) {
            Err(HandlerError::Fatal("veneno".into()))
        } else if let Some(left) = self.retry.lock().unwrap().get_mut(&key)
            && *left > 0
        {
            *left -= 1;
            Err(HandlerError::Retry("todavía no".into()))
        } else {
            self.log.lock().unwrap().push((group.to_string(), n, id));
            Ok(())
        };
        self.busy.lock().unwrap().remove(group);
        result
    }
}

impl Rec {
    fn seqs(&self, group: &str) -> Vec<u32> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|(g, ..)| g == group)
            .map(|(_, n, _)| *n)
            .collect()
    }
    fn len(&self) -> usize {
        self.log.lock().unwrap().len()
    }
}

type Running = (watch::Sender<bool>, JoinHandle<Result<(), Error>>);

fn worker(f: &Fila, h: Rec, cfg: WorkerConfig) -> Running {
    let (tx, rx) = watch::channel(false);
    (tx, tokio::spawn(run_worker(f.clone(), h, cfg, rx)))
}

fn cfg(concurrency: usize, delays_ms: &[u64]) -> WorkerConfig {
    WorkerConfig {
        concurrency,
        lease: ms(2000),
        poll: ms(5),
        maintain_every: ms(20),
        retry_delays: delays_ms.iter().map(|&d| ms(d)).collect(),
    }
}

async fn stop(ws: Vec<Running>) {
    for (tx, h) in ws {
        tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(10), h)
            .await
            .expect("el trabajador no paró a tiempo")
            .unwrap()
            .unwrap();
    }
}

async fn wait_until(what: &str, secs: u64, f: impl Fn() -> bool) {
    for _ in 0..secs * 40 {
        if f() {
            return;
        }
        tokio::time::sleep(ms(25)).await;
    }
    panic!("tiempo agotado esperando: {what}");
}

async fn dead(f: &Fila) -> Vec<HashMap<String, String>> {
    let mut c = client().get_multiplexed_async_connection().await.unwrap();
    type Entries = Vec<(String, Vec<(String, Vec<u8>)>)>;
    let r: Entries = redis::cmd("XRANGE")
        .arg(f.dead_key())
        .arg("-")
        .arg("+")
        .query_async(&mut c)
        .await
        .unwrap();
    r.into_iter()
        .map(|(_, kv)| {
            kv.into_iter()
                .map(|(k, v)| (k, String::from_utf8_lossy(&v).into_owned()))
                .collect()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_per_group_with_many_workers_and_never_two_on_one_group() {
    let f = fila().await;
    let groups: Vec<String> = (0..40).map(|i| format!("g{i}")).collect();
    for i in 0..25u32 {
        for g in &groups {
            f.push(g, Uuid::now_v7(), &i.to_be_bytes()).await.unwrap();
        }
    }
    let h = Rec::default();
    let ws: Vec<_> = (0..4).map(|_| worker(&f, h.clone(), cfg(8, &[]))).collect();
    wait_until("1000 procesados", 20, || h.len() == 1000).await;
    for g in &groups {
        assert_eq!(h.seqs(g), (0..25).collect::<Vec<_>>(), "orden roto en {g}");
    }
    assert!(
        !h.overlap.load(Ordering::SeqCst),
        "dos trabajadores tocaron el mismo grupo a la vez"
    );
    stop(ws).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_group_only_blocks_itself() {
    let f = fila().await;
    let h = Rec::default();
    h.retry.lock().unwrap().insert(("malo".into(), 0), 2);
    push_n(&f, "malo", 5).await;
    for i in 0..5 {
        push_n(&f, &format!("bueno{i}"), 10).await;
    }
    let ws = vec![worker(&f, h.clone(), cfg(4, &[500, 500]))];
    wait_until("todo procesado", 10, || h.len() == 55).await;

    assert_eq!(
        h.seqs("malo"),
        [0, 1, 2, 3, 4],
        "el grupo que falla conserva su orden"
    );
    let log = h.log.lock().unwrap().clone();
    let first_malo = log.iter().position(|(g, ..)| g == "malo").unwrap();
    let last_bueno = log
        .iter()
        .rposition(|(g, ..)| g.starts_with("bueno"))
        .unwrap();
    assert!(
        last_bueno < first_malo,
        "los demás grupos terminaron mientras `malo` esperaba sus reintentos"
    );
    stop(ws).await;
}

#[tokio::test]
async fn an_abandoned_turn_is_resumed_and_a_stale_ack_is_rejected() {
    let f = fila().await;
    let ids = push_n(&f, "a", 1).await;
    let t1 = f.take(ms(200)).await.unwrap().unwrap();
    assert_eq!((t1.id, t1.attempt), (ids[0], 1));
    assert!(
        f.take(ms(200)).await.unwrap().is_none(),
        "nadie más puede tomar el grupo mientras tiene dueño"
    );

    tokio::time::sleep(ms(300)).await;
    assert_eq!(
        f.maintain(100).await.unwrap(),
        (0, 1),
        "turno vencido recuperado"
    );
    let t2 = f.take(ms(5000)).await.unwrap().unwrap();
    assert_eq!(
        (t2.id, t2.attempt),
        (ids[0], 2),
        "mismo mensaje, segundo intento"
    );

    assert!(
        !f.ack(&t1).await.unwrap(),
        "el dueño anterior ya no puede confirmar"
    );
    assert!(f.ack(&t2).await.unwrap());
    assert!(
        f.take(ms(200)).await.unwrap().is_none(),
        "salió una sola vez"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poison_goes_to_dead_and_the_group_continues() {
    let f = fila().await;
    let h = Rec::default();
    h.retry.lock().unwrap().insert(("a".into(), 1), 1000);
    h.fatal.lock().unwrap().insert(("a".into(), 2));
    push_n(&f, "a", 4).await;
    let ws = vec![worker(&f, h.clone(), cfg(2, &[20, 20]))];
    wait_until("0 y 3 procesados", 10, || h.len() == 2).await;
    assert_eq!(h.seqs("a"), [0, 3]);
    let d = dead(&f).await;
    assert_eq!(d.len(), 2);
    assert_eq!(
        (d[0]["attempts"].as_str(), d[0]["error"].as_str()),
        ("3", "todavía no")
    );
    assert_eq!(
        (d[1]["attempts"].as_str(), d[1]["error"].as_str()),
        ("1", "veneno")
    );
    stop(ws).await;
}

#[tokio::test]
async fn a_message_that_keeps_killing_the_process_is_buried_without_running_again() {
    let f = fila().await;
    push_n(&f, "a", 2).await;
    // Tres "muertes" con el mensaje en la mano: se toma y se deja vencer el turno.
    for _ in 0..3 {
        f.take(ms(50)).await.unwrap().unwrap();
        tokio::time::sleep(ms(80)).await;
        f.maintain(100).await.unwrap();
    }
    let h = Rec::default();
    let ws = vec![worker(&f, h.clone(), cfg(1, &[20, 20]))]; // máximo 3 intentos
    wait_until("el siguiente procesado", 10, || h.len() == 1).await;
    assert_eq!(h.seqs("a"), [1]);
    assert_eq!(
        h.calls.load(Ordering::SeqCst),
        1,
        "el mensaje que tumba al proceso no se volvió a ejecutar"
    );
    assert_eq!(dead(&f).await[0]["attempts"], "4");
    stop(ws).await;
}

#[tokio::test]
async fn a_group_with_much_work_does_not_starve_the_others() {
    let f = fila().await;
    push_n(&f, "grande", 300).await;
    push_n(&f, "chico", 1).await;
    let h = Rec::default();
    let ws = vec![worker(&f, h.clone(), cfg(1, &[]))];
    wait_until("chico procesado", 10, || !h.seqs("chico").is_empty()).await;
    let pos = h
        .log
        .lock()
        .unwrap()
        .iter()
        .position(|(g, ..)| g == "chico")
        .unwrap();
    assert!(pos <= 2, "`chico` esperó {pos} mensajes de `grande`");
    stop(ws).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_long_handler_keeps_its_turn() {
    let f = fila().await;
    let h = Rec::default();
    h.slow.lock().unwrap().insert(("a".into(), 0), ms(1000));
    push_n(&f, "a", 2).await;
    let mut c = cfg(2, &[]);
    c.lease = ms(300); // el manejador dura más de tres turnos: debe renovarlo
    let mut c2 = cfg(2, &[]);
    c2.lease = ms(300);
    let ws = vec![worker(&f, h.clone(), c), worker(&f, h.clone(), c2)];
    wait_until("ambos procesados", 10, || h.len() == 2).await;
    tokio::time::sleep(ms(500)).await;
    assert_eq!(h.seqs("a"), [0, 1]);
    assert_eq!(
        h.calls.load(Ordering::SeqCst),
        2,
        "nadie más tomó el grupo durante el manejador largo"
    );
    assert!(!h.overlap.load(Ordering::SeqCst));
    stop(ws).await;
}

fn docker(args: &[&str]) -> bool {
    std::process::Command::new("timeout")
        .arg("20")
        .arg("docker")
        .args(args)
        .status()
        .is_ok_and(|s| s.success())
}

/// Arranca el contenedor al soltarse, también si la prueba entra en pánico: nunca deja Redis caído.
struct Revive(String);

impl Revive {
    fn start(&self) -> bool {
        // Justo después de un SIGKILL, Docker puede rechazar el `start` un momento.
        (0..20).any(|_| {
            docker(&["start", &self.0]) || {
                std::thread::sleep(ms(500));
                false
            }
        })
    }
}

impl Drop for Revive {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.start();
        }
    }
}

/// Mata Redis con SIGKILL en plena carga. Corre sola: `cargo test -- --ignored redis_killed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "mata el contenedor de Redis de pruebas"]
async fn redis_killed_mid_flight_loses_nothing_and_keeps_order() {
    let container = std::env::var("FILA_TEST_REDIS_CONTAINER").unwrap_or("fila-redis-1".into());
    let prefix = format!("t{}:", Uuid::now_v7().simple());
    let h = Rec::default();
    let (stop_tx, stop_rx) = watch::channel(false);

    // Trabajadores supervisados: si Redis cae, terminan con error y se reinician.
    let mut workers = Vec::new();
    for _ in 0..3 {
        let (h, prefix, mut stop) = (h.clone(), prefix.clone(), stop_rx.clone());
        workers.push(tokio::spawn(async move {
            while !*stop.borrow() {
                if let Ok(f) = Fila::open(&client(), &prefix, "q").await
                    && run_worker(f, h.clone(), cfg(8, &[20, 20, 20]), stop.clone())
                        .await
                        .is_ok()
                {
                    return;
                }
                tokio::select! { () = tokio::time::sleep(ms(200)) => {}, _ = stop.changed() => {} }
            }
        }));
    }

    // Productor: reintenta cada `push` con el mismo id hasta que entra.
    let groups: Vec<String> = (0..10).map(|i| format!("g{i}")).collect();
    let sent: Arc<Mutex<HashMap<String, Vec<Uuid>>>> = Arc::default();
    let producer = {
        let (groups, sent, prefix) = (groups.clone(), sent.clone(), prefix.clone());
        tokio::spawn(async move {
            for i in 0..40u32 {
                for g in &groups {
                    let id = Uuid::now_v7();
                    loop {
                        if let Ok(f) = Fila::open(&client(), &prefix, "q").await
                            && f.push(g, id, &i.to_be_bytes()).await.is_ok()
                        {
                            break;
                        }
                        tokio::time::sleep(ms(100)).await;
                    }
                    sent.lock().unwrap().entry(g.clone()).or_default().push(id);
                }
                tokio::time::sleep(ms(50)).await;
            }
        })
    };

    wait_until("algo procesado antes de matar Redis", 10, || h.len() >= 50).await;
    let revive = Revive(container.clone());
    assert!(
        !producer.is_finished(),
        "Redis debe morir con el productor todavía empujando"
    );
    assert!(
        docker(&["kill", "-s", "KILL", &container]),
        "no se pudo matar {container}"
    );
    tokio::time::sleep(ms(1500)).await;
    assert!(revive.start(), "no se pudo volver a arrancar {container}");

    tokio::time::timeout(Duration::from_secs(60), producer)
        .await
        .expect("el productor no terminó")
        .unwrap();
    let distinct = || {
        h.log
            .lock()
            .unwrap()
            .iter()
            .map(|(.., id)| *id)
            .collect::<HashSet<_>>()
            .len()
    };
    wait_until("los 400 mensajes", 30, || distinct() == 400).await;

    // Por grupo: los ids distintos, en el orden de su primera aparición, son exactamente los enviados.
    let log = h.log.lock().unwrap().clone();
    for g in &groups {
        let mut seen = HashSet::new();
        let got: Vec<Uuid> = log
            .iter()
            .filter(|(x, ..)| x == g)
            .map(|(.., id)| *id)
            .filter(|id| seen.insert(*id))
            .collect();
        assert_eq!(
            &got,
            &sent.lock().unwrap()[g],
            "grupo {g}: falta algo o se desordenó"
        );
    }
    assert!(!h.overlap.load(Ordering::SeqCst));

    stop_tx.send(true).unwrap();
    for w in workers {
        tokio::time::timeout(Duration::from_secs(10), w)
            .await
            .expect("un trabajador no paró")
            .unwrap();
    }
}
