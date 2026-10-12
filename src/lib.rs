//! Colas ordenadas por grupo sobre Redis.
//!
//! Cada grupo (una persona, una cita, un tenant) tiene su propia fila. Dentro de un grupo los mensajes
//! se procesan de uno en uno y en orden; grupos distintos van en paralelo. Un mensaje que falla solo
//! detiene a su grupo. Un mensaje sale de su fila únicamente cuando se confirma: al menos una vez.
//!
//! Cada operación es un script Lua, que Redis ejecuta sin interrupciones.

mod worker;

use std::sync::LazyLock;
use std::time::Duration;

use redis::Script;
use redis::aio::MultiplexedConnection;
use uuid::Uuid;

pub use worker::{Handler, HandlerError, WorkerConfig, run_worker};

#[derive(Debug)]
pub enum Error {
    Redis(redis::RedisError),
    Config(&'static str),
    /// Un mensaje en Redis sin el formato que escribe `push`.
    BadMessage,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Redis(e) => write!(f, "redis: {e}"),
            Self::Config(m) => write!(f, "configuración inválida: {m}"),
            Self::BadMessage => write!(f, "mensaje con formato inválido"),
        }
    }
}

impl std::error::Error for Error {}

impl From<redis::RedisError> for Error {
    fn from(e: redis::RedisError) -> Self {
        Self::Redis(e)
    }
}

static PUSH: LazyLock<Script> = LazyLock::new(|| Script::new(include_str!("lua/push.lua")));
static TAKE: LazyLock<Script> = LazyLock::new(|| Script::new(include_str!("lua/take.lua")));
static ACK: LazyLock<Script> = LazyLock::new(|| Script::new(include_str!("lua/ack.lua")));
static RETRY: LazyLock<Script> = LazyLock::new(|| Script::new(include_str!("lua/retry.lua")));
static BURY: LazyLock<Script> = LazyLock::new(|| Script::new(include_str!("lua/bury.lua")));
static EXTEND: LazyLock<Script> = LazyLock::new(|| Script::new(include_str!("lua/extend.lua")));
static MAINTAIN: LazyLock<Script> = LazyLock::new(|| Script::new(include_str!("lua/maintain.lua")));

/// Un mensaje en proceso. Su `token` identifica el turno: si el turno venció y otro trabajador tomó el
/// grupo, este token deja de valer y sus confirmaciones se rechazan.
#[derive(Debug)]
pub struct Taken {
    pub group: String,
    pub id: Uuid,
    pub payload: Vec<u8>,
    /// Empieza en 1. Cuenta también los intentos de procesos que murieron con el mensaje en la mano.
    pub attempt: u32,
    token: String,
}

/// Una cola. Barata de clonar: comparte la conexión.
#[derive(Clone)]
pub struct Fila {
    conn: MultiplexedConnection,
    /// Todas las claves de la cola llevan `{cola}`: en Redis Cluster caen en el mismo nodo, y los scripts
    /// pueden tocarlas juntas.
    base: String,
}

impl Fila {
    /// Abre la cola `queue`. Se niega si Redis no escribe a disco antes de responder o puede expulsar claves.
    pub async fn open(client: &redis::Client, prefix: &str, queue: &str) -> Result<Self, Error> {
        let cfg = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(Some(Duration::from_secs(5)))
            .set_response_timeout(Some(Duration::from_secs(10)));
        let mut conn = client
            .get_multiplexed_async_connection_with_config(&cfg)
            .await?;
        check_durable(&mut conn).await?;
        Ok(Self {
            conn,
            base: format!("{prefix}{{{queue}}}"),
        })
    }

    fn key(&self, name: &str) -> String {
        format!("{}:{name}", self.base)
    }

    fn group_prefix(&self) -> String {
        format!("{}:g:", self.base)
    }

    fn group_key(&self, group: &str) -> String {
        format!("{}{group}", self.group_prefix())
    }

    /// Añade al final de la fila de `group`. El `id` lo da quien produce: si reintenta un `push` que
    /// falló a medias, el duplicado lleva el mismo `id` y el manejador lo reconoce.
    pub async fn push(&self, group: &str, id: Uuid, payload: &[u8]) -> Result<(), Error> {
        let mut msg = Vec::with_capacity(16 + payload.len());
        msg.extend_from_slice(id.as_bytes());
        msg.extend_from_slice(payload);
        PUSH.key(self.key("ready"))
            .key(self.key("busy"))
            .key(self.key("delay"))
            .key(self.group_key(group))
            .arg(group)
            .arg(msg)
            .invoke_async::<i64>(&mut self.conn.clone())
            .await?;
        Ok(())
    }

    /// Toma el turno del siguiente grupo con trabajo, durante `lease`. `None` si no hay ninguno libre.
    pub async fn take(&self, lease: Duration) -> Result<Option<Taken>, Error> {
        let token = Uuid::now_v7().to_string();
        let r: Option<(String, Vec<u8>, u32)> = TAKE
            .key(self.key("ready"))
            .key(self.key("busy"))
            .key(self.key("tok"))
            .key(self.key("att"))
            .arg(lease.as_millis() as u64)
            .arg(&token)
            .arg(self.group_prefix())
            .invoke_async(&mut self.conn.clone())
            .await?;
        let Some((group, msg, attempt)) = r else {
            return Ok(None);
        };
        if msg.len() < 16 {
            return Err(Error::BadMessage);
        }
        let id = Uuid::from_slice(&msg[..16]).map_err(|_| Error::BadMessage)?;
        Ok(Some(Taken {
            group,
            id,
            payload: msg[16..].to_vec(),
            attempt,
            token,
        }))
    }

    /// Confirma el mensaje: sale de la fila. `false` si el turno ya no era de quien confirma.
    pub async fn ack(&self, t: &Taken) -> Result<bool, Error> {
        let n: i64 = ACK
            .key(self.key("busy"))
            .key(self.key("tok"))
            .key(self.key("ready"))
            .key(self.key("att"))
            .key(self.group_key(&t.group))
            .arg(&t.group)
            .arg(&t.token)
            .invoke_async(&mut self.conn.clone())
            .await?;
        Ok(n == 1)
    }

    /// Deja el mensaje en cabeza y aparta solo a su grupo durante `after`.
    pub async fn retry(&self, t: &Taken, after: Duration) -> Result<bool, Error> {
        let n: i64 = RETRY
            .key(self.key("busy"))
            .key(self.key("tok"))
            .key(self.key("delay"))
            .arg(&t.group)
            .arg(&t.token)
            .arg(after.as_millis() as u64)
            .invoke_async(&mut self.conn.clone())
            .await?;
        Ok(n == 1)
    }

    /// Mueve el mensaje a la DLQ (el stream `dead`) y el grupo sigue con el siguiente.
    pub async fn bury(&self, t: &Taken, error: &str) -> Result<bool, Error> {
        let n: i64 = BURY
            .key(self.key("busy"))
            .key(self.key("tok"))
            .key(self.key("ready"))
            .key(self.key("att"))
            .key(self.group_key(&t.group))
            .key(self.key("dead"))
            .arg(&t.group)
            .arg(&t.token)
            .arg(error)
            .invoke_async(&mut self.conn.clone())
            .await?;
        Ok(n == 1)
    }

    /// Alarga el turno. `false` si ya se perdió.
    pub async fn extend(&self, t: &Taken, lease: Duration) -> Result<bool, Error> {
        let n: i64 = EXTEND
            .key(self.key("busy"))
            .key(self.key("tok"))
            .arg(&t.group)
            .arg(&t.token)
            .arg(lease.as_millis() as u64)
            .invoke_async(&mut self.conn.clone())
            .await?;
        Ok(n == 1)
    }

    /// Devuelve a los turnos los grupos con la espera vencida y los de turnos abandonados.
    /// Devuelve (esperas vencidas, turnos recuperados).
    pub async fn maintain(&self, limit: u32) -> Result<(u64, u64), Error> {
        Ok(MAINTAIN
            .key(self.key("ready"))
            .key(self.key("busy"))
            .key(self.key("tok"))
            .key(self.key("delay"))
            .arg(limit)
            .invoke_async(&mut self.conn.clone())
            .await?)
    }

    /// Nombre del stream de la DLQ, para inspeccionarlo.
    pub fn dead_key(&self) -> String {
        self.key("dead")
    }
}

/// Una vez confirmado un `push`, Redis es la única copia del mensaje: no puede responder antes de
/// escribir a disco ni expulsar claves por memoria.
async fn check_durable(conn: &mut MultiplexedConnection) -> Result<(), Error> {
    let mut get = async |k: &str| -> Result<String, Error> {
        let (_, v): (String, String) = redis::cmd("CONFIG")
            .arg("GET")
            .arg(k)
            .query_async(conn)
            .await?;
        Ok(v)
    };
    if get("appendonly").await? != "yes" || get("appendfsync").await? != "always" {
        return Err(Error::Config(
            "Redis debe tener appendonly yes y appendfsync always",
        ));
    }
    if get("maxmemory-policy").await? != "noeviction" {
        return Err(Error::Config(
            "Redis debe tener maxmemory-policy noeviction",
        ));
    }
    Ok(())
}
