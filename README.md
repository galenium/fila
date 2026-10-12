# galenium-fila

Colas ordenadas por grupo sobre Redis: la misma idea que los *message groups* de SQS FIFO, las
*sessions* de Azure Service Bus o *Key_Shared* de Pulsar.

Cada grupo (una persona, una cita, un tenant) tiene su propia fila:

- **Orden dentro del grupo**: sus mensajes se procesan de uno en uno, en el orden en que entraron.
- **Paralelismo entre grupos**: grupos distintos van a la vez, en cualquier número de trabajadores.
- **Un fallo solo frena a su grupo**: el mensaje que falla espera su reintento y los demás grupos siguen.
- **Al menos una vez**: un mensaje sale de su fila solo cuando se confirma.
- **Reparto justo**: tras cada mensaje, el grupo vuelve al final de los turnos; uno con mil mensajes no
  acapara a los demás.

## Cómo funciona

```
{cola}:g:<grupo>   lista con los mensajes del grupo, en orden
{cola}:ready       grupos con trabajo y sin dueño, en turno
{cola}:busy        grupo → vencimiento de su turno
{cola}:tok         grupo → token del turno vigente
{cola}:delay       grupo → cuándo se reintenta
{cola}:att         grupo → intentos del mensaje en cabeza
{cola}:dead        stream con lo que agotó sus intentos (DLQ)
```

Un grupo con mensajes está exactamente en uno de `ready`, `busy` o `delay`. Cada operación es un script
Lua, que Redis ejecuta sin interrupciones, así que dos trabajadores nunca toman el mismo grupo.

- `take` saca un grupo de `ready`, lo marca en `busy` y devuelve su primer mensaje **sin sacarlo**.
- `ack` lo saca y, si quedan mensajes, devuelve el grupo al final de `ready`.
- `retry` aparta al grupo en `delay`; `bury` mueve el mensaje a la DLQ y el grupo sigue.
- `maintain` devuelve a `ready` los grupos con la espera vencida y los de turnos abandonados.
- Cada turno tiene un **token**: si un trabajador se congela, su turno vence y otro toma el grupo, sus
  confirmaciones se rechazan.
- Los intentos se cuentan al tomar: un mensaje que tumba al proceso una y otra vez acaba en la DLQ sin
  volver a ejecutarse.

## Uso

```rust
let fila = Fila::open(&redis, "", "records").await?; // exige Redis durable
fila.push("tenant:42", event_id, &payload).await?;

run_worker(fila, handler, WorkerConfig::default(), stop).await?;
```

El `id` lo da el productor: si reintenta un `push` que falló a medias, el duplicado lleva el mismo `id`.
El manejador puede ejecutarse más de una vez para un mismo `id`: sus efectos deben ser idempotentes.

`WorkerConfig::default()`: 32 grupos a la vez, turnos de 30 s renovados cada tercio, reintentos a los
`1 s, 10 s, 30 s, 1 min, 5 min, 15 min, 1 h` y después a la DLQ. Ante un fallo de Redis, el trabajador deja
de tomar mensajes, espera a los manejadores en vuelo y devuelve el error: el servicio lo reinicia.

## Requisitos de Redis

`Fila::open` se niega a abrir si Redis no tiene `appendonly yes`, `appendfsync always` y
`maxmemory-policy noeviction`: confirmado un `push`, Redis es la única copia del mensaje.

Con Redis Cluster, todas las claves de una cola caen en el mismo nodo (`{cola}`): una cola no se reparte
entre nodos.

## Pruebas

```bash
docker compose up -d   # Redis durable en 56380
cargo test
cargo test --test fila -- --ignored redis_killed   # mata Redis con SIGKILL en plena carga
```

Se verificaron con mutaciones (turno al frente en vez de al final, `take` sin sacar el grupo, `push` que
mete en turnos un grupo ocupado, `ack` sin token, `maintain` sin recuperar turnos, intentos sin contar,
reintento sin espera, sin renovar turnos, `bury` sin devolver el grupo): cada una rompe al menos una prueba.
