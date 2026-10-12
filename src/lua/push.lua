-- KEYS: ready, busy, delay, halted, fila del grupo · ARGV: grupo, mensaje
-- Si la fila estaba vacía y el grupo no está detenido, entra en los turnos.
local n = redis.call('RPUSH', KEYS[5], ARGV[2])
if n == 1
    and not redis.call('ZSCORE', KEYS[2], ARGV[1])
    and not redis.call('ZSCORE', KEYS[3], ARGV[1])
    and redis.call('SISMEMBER', KEYS[4], ARGV[1]) == 0 then
    redis.call('RPUSH', KEYS[1], ARGV[1])
end
return n
