-- KEYS: ready, busy, delay, fila del grupo · ARGV: grupo, mensaje
-- Si la fila estaba vacía, el grupo no estaba en ningún sitio: entra en los turnos.
local n = redis.call('RPUSH', KEYS[4], ARGV[2])
if n == 1 and not redis.call('ZSCORE', KEYS[2], ARGV[1]) and not redis.call('ZSCORE', KEYS[3], ARGV[1]) then
    redis.call('RPUSH', KEYS[1], ARGV[1])
end
return n
