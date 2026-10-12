-- KEYS: ready, busy, delay, halted, dead · ARGV: id de la entrada en la DLQ, prefijo de las filas
-- Devuelve el mensaje muerto AL FRENTE de su fila (conserva el orden) y reactiva el grupo.
local e = redis.call('XRANGE', KEYS[5], ARGV[1], ARGV[1])
if #e == 0 then return 0 end
local f = {}
for i = 1, #e[1][2], 2 do f[e[1][2][i]] = e[1][2][i + 1] end
local g = f['group']
redis.call('LPUSH', ARGV[2] .. g, f['message'])
redis.call('XDEL', KEYS[5], ARGV[1])
redis.call('SREM', KEYS[4], g)
if not redis.call('ZSCORE', KEYS[2], g) and not redis.call('ZSCORE', KEYS[3], g) then
    redis.call('LREM', KEYS[1], 0, g)
    redis.call('RPUSH', KEYS[1], g)
end
return 1
