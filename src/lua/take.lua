-- KEYS: ready, busy, tok, att · ARGV: lease_ms, token, prefijo de las filas
-- Toma el turno del siguiente grupo y devuelve su primer mensaje SIN sacarlo de la fila.
-- El intento se cuenta aquí: un mensaje que tumba al proceso también gasta intentos.
local g = redis.call('LPOP', KEYS[1])
if not g then return false end
local head = redis.call('LINDEX', ARGV[3] .. g, 0)
if not head then
    redis.call('HDEL', KEYS[4], g)
    return false
end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[1]), g)
redis.call('HSET', KEYS[3], g, ARGV[2])
local attempt = redis.call('HINCRBY', KEYS[4], g, 1)
return {g, head, attempt}
