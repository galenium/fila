-- KEYS: ready, busy, tok, att, fence · ARGV: lease_ms, token, prefijo de las filas
-- Toma el turno del siguiente grupo y devuelve su primer mensaje SIN sacarlo de la fila.
-- El intento se cuenta aquí: un mensaje que tumba al proceso también gasta intentos.
-- `fence` crece con cada turno de la cola: un destino que lo guarde rechaza escrituras de turnos viejos.
for _ = 1, 100 do
    local g = redis.call('LPOP', KEYS[1])
    if not g then return false end
    local head = redis.call('LINDEX', ARGV[3] .. g, 0)
    if head then
        local t = redis.call('TIME')
        local now = t[1] * 1000 + math.floor(t[2] / 1000)
        redis.call('ZADD', KEYS[2], now + tonumber(ARGV[1]), g)
        redis.call('HSET', KEYS[3], g, ARGV[2])
        local attempt = redis.call('HINCRBY', KEYS[4], g, 1)
        local fence = redis.call('INCR', KEYS[5])
        return {g, head, attempt, fence}
    end
    -- Un grupo en turno con la fila vacía no debería existir; si aparece, se descarta y se sigue.
    redis.call('HDEL', KEYS[4], g)
end
return false
