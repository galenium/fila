-- KEYS: ready, busy, tok, delay · ARGV: límite
-- Devuelve a los turnos los grupos cuya espera venció y los de trabajadores que dejaron vencer su turno
-- (su mensaje sigue en cabeza: otro lo retoma). Es idempotente; cualquiera puede llamarlo.
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local due = redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
for _, g in ipairs(due) do
    redis.call('ZREM', KEYS[4], g)
    redis.call('RPUSH', KEYS[1], g)
end
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
for _, g in ipairs(expired) do
    redis.call('ZREM', KEYS[2], g)
    redis.call('HDEL', KEYS[3], g)
    redis.call('RPUSH', KEYS[1], g)
end
return {#due, #expired}
