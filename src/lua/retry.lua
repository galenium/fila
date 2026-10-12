-- KEYS: busy, tok, delay · ARGV: grupo, token, espera_ms
-- El mensaje sigue en cabeza; solo este grupo espera.
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[3], now + tonumber(ARGV[3]), ARGV[1])
return 1
