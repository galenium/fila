-- KEYS: busy, tok · ARGV: grupo, token, lease_ms
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('ZADD', KEYS[1], 'XX', now + tonumber(ARGV[3]), ARGV[1])
return 1
