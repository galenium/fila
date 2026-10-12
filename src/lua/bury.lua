-- KEYS: busy, tok, ready, att, fila del grupo, dead · ARGV: grupo, token, error
-- Saca el mensaje de cabeza a la DLQ y el grupo sigue con el siguiente.
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
local attempts = redis.call('HGET', KEYS[4], ARGV[1]) or '0'
local head = redis.call('LPOP', KEYS[5])
redis.call('XADD', KEYS[6], '*', 'group', ARGV[1], 'message', head, 'attempts', attempts, 'error', ARGV[3])
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
if redis.call('LLEN', KEYS[5]) > 0 then
    redis.call('RPUSH', KEYS[3], ARGV[1])
end
return 1
