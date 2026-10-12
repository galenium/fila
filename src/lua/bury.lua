-- KEYS: busy, tok, ready, att, fila del grupo, dead, halted · ARGV: grupo, token, error, detener (1/0)
-- Saca el mensaje de cabeza a la DLQ. Con `detener`, el grupo queda parado (aunque su fila quede vacía)
-- hasta `redrive` o `release`: el siguiente mensaje no se aplica sin el que murió.
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
local attempts = redis.call('HGET', KEYS[4], ARGV[1]) or '0'
local head = redis.call('LPOP', KEYS[5])
redis.call('XADD', KEYS[6], '*', 'group', ARGV[1], 'message', head, 'attempts', attempts, 'error', ARGV[3])
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
if ARGV[4] == '1' then
    redis.call('SADD', KEYS[7], ARGV[1])
elseif redis.call('LLEN', KEYS[5]) > 0 then
    redis.call('RPUSH', KEYS[3], ARGV[1])
end
return 1
