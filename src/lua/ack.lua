-- KEYS: busy, tok, ready, att, fila del grupo · ARGV: grupo, token
-- Solo quien tiene el turno vigente confirma. Si quedan mensajes, el grupo vuelve AL FINAL de los turnos.
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
redis.call('LPOP', KEYS[5])
if redis.call('LLEN', KEYS[5]) > 0 then
    redis.call('RPUSH', KEYS[3], ARGV[1])
end
return 1
