-- KEYS: ready, halted, fila del grupo · ARGV: grupo
-- Reactiva un grupo detenido SIN su mensaje muerto: sigue con el siguiente.
if redis.call('SREM', KEYS[2], ARGV[1]) == 0 then return 0 end
if redis.call('LLEN', KEYS[3]) > 0 then
    redis.call('RPUSH', KEYS[1], ARGV[1])
end
return 1
