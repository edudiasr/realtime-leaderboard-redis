"""Scripts Lua. O Redis roda cada script inteiro sem intercalar com outros comandos,
então tudo que está dentro é atômico: ninguém vê um ranking atualizado pela metade."""

# Registra uma kill em todos os rankings de uma vez.
# KEYS: 1-3 kills (all/dia/semana), 4-6 headshots, 7-9 maior distância,
#       10 hash do matador, 11 hash da vítima, 12 contador de eventos,
#       13 set de jogadores, 14 cache do resumo
# ARGV: 1 matador, 2 nome, 3 vítima, 4 nome, 5 headshot (0/1), 6 distância,
#       7 ttl do dia, 8 ttl da semana, 9 timestamp, 10 json do evento, 11 canal
RECORD_KILL = """
local killer, killer_name = ARGV[1], ARGV[2]
local victim, victim_name = ARGV[3], ARGV[4]
local distance = tonumber(ARGV[6])
local day_ttl, week_ttl = tonumber(ARGV[7]), tonumber(ARGV[8])

for i = 1, 3 do redis.call('ZINCRBY', KEYS[i], 1, killer) end
if ARGV[5] == '1' then
    for i = 4, 6 do redis.call('ZINCRBY', KEYS[i], 1, killer) end
end
-- GT: só troca se a distância nova for maior que o recorde atual do jogador
for i = 7, 9 do redis.call('ZADD', KEYS[i], 'GT', distance, killer) end

-- NX: o prazo começa quando o ranking do dia/semana é criado e não é renovado a cada kill
for _, i in ipairs({2, 5, 8}) do redis.call('EXPIRE', KEYS[i], day_ttl, 'NX') end
for _, i in ipairs({3, 6, 9}) do redis.call('EXPIRE', KEYS[i], week_ttl, 'NX') end

redis.call('HSET', KEYS[10], 'name', killer_name, 'last_seen', ARGV[9])
redis.call('HINCRBY', KEYS[10], 'kills', 1)
redis.call('HSET', KEYS[11], 'name', victim_name, 'last_seen', ARGV[9])
redis.call('HINCRBY', KEYS[11], 'deaths', 1)
redis.call('SADD', KEYS[13], killer, victim)
redis.call('INCR', KEYS[12])
redis.call('DEL', KEYS[14])

redis.call('PUBLISH', ARGV[11], ARGV[10])
return redis.call('ZSCORE', KEYS[1], killer)
"""

# Limite de requisições com janela deslizante: guarda o horário de cada requisição num
# sorted set, apaga as que saíram da janela e conta as que sobraram.
# KEYS: 1 chave do cliente
# ARGV: 1 janela em ms, 2 limite, 3 id único da requisição
# Retorna {liberado (1/0), quantas na janela, ms até liberar}
SLIDING_WINDOW = """
local key = KEYS[1]
local window, limit = tonumber(ARGV[1]), tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)
if count >= limit then
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    return {0, count, math.max(1, tonumber(oldest[2]) + window - now)}
end
redis.call('ZADD', key, now, ARGV[3])
redis.call('PEXPIRE', key, window)
return {1, count + 1, 0}
"""
