local queue_key = KEYS[1]
local head_key = KEYS[2]
local interval_usec = tonumber(ARGV[1])

local redis_time = redis.call('TIME')
local now_usec = tonumber(redis_time[1]) * 1000000 + tonumber(redis_time[2])

local head_usec = tonumber(redis.call('GET', head_key)) or (now_usec - 1)

local passed_slots = 0
if now_usec >= head_usec then
  passed_slots = math.floor((now_usec - head_usec) / interval_usec) + 1
end

local waiting_count = redis.call('ZCARD', queue_key)
local fired_count = passed_slots
if fired_count > waiting_count then
  fired_count = waiting_count
end

if fired_count > 0 then
  redis.call('ZPOPMIN', queue_key, fired_count)
  head_usec = head_usec + fired_count * interval_usec
  waiting_count = waiting_count - fired_count
end

if waiting_count == 0 and head_usec < now_usec then
  head_usec = now_usec
end

redis.call('SET', head_key, head_usec)
