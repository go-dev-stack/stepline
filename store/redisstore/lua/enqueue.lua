local seq_key = KEYS[3]
local version_key = KEYS[4]
local budget_usec = tonumber(ARGV[2])

local position = waiting_count
local slot_usec = head_usec + position * interval_usec
local wait_usec = slot_usec - now_usec
if wait_usec < 0 then
  wait_usec = 0
end

local version = tonumber(redis.call('GET', version_key)) or 0

if wait_usec > budget_usec then
  return {0, wait_usec, version}
end

local ticket = redis.call('INCR', seq_key)
redis.call('ZADD', queue_key, ticket, ticket)
return {ticket, wait_usec, version}
