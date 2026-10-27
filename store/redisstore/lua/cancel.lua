local version_key = KEYS[3]
local events_key = KEYS[4]
local retention_ms = 60000

local removed_tickets = {}
for i = 2, #ARGV do
  local ticket = ARGV[i]
  if redis.call('ZREM', queue_key, ticket) == 1 then
    removed_tickets[#removed_tickets + 1] = ticket
  end
end

if #removed_tickets > 0 then
  local version = redis.call('INCR', version_key)
  local payload = 'cancelled ' .. version .. ' ' .. table.concat(removed_tickets, ' ')
  local oldest_kept_ms = math.floor(now_usec / 1000) - retention_ms
  local oldest_kept_id = oldest_kept_ms .. '-0'
  redis.call('XADD', events_key, 'MINID', '~', oldest_kept_id, '*', 'e', payload)
end

return #removed_tickets
