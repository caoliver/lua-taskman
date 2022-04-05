local volume = 2^32
local msg_size = 2^10
local msg_count = volume/msg_size

local function reader()
   local t=require 'taskman'
   local sw = require 'stopwatch'
   local cpu_time, elapsed_time = sw 'cpu', sw 'wall'
   for i=1,msg_count do
      t.wait_message()
   end
   print(('CPU: %0.2fs  Elapsed: %0.2fs'):format(cpu_time(), elapsed_time()))
   t.change_global_flag(0, true);
end

local function writer()
   local t=require 'taskman'
   local msg=('0'):rep(msg_size)
   while not t.global_flag(0) do
      t.send_message(msg, 'reader')
   end
end

local t=require 'taskman'

t.set_subscriptions{child_task_exits=true}
t.create_task{program=reader, task_name='reader'}

for i=1,4 do t.create_task{program=writer} end
for i=1,5 do t.wait_message() end

t.shutdown()

