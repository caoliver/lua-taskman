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
   return ('CPU: %0.2fs  Elapsed: %0.2fs'):format(cpu_time(), elapsed_time())
end

local function writer()
   local t=require 'taskman'
   local msg=('0'):rep(msg_size)
   local drops = 0
   local sent = 0
   while sent < msg_count do
      if (t.send_message(msg, 'reader')) then
         sent = sent + 1
      else
         drops = drops+1
      end
   end
   return ('msgs %d drops %d  (%6.2f%%)'):format(msg_count, drops,
                                                 100*drops/msg_count)
end

local t=require 'taskman'
local f=require 'freezer'

t.set_subscriptions {child_task_exits=true}

t.create_task{program=reader, task_name='reader', send_results=true}
t.create_task{program=writer, send_results=true}

local m1, _, s1 = t.wait_message()
local m2, _, s2 = t.wait_message()

if (s1 == 'reader') then m1,m2=m2,m1 end

io.write(f.thaw(m1)[1], '\n', f.thaw(m2)[1], '\n')

t.shutdown()
