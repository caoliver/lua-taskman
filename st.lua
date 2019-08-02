t=require 'taskmgr'

local volume=2^32
local msg_size = 2^10
local msg_count = volume/msg_size

function reader()
   t=require 'taskmgr'
   s = os.clock()
   for i=1,msg_count do
      t.waitmsg()
   end
   print(os.clock()-s)
end

t.create_task{program=reader, taskname='reader'}

msg=('0'):rep(msg_size)
drops = 0
sent = 0
while true do
   if (t.sendmsg(msg, 'reader') >= 0 ) then
      sent = sent + 1
      if sent == msg_count then break end
   else
      drops = drops+1
   end
end

print(('msgs %d drops %d  (%f%%)'):format(msg_count, drops, 100*drops/msg_count))

ffi=require 'ffi'
ffi.cdef 'void usleep(int)'
ffi.C.usleep(500000)
t.shutdown()
