f=require 'ffi'
f.cdef [[
  void usleep(int);
]]

function program()
   local f=require 'ffi'
   f.cdef [[
   int fork();
   int execlp(...);
   int wait(int *);
]]
   local function run(a, ...)
      if (f.C.fork() > 0) then
	 f.C.wait(nil)
      else
	 f.C.execlp(a, a, ...)
      end
   end
   t=require 'taskmgr'
   while true do
      run('sleep', '60')
   end
end

t=require 'taskmgr'
ts,no = t.create_task{program=program}
f.C.usleep(5000)
t.create_task{program="t=require 'taskmgr';while true do t.waitmsg() end"}
f.C.usleep(50000);
--t.interrupt_task(true,ts,no)
t.shutdown()

