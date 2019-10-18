function kill_some(i)
   -- This still needs to get loaded here as well since, this is in
   -- a distinct lua_State.
   t=require'taskman'
   f=require 'ffi'
   f.cdef [[ void pause(); ]]
   -- Tail main I'm awake.
   t.send_message('', ':main:')
   if i==3 then
      
      -- Wait 1/4sec before starting the fireworks.
      t.sleep(0.25)
      
      -- Cancel everyone but myself and the main task.
      t.cancel_all()
      
      -- but wait a minute.  Let's be uncivil and busy loop.
      while true do end
   elseif i==4 then
      t.sleep(0.4)
      t.send_message('done', ':main:')
      f.C.pause()
   elseif i~=5 and i~=1 then
      -- Jobs 1 and 5 didn't really want to stick around; we'll just sleep.
      f.C.pause();
   end
end

t=require 'taskman'

-- Get child exit messages
t.set_subscriptions{child_task_exits=true}

-- Pass the value of i as the first argument to our function.
for i=1,6 do
   local task_id = t.create_task{program=kill_some, show_errors=true, i}
   -- Wait for the task's startup message.
   t.wait_message()
   if i == 4 then
      -- Make the fourth task immune from
      -- cancellation/interruption except by us.
      t.set_immunity(true, task_id)
   end
end

-- Show who's running.
t.status()

-- Catch the first four exits.
-- Should take far less than a second.
ts=t.now()+1
for i=1,4 do t.wait_message(ts) end

-- Ignore child exit messages
t.set_subscriptions{child_task_exits=false}

-- Show who's *still* running.
t.status()

-- Wait for task 4 to tell us he's finished.
t.wait_message()
-- Shutdown will kill off anyone (task 3 and task 4) loitering.
print 'Farewell'
t.shutdown()
