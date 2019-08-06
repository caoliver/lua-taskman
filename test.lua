local b='very'

local function f() print 'hello, there' end

local t=require'taskman'
t.set_subscriptions {any_task_exits=true}

-- Launch thread which...
t.create_task{
   program=function (fn)
      -- Launches a second thread that takes...
      require 'taskman'.create_task{program=fn,'hairy'}
   end,
   -- a function with local refs  as argument.
   function (a)
      print (b..' '..a..' wombat')
      f()
   end
}

t.wait_message()
t.wait_message()
t.shutdown()

--[[

Output is:

very hairy wombat
hello, there

--]]
