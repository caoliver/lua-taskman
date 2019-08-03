local b='very'

local function f() print 'hello, there' end

-- Launch thread which...
require 'taskman'.create_task{
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

-- Thread cleanup on main exit not finished yet, so...
os.execute 'sleep 1'

--[[

Output is:

very hairy wombat
hello, there

--]]
