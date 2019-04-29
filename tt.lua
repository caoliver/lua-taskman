local function task()
   local m=require 'freezer'
   local e=require 'tek.lib.exec'
   local nextguy = arg[1]
   while true do
      e.sendmsg(nextguy, m.thaw((e.waitmsg()))(m,e))
   end
end

local counter, animal1, animal2 = 1, 'frog', 'toad'
local function f(m,e)
   print(e.getname(), counter, animal1, animal2)
   animal1 , animal2 = animal2, animal1
   counter=counter+1
   e.sleep(100)
   return m.freeze(f, {}, false, true)
end

local m=require 'freezer'
local e=require 'tek.lib.exec'

function starttask(name, nextguy)
   local function task()
      local m=require 'freezer'
      local e=require 'tek.lib.exec'
      local nextguy = arg[1]
      while true do
	 e.sendmsg(nextguy, m.thaw((e.waitmsg()))(m,e))
      end
   end
   e.run({func=task, taskname=name}, nextguy)
end

starttask('t1','t2')
starttask('t2','t3')
starttask('t3','t4')
starttask('t4','t5')
starttask('t5','t1')
e.sendmsg('t1',m.freeze(f, {}, false, true))
e.wait()
