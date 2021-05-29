local quit_flag=2

function task()
   t=require 'taskman'
   me=t.get_my_name()
   f=require 'freezer'
   while not t.global_flag(quit_flag) do
      msg,_,sender = t.wait_message(0.04)
      if msg then f.thaw(msg)(sender) end
   end
end

local nexttask={
   task1='task2',
   task2='task3',
   task3='task1',
}

t=require 'taskman'
t.change_global_flag(quit_flag, false)

for name in pairs(nexttask) do
   t.create_task{task_name=name, program=task }
end

local count=0

local function job(sender)
   count=count+1
   io.write("Hello, I'm ", me, ", and I've received message ",
	    count, ' from ', sender, '.\n')
   if (count < 10) then
      t.send_message(f.freeze(job), nexttask[me])
   else
      t.change_global_flag(quit_flag, true)
   end
end

t.set_subscriptions{child_task_exits=true}
t.send_message(require'freezer'.freeze(job), 'task1')
for _ in pairs(nexttask) do t.wait_message() end
if t.get_my_name() == ':main:' then t.shutdown() end

      
