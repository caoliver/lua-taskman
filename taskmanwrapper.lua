local taskman=require 'taskman'

local mt=debug.getmetatable(taskman.now())
function mt.__index(tbl,key)
   if key == 'parts' then
      return function(self) return taskman.timestamp_parts(self) end
   end
end

local oldnow = taskman.now
mt = {}
function mt.__index(tbl,key)
   if key == 'now' then return oldnow() end
   if key == 'my_name' then return taskman.get_my_name() end
end
taskman.now = nil

return setmetatable(taskman, mt)

   
