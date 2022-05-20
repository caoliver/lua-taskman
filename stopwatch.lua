-- Stopwatch function for benchmarking
local ffi = require("ffi")
ffi.cdef [[
    typedef struct timespec {
        long tv_sec;
        long tv_nsec;
    } timespec;
 
    int clock_gettime(int clk_id, struct timespec *tp);
]]
local time_struct = ffi.new("timespec")

local function stopwatch(variety)
   local clocktype = ({ wall=4, cpu=2 })[variety] or 2
   local function gettime()
      ffi.C.clock_gettime(clocktype, time_struct);
      return tonumber(time_struct.tv_sec) + 1E-9*tonumber(time_struct.tv_nsec)
   end
   local start_time = gettime()
   return function()
      return gettime() - start_time
   end
end

return setmetatable({stopwatch=stopwatch},
 {__call=function(_, s) return stopwatch(s) end})
