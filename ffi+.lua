-- A convenience wrapper to support the convention of having the
-- cdefs for a library held in the global variable cdef_string.

local ffi=require 'ffi'
ffi.cdef 'char *cdef_string'

function ffi.load_with_cdefs(shared_object_name)
   local shared_object = ffi.load(shared_object_name)
   ffi.cdef(ffi.string(shared_object.cdef_string))
   return shared_object
end

function ffi.require_and_load(library, so_file)
   local handle = require(library)
   if handle then
      setmetatable(handle,
		   {__index=ffi.load_with_cdefs(so_file)})
   end
   return handle
end

return ffi
