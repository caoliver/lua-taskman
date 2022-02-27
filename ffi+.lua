-- A convenience wrapper to support the convention of having the
-- cdefs for a library held in the global variable cdef_string.

local ffi=require 'ffi'
ffi.cdef 'char *cdef_string'

local cdefs_loaded = {}

function ffi.load_with_cdefs(shared_object_name, search_cpath)
   if search_cpath then
      shared_object_name =
	 package.searchpath(shared_object_name, package.cpath)
   end
   local shared_object = ffi.load(shared_object_name)
   if not cdefs_loaded[shared_object_name] then
      ffi.cdef(ffi.string(shared_object.cdef_string))
      cdefs_loaded[shared_object_name] = true
   end
   return shared_object
end

function ffi.require_and_load(library)
   -- Require sees lua code too, but we just want the binary libraries.
   local handle = assert(package.loaders[3](library))()
   if handle then
      local ffilib = ffi.load_with_cdefs(library, true)
      -- Don't crash on missing index.
      local mt = getmetatable(ffilib)
      local old_index = mt.__index
      function mt.__index(tab, key)
	 local success, value = pcall(old_index, tab,key)
	 if success then return value end
      end
      setmetatable(handle, { __index = ffilib })
   end
   return handle
end

package.loaded['ffi+'] = ffi

return ffi
