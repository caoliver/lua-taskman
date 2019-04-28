-- Stopwatch function for benchmarking

ffi = require("ffi")
ffi.cdef [[
    typedef struct timespec {
        long tv_sec;
        long tv_nsec;
    } timespec;
 
    int clock_gettime(int clk_id, struct timespec *tp);
]]
local time_struct = ffi.new("timespec")

function stopwatch()
   function getcputime()
      ffi.C.clock_gettime(2, time_struct);
      return tonumber(time_struct.tv_sec) + 1E-9*tonumber(time_struct.tv_nsec)
   end
   local start_time = getcputime()
   return function()
      return getcputime() - start_time
   end
end

td={one='alfred', two='basil', three='clara', four='desmond'}
td={one='alfred', two='alfred', three='alfred', four='alfred'}

td = { a='a', b='b', c='c', d='d', hop='jump', skip='foo', answer=42 }

local times
scale=1

local last_time

function bench(name, encoder, decoder, show)
   function help(td) return encoder(td, {}, false, strip) end
   collectgarbage()
   s=stopwatch()
   for i=1,times do
      decoder(help(td))
   end
   this_time=s()
   if not show then
      last_time=this_time
   end
   print(('%s\t%.8f\t%5d\t%.8f'):format(name, this_time, #help(td),
					this_time/last_time))
end
   
nm = require 'newmarshal'
rh= require 'marshal'
cb = require 'CBOR'

print [[
rh = Richard Hundt's lua-marshal.  (Speed standard)
cb = Francois Perrad's CBOR implementation
nm = New lua marshal

values:
	time for operations
	size of result (for the encoding)
	ratio of time taken to rh time]]

function header(bench)
   print (bench..' '..times..' times');
end

local function null() end
local function ident(x) return x end

-- [[
print('\n\nscalars\n')


times=scale*5e3
td={}; for i=-1024,1024 do td[i+1025]=i end
header '\nsmall ints'
bench('rh', rh.encode, rh.decode)
bench('cb', cb.encode, cb.decode, true)
bench('nm', nm.encode, nm.decode, true)

times=scale*1e5
td={}; for i=1,16 do td[i]=i/17 end
header '\ndoubles'
bench('rh', rh.encode, rh.decode)
bench('cb', cb.encode, cb.decode, true)
bench('nm', nm.encode, nm.decode, true)

times=scale*1e6
td=('#'):rep(1024)
header '\nlong string'
bench('rh', rh.encode, rh.decode)
bench('cb', cb.encode, cb.decode, true)
bench('nm', nm.encode, nm.decode, true)

td=true
header '\nboolean'
bench('rh', rh.encode, rh.decode)
bench('cb', cb.encode, cb.decode, true)
bench('nm', nm.encode, nm.decode, true)

td=nil
header '\nnil'
bench('rh', rh.encode, rh.decode)
bench('cb', cb.encode, cb.decode, true)
bench('nm', nm.encode, nm.decode, true)
--]]

-- [[
print('\n\ntables\n')

times=scale*5e5

td={'one','two','three','four','five','six','seven'}
header '\nnum->string'
bench('rh', rh.encode, rh.decode)
bench('cb', cb.encode, cb.decode, true)
bench('nm', nm.encode, nm.decode, true)

td={aardvark='one',bat='two',cheetah='three',dog='four',elephant='five',
    fish='six',giraffe='seven'}
header '\nstring->string'
bench('rh', rh.encode, rh.decode)
bench('cb', cb.encode, cb.decode, true)
bench('nm', nm.encode, nm.decode, true)

td={aardvark='animal',bat='animal',cheetah='animal',dog='animal',    elephant='animal', fish='animal',giraffe='animal'}
header '\nstring->string dup values'
bench('rh', rh.encode, rh.decode)
bench('cb', cb.encode, cb.decode, true)
bench('nm', nm.encode, nm.decode, true)

function make_clique(n)
   clique = {}
   for i=1,n do table.insert(clique, {}) end
   for _,from in pairs(clique) do
      for _,to in pairs(clique)  do
	 if from ~= to then table.insert(from,to) end
      end
   end
   return clique[1]
end

times=scale*2e5

td=make_clique(8)
header '\ncyclic references / 8 node clique'
bench('rh', rh.encode, rh.decode)
print('cb\tN/A')
bench('nm', nm.encode, nm.decode, true)
--]]

-- [[
function closures(strip_flag)
   strip=strip_flag
   io.write('\n\nClosure serialization with stripping ',
	    strip and "en" or "dis", "abled\n")

   times=scale*5e5

   do
      function td() print('I have no upvalues') end
   end
   header '\nfunction without upvalues encoding'
   bench('rh', rh.encode, null)
   print('cb\tN/A')
   bench('nm', nm.encode, null, true)

   header '\nfunction without upvalues decoding'
   save=td
   td=rh.encode(save)
   bench('rh', ident, rh.decode)
   print('cb\tN/A')
   td=nm.encode(save, {}, false, strip)
   bench('nm', ident, nm.decode, true)

   do
      local x = 'wombat'
      function td() print('My upvalue has the value '..x) end
   end
   header '\nfunction with upvalues encoding'
   bench('rh', rh.encode, null)
   print('cb\tN/A')
   bench('nm', nm.encode, null, true)

   header '\nfunction with upvalues decoding'
   save=td
   td=rh.encode(save)
   bench('rh', ident, rh.decode)
   print('cb\tN/A')
   td=nm.encode(save, {}, false, strip)
   bench('nm', ident, nm.decode, true)

   times=scale*1e5
   td=loadfile 'bench.lua'
   header '\nthis file as a chunk encoding'
   bench('rh', rh.encode, null)
   print('cb\tN/A')
   bench('nm', nm.encode, null, true)

   header '\nthis file as a chunk decoding'
   save=td
   td=rh.encode(save)
   bench('rh', ident, rh.decode)
   print('cb\tN/A')
   td=nm.encode(save, {}, false, strip)
   bench('nm', ident, nm.decode, true)
end

closures(false)
closures(true)
--]]
