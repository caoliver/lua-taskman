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
   local microsecs = 1e6*this_time/times
   print(('%s\t%.4fus\t%5d\t%.2f%%'):
	 format(name, microsecs, #help(td),
		100*(this_time-last_time)/last_time))
   return microsecs
end

nfz = package.loadlib('newfreezer.so', 'luaopen_freezer')
if (nfz) then nfz = nfz() end
fz = require 'freezer'
rh= require 'marshal'
cb = require 'CBOR'

print [[
rh = Richard Hundt's lua-marshal.  (Speed standard)
cb = Francois Perrad's CBOR implementation
fz = Freezer

values:
	time for operations
	size of result (for the encoding)
	ratio of time taken to rh time]]

function header(bench)
   print (bench..' '..times..' times');
end

local function null() end
local function ident(x) return x end
local otd

function ebench(head, tdata, skipcb)
   header('\n'..head..' encode')
   td = tdata
   bench('rh', rh.encode, null)
   if not skipcb then
      bench('cb', cb.encode, null, true)
   else
      print('cb\tN/A')
   end
   local elapsedfz = bench('fz', fz.encode, null, true)
   if nfz then
      local elapsednfz = bench('new fz', nfz.encode, null, true)
      print(("gain    %0.2f%%"):format(100*(elapsedfz-elapsednfz)/elapsedfz))
      assert(fz.encode(td) == nfz.encode(td))
   end
end

function dbench(head, tdata, skipcb)
   header('\n'..head..' decode')
   td=rh.encode(tdata)
   bench('rh', ident, rh.decode)
   if not skipcb then
      td=cb.encode(tdata)
      bench('cb', ident, cb.decode, true)
   else
      print('cb\tN/A')
   end
   td=fz.encode(tdata,nil,false,strip)
   local elapsedfz = bench('fz', ident, fz.decode, true)
   if nfz then
      local elapsednfz = bench('new fz', ident, nfz.decode, true)
      print(("gain    %0.2f%%"):format(100*(elapsedfz-elapsednfz)/elapsedfz))
   end
end

function edbench(head, tdata, skipcb)
   header('\n'..head)
   bench('rh', rh.encode, rh.decode)
   if not skipcb then
      bench('cb', cb.encode, cb.decode, true)
   else
      print('cb\tN/A')
   end
   bench('fz', fz.encode, fz.decode, true)
   if nfz then
      bench('new fz', nfz.encode, nfz.decode, true)
      assert(fz.encode(td) == nfz.encode(td))
   end
end


-- [[
print('\n\nscalars\n')

times=scale*1e6
--ebench('small ints', 456)
dbench('small ints', 456)

--ebench('doubles', math.pi)
dbench('doubles', math.pi)

tdat=('#'):rep(1024)
--ebench('long strings', tdat)
dbench('long strings', tdat)

--ebench('booleans', true)
dbench('booleans', true)

--ebench('nil', nil)
dbench('nil', nil)

--]]

-- [[
print('\n\ntables\n')

times=scale*5e5

--ebench('empty', {})
dbench('empty', {})

tdat={'one','two','three','four','five','six','seven'}
--ebench('num->string', tdat)
dbench('num->string', tdat)

times=scale*25e4

td={aardvark='one',bat='two',cheetah='three',dog='four',elephant='five',
    fish='six',giraffe='seven'}
--ebench('string->string', tdat)
dbench('string->string', tdat)

td={aardvark='animal',bat='animal',cheetah='animal',dog='animal',    elephant='animal', fish='animal',giraffe='animal'}
--ebench('string->string dup values', tdat)
dbench('string->string dup values', tdat)

times=scale*5e5

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

tdat=make_clique(8)
--ebench('cyclic references / 8 node clique', tdat, true)
dbench('cyclic references / 8 node clique', tdat, true)

--]]

-- [[
function closures(strip_flag)
   strip=strip_flag
   io.write('\n\nClosure serialization with stripping ',
	    strip and "en" or "dis", "abled\n")

   times=scale*5e5

   function tdat() print('I have no upvalues') end
   --ebench('function without upvalues', tdat, true)
   dbench('function without upvalues', tdat, true)

   do
      local x = 'wombat'
      function tdat() print('My upvalue has the value '..x) end
   end
   --ebench('function with upvalues', tdat, true)
   dbench('function with upvalues', tdat, true)

   times=scale*1e5
   tdat=loadfile 'bench.lua'
   --ebench('this file as a chunk', tdat, true)
   dbench('this file as a chunk', tdat, true)
end

closures(false)
closures(true)
--]]
