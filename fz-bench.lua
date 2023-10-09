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

local times
scale=1

do
   local saved_time
   function bench(name, encoder, decoder, data, set_std)
      local times = scale * times
      local help=encoder
      if strip then
	 function help(data) return encoder(data, {}, false, strip) end
      end
      collectgarbage()
      s=stopwatch()
      for i=1,times do
	 decoder(help(data))
      end
      local this_time=s()
      if set_std then
	 saved_time=this_time
      end
      local microsecs = 1e6*this_time/times
      print(('%s\t%.4fus\t%5d\t%.2f%%'):
	 format(name, microsecs, #help(data),
		100*(this_time-saved_time)/saved_time))
      return microsecs
   end
end

nfz = package.loadlib('./newfreezer.so', 'luaopen_freezer')
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
   print (bench..' '..(times*scale)..' times');
end

local function null() end
local function ident(x) return x end

function ebench(head, data, skipcb)
   header('\n'..head..' encode')
   local td = data
   bench('rh', rh.encode, null, data, true)
   if not skipcb then
      bench('cb', cb.encode, null, data)
   else
      print('cb\tN/A')
   end
   local elapsedfz = bench('fz', fz.encode, null, data)
   if nfz then
      local elapsednfz = bench('new fz', nfz.encode, null, data)
      local diffns = 1000*(elapsedfz-elapsednfz)
      print(("Improvement   %0.1fns (%0.2f%%)"):
	    format(diffns, diffns/elapsedfz/10))
      assert(fz.encode(data) == nfz.encode(data))
   end
end

function dbench(head, data, skipcb, check_table)
   header('\n'..head..' decode')
   local td=rh.encode(data)
   bench('rh', ident, rh.decode, td, true)
   if not skipcb then
      td=cb.encode(data)
      bench('cb', ident, cb.decode, td)
   else
      print('cb\tN/A')
   end
   td=fz.encode(data,nil,false,strip)
   if check_table then
      local decoded = fz.decode(td)
      table.foreach(decoded,function(k,v) assert(data[k] == v) end)
      table.foreach(data, function(k,v) assert(decoded[k] == v) end)
   end
   
   local elapsedfz = bench('fz', ident, fz.decode, td)
   if nfz then
      local elapsednfz = bench('new fz', ident, nfz.decode, td)
      local diffns = 1000*(elapsedfz-elapsednfz)
      print(("Improvement   %0.1fns (%0.2f%%)"):
	 format(diffns, diffns/elapsedfz/10))
   end
end

function edbench(head, data, skipcb)
   header('\n'..head)
   bench('rh', rh.encode, rh.decode, data, true)
   if not skipcb then
      bench('cb', cb.encode, cb.decode, data)
   else
      print('cb\tN/A')
   end
   bench('fz', fz.encode, fz.decode, data)
   if nfz then
      bench('new fz', nfz.encode, nfz.decode, data)
      assert(fz.encode(td) == nfz.encode(td))
   end
end


-- [[
print('\n\nscalars\n')

times=scale*1e6
ebench('small ints', 456)
dbench('small ints', 456)

ebench('doubles', math.pi)
dbench('doubles', math.pi)

tdat=('#'):rep(1024)
ebench('long strings', tdat)
dbench('long strings', tdat)

ebench('booleans', true)
dbench('booleans', true)

ebench('nil', nil)
dbench('nil', nil)

--]]

-- [[
print('\n\ntables\n')

times=scale*5e5

ebench('empty', {})
dbench('empty', {})

td={'one','two','three','four','five','six','seven'}
ebench('num->string', td)
dbench('num->string', td, false, true)
times=scale*25e4

td={aardvark='one',bat='two',cheetah='three',dog='four',elephant='five',
    fish='six',giraffe='seven'}
ebench('string->string', td)
dbench('string->string', td, false, true)

for k, v in ipairs {'one','two','three','four','five','six','seven'} do
   td[k] = v
end
ebench('string->string + num->string', td)
dbench('string->string + num->string', td, false, true)

td={aardvark='animal',bat='animal',cheetah='animal',dog='animal',
    elephant='animal', fish='animal',giraffe='animal'}
ebench('string->string dup values', td)
dbench('string->string dup values', td, false, true)

for i=1,7 do table.insert(td, 'animal') end
ebench('string->string + num->string dup values', td)
dbench('string->string + num->string dup values', td, false, true)

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

function make_clique_keys(n)
   clique = {}
   for i=1,n do table.insert(clique, {}) end
   for _,from in pairs(clique) do
      for _,to in pairs(clique)  do
	 if from ~= to then from[to] = true end
      end
   end
   return clique[1]
end

times=scale*2e5

tdat=make_clique(8)
ebench('cyclic references / 8 node clique', tdat, true)
dbench('cyclic references / 8 node clique', tdat, true)

tdat=make_clique_keys(8)
ebench('cyclic references / 8 node clique on keys', tdat, true)
dbench('cyclic references / 8 node clique on keys', tdat, true)

--]]

-- [[
function closures(strip_flag)
   strip=strip_flag
   io.write('\n\nClosure serialization with stripping ',
	    strip and "en" or "dis", "abled\n")

   times=scale*5e5

   function tdat() print('I have no upvalues') end
   ebench('function without upvalues', tdat, true)
   dbench('function without upvalues', tdat, true)

   do
      local x = 'wombat'
      function tdat() print('My upvalue has the value '..x) end
   end
   ebench('function with upvalues', tdat, true)
   dbench('function with upvalues', tdat, true)

   times=scale*1e5
   tdat=loadfile 'fz-bench.lua'
   ebench('this file as a chunk', tdat, true)
   dbench('this file as a chunk', tdat, true)
end

closures(false)
closures(true)
--]]
