PREFIX=/usr/local
LIBPATH=$(PREFIX)/lib/lua/5.1
LUAPATH=$(PREFIX)/share/lua/5.1
INCPATH=$(PREFIX)/include/lua/5.1
CFLAGS+=-fPIC -I$(INCPATH) -I/usr/local/include -pthread
CFLAGS+=-Wall -Wno-parentheses -fomit-frame-pointer -std=c99 -O2
CFLAGS+=-D_POSIX_C_SOURCE=200112L
CFLAGS+=-mtune=generic
#CFLAGS+=-march=native -mfloat-abi=hard
LDFLAGS+=-ldl -shared -pthread -lm -Wl,-rpath=$(LIBPATH)
VER=0.0

.PHONY: all clean tests install

all: taskman.so freezer.so

freezer.so: freezer.o 
	gcc $(LDFLAGS) -Wl,-soname,lua-freezer.so.$(VER) -o $@ $^
	lua fz-test.lua

taskman.so: taskman.o mmaputil.so freezer.so
	gcc -Wl,-soname,lua-taskman.so.$(VER) $(LDFLAGS) -o $@ $^

mmaputil.so: mmaputil.o
	gcc $(LDFLAGS) -Wl,-soname,lua-mmaputil.so.$(VER) -o $@ $^

install: freezer.so taskman.so mmaputil.so ffi+.lua
	install -m 0755 *.so $(LIBPATH)
	install -m 0644 ffi+.lua $(LUAPATH)
	install -m 0644 mmaputil.h cbuf.h $(INCPATH)
	(cd $(LIBPATH) && ldconfig -N -l freezer.so taskman.so mmaputil.so)

clean:
	find \( -name \*.o -o -name \*.so -o -name \*.so.$(VER) \) -delete
