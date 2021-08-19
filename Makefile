PREFIX=/usr/local
LIBPATH=$(PREFIX)/lib/lua/5.1
LUAPATH=$(PREFIX)/share/lua/5.1
INCPATH=$(PREFIX)/include/lua/5.1
CFLAGS+=-fPIC -I$(INCPATH) -I/usr/local/include -pthread
CFLAGS+=-Wall -Wno-parentheses -Wno-maybe-uninitialized
CFLAGS+=-fomit-frame-pointer -std=c99 -O3
CFLAGS+=-D_POSIX_C_SOURCE=200112L
CFLAGS+=-mtune=generic
#CFLAGS+=-march=native -mfloat-abi=hard
LDFLAGS+=-ldl -shared -pthread -lm
VER=0.0
SHOBJS=freezer.so taskman.so twinmap.so strbuff.so

.PHONY: all clean tests install

all: taskman.so freezer.so

strbuff.so: strbuff.o
	gcc $(LDFLAGS) -Wl,-soname,lua-strbuff.so.$(VER) -o $@ $^
	ldconfig -N -l $@

freezer.so: freezer.o strbuff.so
	gcc $(LDFLAGS) -Wl,-soname,lua-freezer.so.$(VER) \
	-Wl,-rpath=$(LIBPATH) -o $@ $^
	LD_LIBRARY_PATH=$(PWD) lua fz-test.lua

taskman.so: taskman.o twinmap.so freezer.so
	gcc -Wl,-soname,lua-taskman.so.$(VER) $(LDFLAGS) \
	-Wl,-rpath=$(LIBPATH) -o $@ $^

twinmap.so: twinmap.o
	gcc $(LDFLAGS) -Wl,-soname,lua-twinmap.so.$(VER) -o $@ $^

install: freezer.so taskman.so twinmap.so ffi+.lua
	install -m 0755 *.so $(LIBPATH)
	install -m 0644 ffi+.lua $(LUAPATH)
	install -m 0644 twinmap.h cbuf.h $(INCPATH)
	(cd $(LIBPATH) && ldconfig -N -l $(SHOBJS))

clean:
	find \( -name \*.o -o -name \*.so -o -name \*.so.$(VER) \) -delete
