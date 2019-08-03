CFLAGS+=-fPIC -I /usr/local/include/luajit-2.0 -I/usr/local/include
CFLAGS+=-Wall -Wno-parentheses -O2 -mtune=generic -fomit-frame-pointer -std=c99
CFLAGS+=-D_POSIX_C_SOURCE=200112L
LDFLAGS+=-lluajit -ldl -pthread -lm

all: taskman.so

globals.h:

taskman.so: taskman.o mmaputil.so /usr/local/lib/lua/5.1/freezer.so
	gcc -Wl,-rpath='$$ORIGIN' -shared $(LDFLAGS) -o $@ $^

%.so: %.o
	gcc -shared $(LDFLAGS) -o $@ $<

clean:
	find -name \*.o -delete -o -name \*.so -delete
