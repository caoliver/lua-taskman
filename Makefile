CFLAGS+=-fPIC -I /usr/local/include/luajit-2.0 -I/usr/local/include
CFLAGS+=-Wall -Wno-parentheses -O2 -mtune=generic -std=c99
CFLAGS+=-D_POSIX_C_SOURCE=200112L
LDFLAGS+=-lluajit -lpthread -lm

.PHONY: all clean

all: marshal.so
	lua test.lua

%.so: %.o 
	gcc -shared $(LDFLAGS) -o $@ $<

clean:
	find -name \*.o -delete -o -name \*.so -delete
