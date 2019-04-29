CFLAGS+=-fPIC -I /usr/local/include/luajit-2.0 -I/usr/local/include
CFLAGS+=-Wall -Wno-parentheses -O2 -std=c99
CFLAGS+=-mtune=generic
#CFLAGS+=-march=native -mfloat-abi=hard
CFLAGS+=-D_POSIX_C_SOURCE=200112L
LDFLAGS+=-lpthread -lm

.PHONY: all test clean

test: newmarshal.so
	lua test.lua


all: newmarshal.so
	lua test.lua

%.so: %.o 
	gcc -shared $(LDFLAGS) -o $@ $<

clean:
	find -name \*.o -delete -o -name \*.so -delete
