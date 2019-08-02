#pragma once
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#define _STRINGIFY_(arg...) #arg
#define STRINGIFY(arg) _STRINGIFY_(arg)

#define LUAFN(NAME, ...) static int lua_fn_##NAME(lua_State *L, ## __VA_ARGS__)
#define LUAFN_NAME(NAME) lua_fn_##NAME
#define CALL_LUAFN(NAME, ...) lua_fn_##NAME(L, ## __VA_ARGS__)
#define FN_ENTRY(NAME) { #NAME, lua_fn_##NAME }
#define AT_NAME_PUT(NAME, VALUE, TYPE)		\
    lua_pushstring(L, #NAME);  \
    lua_push##TYPE(L, VALUE);  \
    lua_rawset(L, -3)
#define AT_NAME_PUT_INT(NAME, VALUE) AT_NAME_PUT(NAME, VALUE, integer)
