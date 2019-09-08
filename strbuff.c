#include <string.h>
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#include "strbuff.h"

void strbuff_buffinit(lua_State *L, int lua_index, struct strbuff *buf)
{
    buf->L = L;
    buf->ptr = buf->buf;
    buf->end = buf->buf + sizeof(buf->buf);
    buf->depth = 0;
    buf->lua_index = lua_index;
}

void strbuff_addlstring(struct strbuff *buf, const char *str, size_t len)
{
    while (len > 0) {
	size_t remaining = buf->end - buf->ptr;
	if (len > remaining) {
	    memcpy(buf->ptr, str, remaining);
	    lua_pushlstring(buf->L, buf->buf, sizeof(buf->buf));
	    lua_rawseti(buf->L, buf->lua_index, ++buf->depth);
	    str += remaining;
	    len -= remaining;
	    buf->ptr = buf->buf;
	} else {
	    memcpy(buf->ptr, str, len);
	    buf->ptr += len;
	    break;
	}
    }
}

void strbuff_addchar(struct strbuff *buf, char ch)
{
    if (buf->end - buf->ptr == 0) {
	lua_pushlstring(buf->L, buf->buf, sizeof(buf->buf));
	lua_rawseti(buf->L, buf->lua_index, ++buf->depth);
	buf->ptr = buf->buf;
    }
    *buf->ptr++ = ch;
}


#define CONCAT_SIZE 8
void strbuff_pushresult(struct strbuff *buf)
{
    lua_State *L = buf->L;
    lua_pushlstring(L, buf->buf, buf->ptr - buf->buf);
    int depth = buf->depth;
    if (depth==0)
	return;
    int tsrc = buf->lua_index;
    lua_rawseti(L, tsrc, ++depth);

    if (depth > CONCAT_SIZE) {
	lua_newtable(L);
	int tdst = lua_gettop(L);
	while (depth > CONCAT_SIZE) {
	    int dst = 1, count = 0;
	    for (int src=1; src <= depth; src++) {
		lua_rawgeti(L, tsrc, src);
		lua_pushnil(L);
		lua_rawseti(L, tsrc, src);
		if (++count >= CONCAT_SIZE) {
		    lua_concat(L, CONCAT_SIZE); 
		    lua_rawseti(L, tdst, dst++);
		    count = 0;
		}
	    }
	    if (count) {
		lua_concat(L, count);
		lua_rawseti(L, tdst, dst++);
	    }
	    int tmp = tsrc;
	    tsrc = tdst;
	    tdst = tmp;
	    depth = dst - 1;
	}
    }

    for (int i = 1; i <= depth; i++) {
	lua_rawgeti(L, tsrc, i);
	lua_pushnil(L);
	lua_rawseti(L, tsrc, i);
    }
    lua_concat(L, depth);
}
