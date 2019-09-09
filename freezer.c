/*********************************************************************/
/* Freezer a serializer/deserializer for Lua values inspired by	     */
/* Richard Hundt's lmarshal and the compactness of CBOR.	     */
/* 								     */
/* License: MIT							     */
/* 								     */
/* Copyright (c) 2019 Christopher Oliver			     */
/* 								     */
/* Permission is hereby granted, free of charge, to any person	     */
/* obtaining a copy of this software and associated documentation    */
/* files (the "Software"), to deal in the Software without	     */
/* restriction, including without limitation the rights to use,	     */
/* copy, modify, merge, publish, distribute, sublicense, and/or sell */
/* copies of the Software, and to permit persons to whom the	     */
/* Software is furnished to do so, subject to the following	     */
/* conditions:							     */
/* 								     */
/* The above copyright notice and this permission notice shall be    */
/* included in all copies or substantial portions of the Software.   */
/* 								     */
/* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,   */
/* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES   */
/* OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND	     */
/* NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT	     */
/* HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,	     */
/* WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING	     */
/* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR     */
/* OTHER DEALINGS IN THE SOFTWARE.				     */
/*********************************************************************/

#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <stdint.h>
#include <sys/types.h>
#include <stdbool.h>
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#include <assert.h>
#include <limits.h>

#include "strbuff.h"

#if defined(__APPLE__)
#define bswap_32(x) OSSwapInt32(x)
#define bswap_64(x) OSSwapInt64(x)
#elif defined(__linux__)
#include <byteswap.h>
#else
// In case the system doesn't provide any magic for us...
#define SWAP_BYTES(a,b) do { uint8_t t = a; a = b; b = t; } while(0)
static uint32_t bswap_32(uint32_t n)
{
    union { uint8_t ch[4]; uint32_t word; } pun = { .word = n };
    pun.word = n;
    SWAP_BYTES(pun.ch[0], pun.ch[3]);
    SWAP_BYTES(pun.ch[1], pun.ch[2]);
    return pun.word;
}

static uint64_t bswap_64(uint64_t n)
{
    union { uint8_t ch[4]; uint64_t word; } pun = { .word = n };
    SWAP_BYTES(pun.ch[0], pun.ch[7]);
    SWAP_BYTES(pun.ch[1], pun.ch[6]);
    SWAP_BYTES(pun.ch[2], pun.ch[5]);
    SWAP_BYTES(pun.ch[3], pun.ch[4]);
    return pun.word;
}
#endif

#define STRLIT(VAL) (char []){VAL}

// Include for debugging.
// #include "show_stack.h"

// Do you want to start the serialization with a magic cookie?
// #define MAGIC_COOKIE { 'L', 'F', 'R', 'Z' }

#ifdef MAGIC_COOKIE
unsigned char magic_header[] = MAGIC_COOKIE;
#endif

static int big_endian=0;

#define USER_SERIALIZER_IDX 3
#define SEEN_OBJECT_IDX 2
#define SEEN_UPVALUE_IDX 4
#define STRING_ACCUMULATOR_IDX 5


// Lower order tag for multibyte numbers
#define NUMTYPE_DOUBLE 31
#define NUMTYPE_UINT32 30
#define NUMTYPE_UINT24 29
#define NUMTYPE_UINT16 28
#define NUMTYPE_UINT8 27

// Explicit constants and special headers (Unused 72 to 95)
#define ALLOCATE_REFS 64
#define MERGE_DUPL_STRS 65
#define TABLE_END 66
#define LUA_CLOSURE 67
#define CONSTRUCTOR 68

#define VALUE_NIL 69
#define VALUE_FALSE 70
#define VALUE_TRUE 71

// Types aside from constants ()
#define TYPE_NUMBER 0
#define TYPE_NEGATIVE_INT 32
#define TYPE_STRING 96
#define TYPE_REF 128
#define TYPE_TABLE 160
#define TYPE_UVREF 192
#define TYPE_UINT 224

static unsigned int freeze_uint(uint32_t src, uint8_t type, uint8_t *dst)
{
    if (src < 27) {
	*dst = type | src;
	return 1;
    }
    if (src < 256) {
	*dst = type | NUMTYPE_UINT8;
	dst[1] = src;
	return 2;
    }
    if (src < 65536) {
	*dst = type | NUMTYPE_UINT16;
	dst[1] = src & 0xFF;
	dst[2] = src >> 8;
	return 3;
    }
    if (src < 1<<24) {
	*dst = type | NUMTYPE_UINT24;
	dst[1] = src & 0xFF;
	dst[2] = src >> 8 & 0xFF;
	dst[3] = src >> 16;
	return 4;
    }
    *dst = type | NUMTYPE_UINT32;
    if (big_endian)
	src = bswap_32(src);
    memcpy(dst+1, &src, 4);
    return 5;
}

static int thaw_uint(const uint8_t *src, uint32_t *dst,
		     uint8_t *type, size_t available)

{
    if (available < 1) return 0;
    *type = *src & 0xE0;
    
    switch(src[0] & 0x1F) {
    case NUMTYPE_UINT8:
	if (available < 2) return 0;
	*dst = src[1];
	return 2;
    case NUMTYPE_UINT16:
	if (available < 3) return 0;
	*dst = src[2] << 8 | src[1];
	return 3;
    case NUMTYPE_UINT24:
	if (available < 4) return 0;
	*dst = (src[3]<<8 | src[2])<<8 | src[1];
	return 4;
    case NUMTYPE_UINT32:
	if (available < 5) return 0;
	memcpy(dst, src+1, 4);
	if (big_endian)
	    *dst = bswap_32(*dst);
	return 5;
    default:
	*dst = src[0] & 0x1F;
	return 1;
    }
}

static unsigned int freeze_num(double src, uint8_t *dst)
{
    double posnum = fabs(src);
    uint32_t trunc = posnum;
    if ((double)trunc != posnum) {
	*dst++ = NUMTYPE_DOUBLE;
	if (big_endian) {
	    union { double in; uint64_t out; } pun = { .in = src };
	    pun.out = bswap_64(pun.out);
	    memcpy(dst, &pun.out, 8);
	} else
	    memcpy(dst, &src, sizeof(src));
	return 9;
    }
    return freeze_uint(trunc, src < 0 ? TYPE_NEGATIVE_INT : TYPE_NUMBER, dst);
}

static unsigned int thaw_num(const uint8_t *src, double *dst,
			     size_t available)
{
    if (src[0] == NUMTYPE_DOUBLE) {
	if (available < 9) return 0;
	union { uint64_t in; double out; } pun;
	memcpy(&pun.in, src+1, sizeof(pun.in));
	if (big_endian)
	    pun.in = bswap_64(pun.in);
	*dst = pun.out;
	return 9;
    }
    uint32_t u32dst;
    uint8_t type;
    unsigned int len = thaw_uint(src, &u32dst, &type, available);
    if (len > 0)
	*dst = type == TYPE_NUMBER ? u32dst : -1.0*u32dst;
    return len;
}


static int use_or_make_ref(lua_State *L, int index,
			   unsigned int *seen_object_count,
			   struct strbuff *catbuf)
{
    uint8_t numbuf[9];
    size_t len;
    lua_pushvalue(L, index);
    lua_rawget(L, SEEN_OBJECT_IDX);
    if (!lua_isnil(L, -1)) {
	len = freeze_uint(lua_tointeger(L, -1), TYPE_REF, numbuf);
	strbuff_addlstring(catbuf, (void *)numbuf, len);
	lua_pop(L, 1);
	return 1;
    }
    lua_pop(L, 1);
    lua_pushvalue(L, index);
    lua_pushinteger(L, ++*seen_object_count);
    lua_rawset(L, SEEN_OBJECT_IDX);
    return 0;
}

static void freeze_recursive(lua_State *L,
			     int index,
			     unsigned int *seen_object_count,
			     unsigned int *seen_upvalue_count,
			     struct strbuff *catbuf,
			     bool merge_dupl_strs,
			     bool strip_debug)
{
    uint8_t numbuf[9];
    size_t len;

    // Convert index to bottom relative.
    if (index < 0)
	index += 1 + lua_gettop(L);

    switch(lua_type(L, index)) {
    case LUA_TNIL:
	strbuff_addchar(catbuf, VALUE_NIL);
	return;
    case LUA_TBOOLEAN:
	strbuff_addchar(catbuf,
			lua_toboolean(L, index) ? VALUE_TRUE : VALUE_FALSE);
	return;
    case LUA_TNUMBER:
	len = freeze_num(lua_tonumber(L, index), numbuf);
	strbuff_addlstring(catbuf, (void *)numbuf, len);
	return;
    case LUA_TSTRING:
	if (!merge_dupl_strs ||
	    !use_or_make_ref(L, index, seen_object_count, catbuf)) {
	    const char *str = lua_tolstring(L, index, &len);
	    strbuff_addlstring(catbuf, (void *)numbuf,
			       freeze_uint(len, TYPE_STRING, numbuf));
	    strbuff_addlstring(catbuf, str, len);
	}
	return;
    case LUA_TTABLE:
	if (!use_or_make_ref(L, index, seen_object_count, catbuf)) {
	    if (lua_getmetatable(L, index)) {
		lua_pop(L, 1);
		break;
	    } else {
		unsigned int array_size = lua_objlen(L, index);
		len = freeze_uint(array_size, TYPE_TABLE, numbuf);
		strbuff_addlstring(catbuf, (void *)numbuf, len);
		for (int i = 1; i <= array_size; i++) {
		    lua_rawgeti(L, index, i);
		    freeze_recursive(L, -1, seen_object_count,
				     seen_upvalue_count, catbuf,
				     merge_dupl_strs, strip_debug);

		    lua_pop(L, 1);
		}
		
		if (array_size > 0)
		    lua_pushinteger(L, array_size);
		else
		    lua_pushnil(L);

		while (lua_next(L, index)) {
		    freeze_recursive(L, -2, seen_object_count,
				     seen_upvalue_count, catbuf,
				     merge_dupl_strs, strip_debug);
		    freeze_recursive(L, -1, seen_object_count,
				     seen_upvalue_count, catbuf,
				     merge_dupl_strs, strip_debug);
		    lua_pop(L, 1);
		}
		strbuff_addchar(catbuf, TABLE_END);
	    }
	}
	return;
    case LUA_TFUNCTION:
	if (!use_or_make_ref(L, index, seen_object_count, catbuf)) {
	    if (lua_iscfunction(L, index))
		break;
	    strbuff_addchar(catbuf, LUA_CLOSURE);
	    lua_pushvalue(L, index);
	    lua_pushboolean(L, strip_debug);
	    lua_pushvalue(L, lua_upvalueindex(1));
	    lua_insert(L, -3);
	    lua_call(L, 2, 1);
	    if (!merge_dupl_strs ||
		!use_or_make_ref(L, -1, seen_object_count, catbuf)) {
		const char *str = lua_tolstring(L, -1, &len);
		strbuff_addlstring(catbuf, (void *)numbuf,
				   freeze_uint(len, TYPE_STRING, numbuf));
		strbuff_addlstring(catbuf, str, len);
	    }
	    lua_pop(L, 1);
	    unsigned int upvalue_index = 1;
	    while (lua_getupvalue(L, index, upvalue_index)) {
		void *ident = lua_upvalueid(L, index, upvalue_index);
		lua_pushlightuserdata(L, ident);
		lua_rawget(L, SEEN_UPVALUE_IDX);
		if (!lua_isnil(L, -1)) {
		    len =
			freeze_uint(lua_tointeger(L, -1), TYPE_UVREF, numbuf);
		    strbuff_addlstring(catbuf, (void *)numbuf, len);
		    lua_pop(L, 2);
		} else {
		    lua_pop(L, 1);
		    lua_pushlightuserdata(L, ident);
		    lua_pushinteger(L, ++*seen_upvalue_count);
		    lua_rawset(L, SEEN_UPVALUE_IDX);
		    len = freeze_uint(upvalue_index, TYPE_UINT, numbuf);
		    strbuff_addlstring(catbuf, (void *)numbuf, len);
		    freeze_recursive(L, -1, seen_object_count,
				     seen_upvalue_count, catbuf,
				     merge_dupl_strs, strip_debug);
		    lua_pop(L, 1);
		}
		upvalue_index++;
	    }
	    strbuff_addchar(catbuf, TABLE_END);
	}
	return;
    default:
	if (use_or_make_ref(L, index, seen_object_count, catbuf))
	    return;
    }

    if (lua_isnil(L, USER_SERIALIZER_IDX))
	goto bugout;
    int curtop = lua_gettop(L);
    lua_pushvalue(L, USER_SERIALIZER_IDX);
    lua_pushvalue(L, index);
    lua_call(L, 1, LUA_MULTRET);
    int rvals = lua_gettop(L)-curtop;
    if (rvals == 0 || !lua_toboolean(L, curtop+1))
	goto bugout;
    if (!lua_isfunction(L, curtop+1)) {
	if (rvals == 1)
	    lua_pushnil(L);
	else
	    lua_settop(L, curtop+2);
	int replace_ix=index==1 ? curtop+1 : curtop;
	lua_replace(L, replace_ix);
	lua_settop(L, replace_ix);
	freeze_recursive(L, -1, seen_object_count,
			 seen_upvalue_count, catbuf,
			 merge_dupl_strs, strip_debug);
	return;
    }
    if (lua_iscfunction(L, -1))
	luaL_error(L, "Constructor must be a Lua function!");
    strbuff_addchar(catbuf, CONSTRUCTOR);
    freeze_recursive(L, -1, seen_object_count, seen_upvalue_count,
		     catbuf, merge_dupl_strs, strip_debug);
    lua_pop(L, 1);
    return;

bugout: {
	const char *typename;

	if (lua_type(L, index) == LUA_TTABLE)
	    typename = "table+metatable";
	else if (lua_iscfunction(L, index))
	    typename = "C function";
	else
	    typename = luaL_typename(L, index);

	luaL_error(L, "Can't serialize type: %s", typename);
    }
}

int freezer_freeze(lua_State *L)
{
    struct strbuff catbuf;
    unsigned int seen_object_count = 0;

    // Handle trivial cases
    switch(lua_type(L, 1)) {
    case LUA_TNONE:
    case LUA_TNIL:
	lua_pushlstring(L, STRLIT(VALUE_NIL), 1);
	break;
    case LUA_TBOOLEAN:
	lua_pushlstring(L,
			lua_toboolean(L, 1) ?
			STRLIT(VALUE_TRUE) : STRLIT(VALUE_FALSE),
			1);
	break;
    case LUA_TNUMBER: {
	char numbuf[9];
	lua_pushlstring(L, numbuf,
			freeze_num(lua_tonumber(L, 1), (uint8_t *)numbuf));
	break;
    }
    case LUA_TSTRING: {
	size_t len;
	char numbuf[9];
	lua_newtable(L);
	lua_replace(L, 2);
	strbuff_buffinit (L, 2, &catbuf);
	const char *str = lua_tolstring(L, 1, &len);
	strbuff_addlstring(&catbuf, (void *)numbuf,
			   freeze_uint(len, TYPE_STRING, (uint8_t *)numbuf));
	strbuff_addlstring(&catbuf, str, len);
	strbuff_pushresult(&catbuf);
	break;
    }
    case LUA_TTABLE: {
	lua_pushnil(L);
	if (!lua_next(L, 1)) {
	    lua_newtable(L);
	    strbuff_buffinit (L, 2, &catbuf);
	    strbuff_addchar(&catbuf, TYPE_TABLE);
	    strbuff_addchar(&catbuf, TABLE_END);
	    strbuff_pushresult(&catbuf);
	    break;
	}
	lua_pop(L, 2);
    }
    default:
	goto complex_type;
    }

#ifdef MAGIC_COOKIE
    lua_pushlstring(L, magic_header, sizeof(magic_header));
    lua_insert(L, -2);
    lua_concat(L, 2);
#endif
    return 1;

complex_type:
    strbuff_buffinit (L, STRING_ACCUMULATOR_IDX, &catbuf);
#ifdef MAGIC_COOKIE
    strbuff_addlstring(&catbuf, (void *)magic_header, sizeof(magic_header));
#endif

    if (lua_gettop(L) == 1)
	lua_newtable(L); // No preload
    else if (!lua_istable(L, 2)) {
	lua_newtable(L); // ditto
	lua_replace(L, 2);
    } else {
	seen_object_count = lua_objlen(L, 2);
	lua_newtable(L); // Seen object table
	int top = lua_gettop(L);
	if (seen_object_count > 0) {
	    uint8_t numbuf[9];

	    strbuff_addchar(&catbuf, ALLOCATE_REFS);
	    int len = freeze_uint(seen_object_count, TYPE_UINT, numbuf);
	    strbuff_addlstring(&catbuf, (void *)numbuf, len);

	    for (unsigned int i = 1; i <= seen_object_count; i++) {
		lua_rawgeti(L, 2, i);
		if (lua_isnil(L, -1)) {
		    lua_pop(L, 1);
		    break;;
		}
		lua_pushinteger(L, i);
		lua_rawset(L, top);
	    }
	}
	lua_replace(L, 2);
    }

    bool merge_dupl_strs;
    bool strip_debug;
    merge_dupl_strs = lua_toboolean(L, 3);     // Default false
    strip_debug = lua_toboolean(L, 4);         // Default false
    if (lua_gettop(L) >= 5) {
      lua_settop(L, 5);
	lua_insert(L, 3);
    } else
	lua_pushnil(L);
    lua_settop(L, 3);

    lua_newtable(L); // Seen upvalue table
    lua_newtable(L); // String accumulator
    unsigned int seen_upvalue_count = 0;

    if (merge_dupl_strs)
	strbuff_addchar(&catbuf, MERGE_DUPL_STRS);

    freeze_recursive(L,
		     1,
		     &seen_object_count,
		     &seen_upvalue_count,
		     &catbuf,
		     merge_dupl_strs,
		     strip_debug);

    strbuff_pushresult(&catbuf);
    return 1;
}

int freezer_self_call(lua_State *L)
{
    lua_remove(L, 1);
    return freezer_freeze(L);
}

static char *end_of_data = "Premature end of data in serialization";
static char *invalid_data = "Invalid data in serialization";


static uint32_t thaw_string_header(lua_State *L, uint8_t **src,
				   size_t *available)
{
    uint32_t string_len;
    uint8_t type;
    int used = thaw_uint(*src, &string_len, &type, *available);
    if (!used) luaL_error(L, end_of_data);
    *src += used;
    *available -= used;
    if (string_len > *available) luaL_error(L, end_of_data);
    return string_len;
}

#undef SEEN_OBJECT_IDX
#undef SEEN_UPVALUE_IDX
#define SEEN_OBJECT_IDX 3
#define SEEN_UPVALUE_IDX (SEEN_OBJECT_IDX+1)

static void thaw_recursive(lua_State *L, uint8_t **src, size_t *available,
			   unsigned int *seen_object_count,
			   unsigned int *seen_upvalue_count,
			   bool merge_dupl_strs)
{
    if (*available < 0)
	luaL_error(L, end_of_data);

    switch (**src) {
    case VALUE_NIL:
	lua_pushnil(L);
	++*src;
	--*available;
	return;
    case VALUE_FALSE:
    case VALUE_TRUE:
	lua_pushboolean(L, **src == VALUE_TRUE);
	++*src;
	--*available;
	return;
    case LUA_CLOSURE:
	++*src;
	--*available;
	int code_position = ++*seen_object_count;
	size_t codelen;
	const char *code;
	if (merge_dupl_strs) {
	    thaw_recursive(L, src, available, seen_object_count,
			   seen_upvalue_count, merge_dupl_strs);
	    if (lua_type(L, -1) != LUA_TSTRING)
		luaL_error(L, invalid_data);
	    code = lua_tolstring(L, -1, &codelen);
	} else {
	    if ((**src & 0xE0) != TYPE_STRING)
		luaL_error(L, "Expecting string");
	    codelen = thaw_string_header(L, src, available);
	    code = (const char *)*src;
	    *src += codelen;
	    *available -= codelen;
	}
	if (luaL_loadbuffer(L, code, codelen, "Serialized code"))
	    luaL_error(L, invalid_data);
	if (merge_dupl_strs)
	    lua_remove(L, -2);
	lua_pushvalue(L, -1);
	lua_rawseti(L, SEEN_OBJECT_IDX, code_position);
	int upvalueix=1;
	while (*available > 0 && **src != TABLE_END) {
	    uint8_t type;
	    uint32_t result;
	    int used = thaw_uint(*src, &result, &type, *available);
	    if (!used) luaL_error(L, end_of_data);
	    *available -= used;
	    *src += used;
	    switch(type) {
	    case TYPE_UINT: {
		uint32_t *uvent = lua_newuserdata(L, 2*sizeof(uint32_t));
		uvent[0] = code_position;
		uvent[1] = result;
		lua_rawseti(L, SEEN_UPVALUE_IDX, ++*seen_upvalue_count);
		thaw_recursive(L, src, available, seen_object_count,
			       seen_upvalue_count, merge_dupl_strs);
		lua_setupvalue(L, -2, result);
		break;
	    }
	    case TYPE_UVREF: {
		lua_rawgeti(L, SEEN_UPVALUE_IDX, result);
		if (lua_isnil(L, 1))
		    luaL_error(L, invalid_data);
		uint32_t *uvent = lua_touserdata(L, -1);
		lua_rawgeti(L, SEEN_OBJECT_IDX, uvent[0]);
		lua_upvaluejoin(L, -3, upvalueix, -1, uvent[1]);
		lua_pop(L, 2);
		break;
	    }
	    default:
		luaL_error(L, invalid_data);
	    }
	    upvalueix++;
	}
	if (**src != TABLE_END || !*available)
	    luaL_error(L, "Can't find end of table");
	++*src;
	--*available;
	return;
    case CONSTRUCTOR:
	++*src;
	--*available;
	thaw_recursive(L, src, available, seen_object_count,
		       seen_upvalue_count, merge_dupl_strs);
	lua_call(L, 0, 1);
	return;
    }
    
    switch (**src & 0xE0) {
    case TYPE_NUMBER:
    case TYPE_NEGATIVE_INT: { // Number type.
	double result;
	int used = thaw_num(*src, &result, *available);
	if (!used) {
	    luaL_error(L, end_of_data);
	    // This never runs, but the compiler flags the out-param otherwise.
	    return;
	}
	*src += used;
	*available -= used;
	lua_pushnumber(L, result);
	break;
    }
    case TYPE_STRING: {
	uint32_t len = thaw_string_header(L, src, available);
	lua_pushlstring(L, (const char *)*src, len);
	*src += len;
	*available -= len;
	if (merge_dupl_strs) {
	    lua_pushvalue(L, -1);
	    lua_rawseti(L, SEEN_OBJECT_IDX, ++*seen_object_count);
	}
	break;
    }
    case TYPE_TABLE: {
	uint32_t result;
	uint8_t type;
	int used = thaw_uint(*src, &result, &type, *available);
	if (!used) luaL_error(L, end_of_data);
	*src += used;
	*available -= used;
	lua_createtable(L, result, 0);
	lua_pushvalue(L, -1);
	lua_rawseti(L, SEEN_OBJECT_IDX, ++*seen_object_count);
	for (int i = 1; i <= result; i++) {
	    thaw_recursive(L, src, available, seen_object_count,
			   seen_upvalue_count, merge_dupl_strs);
	    lua_rawseti(L, -2, i);
	}
	while (*available > 0 && **src != TABLE_END) {
	    thaw_recursive(L, src, available, seen_object_count,
			   seen_upvalue_count, merge_dupl_strs);
	    thaw_recursive(L, src, available, seen_object_count,
			   seen_upvalue_count, merge_dupl_strs);
	    lua_rawset(L, -3);
	}
	if (**src != TABLE_END || !*available)
	    luaL_error(L, "Can't find end of table");
	++*src;
	--*available;
	break;
    }
    case TYPE_REF: {
	uint32_t result;
	uint8_t type;
	int used = thaw_uint(*src, &result, &type, *available);
	if (!used) luaL_error(L, end_of_data);
	*src += used;
	*available -= used;
	lua_rawgeti(L, SEEN_OBJECT_IDX, result);
	if (!lua_isnil(L, -1))
	    break;
	luaL_error(L, invalid_data);
    }
    default:
	luaL_error(L, invalid_data);
    }
}

static char extra[] = "Extra bytes";

static int freezer_thaw_something(lua_State *L, uint8_t *buf, size_t len)
{
#ifdef MAGIC_COOKIE
    size_t magiclen;
    if (len < sizeof(magic_header) ||
	memcmp(magic_header, buf, sizeof(magic_header)))
	luaL_error(L, invalid_data);
    
    len -= sizeof(magic_header);
    buf += sizeof(magic_header);
#endif

    // Thaw trivial values.
    if (len > 0) {
	switch(*buf) {
	case VALUE_NIL:
	    len--;
	    lua_pushnil(L);
	    break;
	case VALUE_TRUE:
	    len--;
	    lua_pushboolean(L, true);
	    break;
	case VALUE_FALSE:
	    len--;
	    lua_pushboolean(L, false);
	    break;
	case TYPE_TABLE:
	    if (len == 2 && buf[1] == TABLE_END) {
		lua_newtable(L);
		len = 0;
		break;
	    }
	default:
	    switch(*buf & 0xE0) {
	    case TYPE_STRING: {
		size_t avail = len;
		uint32_t slen = thaw_string_header(L, &buf, &avail);
		lua_pushlstring(L, (void *)buf, slen);
		len = avail - slen;
		break;
	    }
	    case TYPE_NUMBER:
	    case TYPE_NEGATIVE_INT: { // Number type.
		double result;
		int used = thaw_num(buf, &result, len);
		if (!used)
		    luaL_error(L, end_of_data);
		len -= used;
		lua_pushnumber(L, result);
		break;
	    }
	    default:
		goto complex_type;
	    }
	}

	if (len != 0)
	    luaL_error(L, extra);
	return 1;
    }

complex_type:

    if (lua_gettop(L) == 1) {
	lua_newtable(L);   // Empty initial seen objects
    } else {
	luaL_checktype(L, 2, LUA_TTABLE);
	lua_settop(L, 2);
    }

    bool merge_dupl_strs = false;

    uint32_t seen_object_count = 0;
    uint32_t seen_upvalue_count = 0;

    if (len > 0 && *buf == MERGE_DUPL_STRS) {
	buf++;
	len--;
	merge_dupl_strs = true;
    }

    // Make seen object table.
    lua_newtable(L);
    if (len > 0 && *buf == ALLOCATE_REFS) {
	buf++;
	len--;
	uint8_t type;
	int result = thaw_uint(buf, &seen_object_count, &type, len);
	if (!result) luaL_error(L, end_of_data);
	if (type != TYPE_UINT)
	    luaL_error(L, invalid_data);
	buf += result;
	len -= result;
	for (unsigned int i = 1; i <= seen_object_count; i++) {
	    lua_rawgeti(L, 2, i);
	    if (lua_isnil(L, -1)) {
		lua_pop(L, 1);
		break;
	    }
	    lua_rawseti(L, SEEN_OBJECT_IDX, i);
	}
    }

    lua_newtable(L); // seen upvalues table
    
    thaw_recursive(L, &buf, &len, &seen_object_count,
		   &seen_upvalue_count, merge_dupl_strs);
    
    if (len) luaL_error(L, extra);

    return 1;
}

int freezer_thaw_buffer(lua_State *L)
{
    size_t len;
    uint8_t *buf;
    if (lua_islightuserdata(L, 1)) {
	len = (size_t)luaL_checkinteger(L, 2);
	buf = lua_touserdata(L, 1);
	lua_remove(L, 1);
    } else
	buf = (uint8_t *)luaL_checklstring(L, 1, &len);

    if (len < 0)
	luaL_error(L, "Invalid length in freeze string");

    return freezer_thaw_something(L, buf, len);
}

int freezer_thaw(lua_State *L)
{
    luaL_checktype(L, 1, LUA_TSTRING);
    size_t len;
    uint8_t *buf = (uint8_t *)lua_tolstring(L, 1, (size_t *)&len);
    return freezer_thaw_something(L, buf, len);
}

static int clone(lua_State *L)
{
    freezer_freeze(L);
    lua_replace(L, 1);
    lua_settop(L, 2);
    return freezer_thaw(L);
}

LUALIB_API int luaopen_freezer(lua_State *L)
{
    // Detect endianness.  Bizarre byte sex is not supported.
    union { uint32_t in; uint8_t out[sizeof(uint32_t)]; } pun = { .in = 1 };
    big_endian = pun.out[0] == 0;

    // The usual registration doesn't produce synonyms for
    // the same C function, so we build the table ourselves.
    lua_newtable(L);

    // LuaJIT doesn't expose the strip parameter in the C API,
    // and the overhead of the callout is minor.
    lua_getglobal(L, "string");
    lua_getfield(L, -1, "dump");
    lua_remove(L, -2);
    lua_newtable(L);
    lua_pushvalue(L, -2);
    lua_pushcclosure(L, freezer_self_call, 1);
    lua_setfield(L, -2, "__call");
    lua_setmetatable(L, -3);
    lua_pushvalue(L, -1);
    lua_pushcclosure(L, clone, 1);
    lua_setfield(L, -3, "clone");
    lua_pushcclosure(L, freezer_freeze, 1);
    lua_pushvalue(L, -1);
    lua_setfield(L, -3, "freeze");
    lua_setfield(L, -2, "encode");
    lua_pushcfunction(L, freezer_thaw);
    lua_pushvalue(L, -1);
    lua_setfield(L, -3, "thaw");
    lua_setfield(L, -2, "decode");

    return 1;
}
