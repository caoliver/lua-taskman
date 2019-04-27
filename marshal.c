/*********************************************************************/
/* Marshal a serializer/deserializer for Lua values inspired by	     */
/* Richard Hundt's lmarshal and various readings on CBOR.	     */
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
#include <byteswap.h>
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#include <assert.h>

// Do you want to start the serialization with a magic cookie?
// #define MAGIC_COOKIE { 'L', 'M', 'S', 'H' }

#ifdef MAGIC_COOKIE
unsigned char magic_header[] = MAGIC_COOKIE;
#endif

static int big_endian=0;

#define TYPE_DOUBLE 14
#define TYPE_INT8 15
#define TYPE_INT16 16
#define TYPE_INT32 17

static unsigned int encode_num(double src, uint8_t *dst)
{
    int32_t trunc = src;
    if ((double)trunc != src) {
	*dst++ = TYPE_DOUBLE;
	if (big_endian) {
	    union { double in; uint64_t out; } pun = { .in = src };
	    pun.out = bswap_64(pun.out);
	    memcpy(dst, &pun.out, 8);
	} else
	    memcpy(dst, &src, sizeof(src));
	return 9;
    }
    if ( trunc > -15 && trunc < 14 ) {
	*dst = trunc & 0x01F;
	return 1;
    }
    if (trunc >= -128 && trunc < 128) {
	*dst = TYPE_INT8;
	dst[1] = trunc & 0xFF;
	return 2;
    }
    if (trunc >= -32768 && trunc < 32768) {
	*dst = TYPE_INT16;
	dst[1] = trunc & 0xFF;
	dst[2] = (trunc>>8) & 0xFF;
	return 3;
    } 
    *dst = TYPE_INT32;
    if (big_endian)
	trunc = bswap_32(trunc);
    memcpy(dst+1, &trunc, 4);
    return 5;
}

static int decode_num(const uint8_t *src, double *dst, size_t available)
{
    switch (src[0]) {
    case TYPE_DOUBLE: {
	if (available < 9) return 0;
	union { uint64_t in; double out; } pun;
	memcpy(&pun.in, src+1, sizeof(pun.in));
	if (big_endian)
	    pun.in = bswap_64(pun.in);
	*dst = pun.out;
	return 9;
    };
    case TYPE_INT8:
	if (available < 2) return 0;
	*dst = *(int8_t *)&src[1];
	return 2;
    case TYPE_INT16: {
	if (available < 3) return 0;
	int16_t val = src[2] << 8 | src[1];
	*dst = val;
	return 3;
    }
    case TYPE_INT32: {
	if (available < 5) return 0;
	int32_t val;
	memcpy(&val, src+1, sizeof(val));
	if (big_endian)
	    val = bswap_32(val);
	*dst = val;
	return 5;
    }
    default:
	if (available < 1) return 0;
	*dst = *(int8_t *)src;
	return 1;
    }
}

#define TYPE_UINT32 31
#define TYPE_UINT24 30
#define TYPE_UINT16 29
#define TYPE_UINT8 28

static unsigned int encode_uint(uint32_t src, uint8_t type, uint8_t *dst)
{
    if (src < 28) {
	*dst = type | src;
	return 1;
    }
    if (src < 256) {
	*dst = type | TYPE_UINT8;
	dst[1] = src;
	return 2;
    }
    if (src < 65536) {
	*dst = type | TYPE_UINT16;
	dst[1] = src & 0xFF;
	dst[2] = src >> 8;
	return 3;
    }
    if (src < 1<<24) {
	*dst = type | TYPE_UINT24;
	dst[1] = src & 0xFF;
	dst[2] = src >> 8 & 0xFF;
	dst[3] = src >> 16;
	return 4;
    }
    *dst = type | TYPE_UINT32;
    if (big_endian)
	src = bswap_32(src);
    memcpy(dst+1, &src, 4);
    return 5;
}

static int decode_uint(const uint8_t *src, uint32_t *dst,
		       uint8_t *type, size_t available)

{
    if (available < 1) return 0;
    *type = *src & 0xE0;
    
    switch(src[0] & 0x1F) {
    case TYPE_UINT8:
	if (available < 2) return 0;
	*dst = src[1];
	return 2;
    case TYPE_UINT16:
	if (available < 3) return 0;
	*dst = src[2] << 8 | src[1];
	return 3;
    case TYPE_UINT24:
	if (available < 4) return 0;
	*dst = (src[3]<<8 | src[2])<<8 | src[1];
	return 4;
    case TYPE_UINT32:
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

#define SEEN_OBJECT_IDX 3
#define SEEN_UPVALUE_IDX 4

// Explicit constants and special headers
#define ALLOCATE_REFS 32
#define MERGE_DUPL_STRS 33
#define TABLE_END 34
#define LUA_CLOSURE 35
#define CONSTRUCTOR 36

#define VALUE_NIL 37
#define VALUE_FALSE 38
#define VALUE_TRUE 39

#define STRLIT(VAL) (char []){VAL}

// Types aside from constants (32-63)
#define TYPE_NUMBER 0
#define TYPE_STRING 64
#define TYPE_REF 96
#define TYPE_TABLE 128
#define TYPE_UVREF 160
#define TYPE_UINT 192

static int bcwriter(lua_State *L, const void *p, size_t sz, void *buf)
{
    luaL_addlstring(buf, p, sz);
    return 0;
}

static int push_bytecode(lua_State *L)
{
    luaL_Buffer bcbuf;

    return 1;
}

int use_or_make_ref(lua_State *L, int index,
		    unsigned int *seen_object_count, luaL_Buffer *catbuf)
{
    uint8_t numbuf[9];
    size_t len;

    lua_pushvalue(L, index);
    lua_rawget(L, SEEN_OBJECT_IDX);
    if (!lua_isnil(L, -1)) {
	len = encode_uint(lua_tointeger(L, -1), TYPE_REF, numbuf);
	luaL_addlstring(catbuf, (void *)numbuf, len);
	lua_pop(L, 1);
	return 1;
    }
    lua_pop(L, 1);
    lua_pushvalue(L, index);
    lua_pushinteger(L, ++*seen_object_count);
    lua_rawset(L, SEEN_OBJECT_IDX);
    return 0;
}

static const char *bad_make_restore =
    "__make_restore must return a lua closure";

static void encode_recursive(lua_State *L,
			     int index,
			     unsigned int *seen_object_count,
			     unsigned int *seen_upvalue_count,
			     luaL_Buffer *catbuf,
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
	luaL_addlstring(catbuf, STRLIT(VALUE_NIL), 1);
	break;
    case LUA_TBOOLEAN:
	luaL_addlstring(catbuf,
			lua_toboolean(L, index)
			? STRLIT(VALUE_TRUE)
			: STRLIT(VALUE_FALSE),
			1);
	break;
    case LUA_TNUMBER:
	len = encode_num(lua_tonumber(L, index), numbuf);
	luaL_addlstring(catbuf, (void *)numbuf, len);
	break;
    case LUA_TSTRING:
	if (!merge_dupl_strs ||
	    !use_or_make_ref(L, index, seen_object_count, catbuf)) {
	    len = lua_objlen(L, index);
	    len = encode_uint(len, TYPE_STRING, numbuf);
	    luaL_addlstring(catbuf, (void *)numbuf, len);
	    const char *str = lua_tolstring(L, index, &len);
	    luaL_addlstring(catbuf, str, len);
	}
	break;
    case LUA_TTABLE:
	if (!use_or_make_ref(L, index, seen_object_count, catbuf)) {
	    if (luaL_getmetafield(L, index, "__make_restore")) {
		lua_pushvalue(L, index);
		lua_call(L, 1, 1);
		if (!lua_isfunction(L, -1) || lua_iscfunction(L,-1))
		    luaL_error(L, bad_make_restore);
		luaL_addlstring(catbuf, STRLIT(CONSTRUCTOR), 1);
		encode_recursive(L, -1, seen_object_count, seen_upvalue_count,
				 catbuf, merge_dupl_strs, strip_debug);
	    } else {
		unsigned int array_size = lua_objlen(L, index);
		len = encode_uint(array_size, TYPE_TABLE, numbuf);
		luaL_addlstring(catbuf, (void *)numbuf, len);
		for (int i = 1; i <= array_size; i++) {
		    lua_rawgeti(L, index, i);
		    encode_recursive(L, -1, seen_object_count,
				     seen_upvalue_count, catbuf,
				     merge_dupl_strs, strip_debug);
		    lua_pop(L, 1);
		}
		
		if (array_size > 0)
		    lua_pushinteger(L, array_size);
		else
		    lua_pushnil(L);

		while (lua_next(L, index)) {
		    encode_recursive(L, -2, seen_object_count,
				     seen_upvalue_count, catbuf,
				     merge_dupl_strs, strip_debug);
		    encode_recursive(L, -1, seen_object_count,
				     seen_upvalue_count, catbuf,
				     merge_dupl_strs, strip_debug);
		    lua_pop(L, 1);
		}
		luaL_addlstring(catbuf, STRLIT(TABLE_END), 1);
	    }
	}
	break;
    case LUA_TUSERDATA:
	if (!use_or_make_ref(L, index, seen_object_count, catbuf)) {
	    if (!luaL_getmetafield(L, index, "__make_restore"))
		luaL_error(L, "Can't serialize this userdata.");
	    lua_pushvalue(L, index);
	    lua_call(L, 1, 1);
	    if (!lua_isfunction(L, -1) || lua_iscfunction(L,-1))
		luaL_error(L, bad_make_restore);
	    luaL_addlstring(catbuf, STRLIT(CONSTRUCTOR), 1);
	    encode_recursive(L, -1, seen_object_count, seen_upvalue_count,
			     catbuf, merge_dupl_strs, strip_debug);
	}
	break;
    case LUA_TFUNCTION:
	if (!use_or_make_ref(L, index, seen_object_count, catbuf)) {
	    if (lua_iscfunction(L, index))
		luaL_error(L, "Can't serialize a C function.");
	    luaL_addlstring(catbuf, STRLIT(LUA_CLOSURE), 1);
	    lua_pushvalue(L, index);
	    lua_pushboolean(L, strip_debug);
	    lua_pushvalue(L, lua_upvalueindex(1));
	    lua_insert(L, -3);
	    lua_call(L, 2, 1);
	    
	    encode_recursive(L, -1, seen_object_count, seen_upvalue_count,
			     catbuf, merge_dupl_strs, strip_debug);
	    lua_pop(L, 1);
	    unsigned int upvalue_index = 1;
	    while (lua_getupvalue(L, index, upvalue_index)) {
		void *ident = lua_upvalueid(L, index, upvalue_index);
		lua_pushlightuserdata(L, ident);
		lua_rawget(L, SEEN_UPVALUE_IDX);
		if (!lua_isnil(L, -1)) {
		    len =
			encode_uint(lua_tointeger(L, -1), TYPE_UVREF, numbuf);
		    luaL_addlstring(catbuf, (void *)numbuf, len);
		    lua_pop(L, 2);
		} else {
		    lua_pop(L, 1);
		    lua_pushlightuserdata(L, ident);
		    lua_pushinteger(L, ++*seen_upvalue_count);
		    lua_rawset(L, SEEN_UPVALUE_IDX);
		    len = encode_uint(upvalue_index, TYPE_UINT, numbuf);
		    luaL_addlstring(catbuf, (void *)numbuf, len);
		    encode_recursive(L, -1, seen_object_count,
				     seen_upvalue_count, catbuf,
				     merge_dupl_strs, strip_debug);
		    lua_pop(L, 1);
		}
		upvalue_index++;
	    }
	    luaL_addlstring(catbuf, STRLIT(TABLE_END), 1);
	}
	break;
    default:
	luaL_error(L, "Can't serialize type %s", luaL_typename(L, index));
    }
}

static int encode(lua_State *L)
{
    switch (lua_gettop(L)) {
    case 0:
	lua_pushnil(L);    // Empty source
    case 1:
	lua_newtable(L);   // Empty initial seen objects
    }

    bool merge_dupl_strs;
    bool strip_debug;
    merge_dupl_strs = lua_toboolean(L, 3);     // Default false
    strip_debug = lua_toboolean(L, 4);         // Default false
    lua_settop(L, 2);

    lua_newtable(L); // Seen object table
    lua_newtable(L); // Seen upvalue table
    unsigned int seen_object_count = 0;
    unsigned int seen_upvalue_count = 0;

    luaL_Buffer catbuf;
    luaL_buffinit (L, &catbuf);

#ifdef MAGIC_COOKIE
    luaL_addlstring(&catbuf, (void *)magic_header, sizeof(magic_header));
#endif
    
    if (merge_dupl_strs)
	luaL_addlstring(&catbuf, STRLIT(MERGE_DUPL_STRS), 1);

    // Build initial seen objects
    seen_object_count = lua_objlen(L, 2);
    if (seen_object_count > 0) {
	uint8_t numbuf[9];
	
	luaL_addlstring(&catbuf, STRLIT(ALLOCATE_REFS), 1);
	int len = encode_uint(seen_object_count, TYPE_UINT, numbuf);
	luaL_addlstring(&catbuf, (void *)numbuf, len);

	for (unsigned int i = 1; i <= seen_object_count; i++) {
	    lua_rawgeti(L, 2, i);
	    if (lua_isnil(L, -1)) {
		lua_pop(L, 1);
		break;;
	    }
	    lua_pushinteger(L, i);
	    lua_rawset(L, SEEN_OBJECT_IDX);
	}
    }

    encode_recursive(L,
		     1,
		     &seen_object_count,
		     &seen_upvalue_count,
		     &catbuf,
		     merge_dupl_strs,
		     strip_debug);

    lua_pop(L, 1);
    luaL_pushresult(&catbuf);
    return 1;
}

static char *end_of_data = "Premature end of data in serialization";
static char *invalid_data = "Invalid data in serialization";


uint32_t decode_string_header(lua_State *L, uint8_t **src, size_t *available)
{
    uint32_t string_len;
    uint8_t type;
    int used = decode_uint(*src, &string_len, &type, *available);
    if (!used) luaL_error(L, end_of_data);
    if (type != TYPE_STRING)
	luaL_error(L, "Expecting string");
    *src += used;
    *available -= used;
    if (string_len > *available) luaL_error(L, end_of_data);
    return string_len;
}

static void decode_recursive(lua_State *L,
			     uint8_t **src,
			     size_t *available,
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
	(*available)--;
	return;
    case VALUE_FALSE:
    case VALUE_TRUE:
	lua_pushboolean(L, **src == VALUE_TRUE);
	++*src;
	(*available)--;
	return;
    case LUA_CLOSURE:
	++*src;
	(*available)--;
	int code_position = ++*seen_object_count;
	size_t codelen;
	const char *code;
	if (merge_dupl_strs) {
	    decode_recursive(L, src, available, seen_object_count,
			     seen_upvalue_count, merge_dupl_strs);
	    code = lua_tolstring(L, -1, &codelen);
	} else {
	    codelen = decode_string_header(L, src, available);
	    code = *src;
	    *src += codelen;
	    *available -= codelen;
	}
	if (luaL_loadbuffer(L, code, codelen, "Serialized code"))
	    luaL_error(L, "Bad serialized code");
	if (merge_dupl_strs)
	    lua_remove(L, -2);
	lua_pushvalue(L, -1);
	lua_rawseti(L, SEEN_OBJECT_IDX, code_position);
	int upvalueix=1;
	while (*available > 0 && **src != TABLE_END) {
	    uint8_t type;
	    uint32_t result;
	    int used = decode_uint(*src, &result, &type, *available);
	    if (!used) luaL_error(L, end_of_data);
	    *available -= used;
	    *src += used;
	    switch(type) {
	    case TYPE_UINT: {
		lua_newtable(L);
		lua_pushinteger(L, code_position);
		lua_rawseti(L, -2, 1);
		lua_pushinteger(L, result);
		lua_rawseti(L, -2, 2);
		lua_rawseti(L, SEEN_UPVALUE_IDX, ++*seen_upvalue_count);
		decode_recursive(L, src, available, seen_object_count,
				 seen_upvalue_count, merge_dupl_strs);
		lua_setupvalue(L, -2, result);
		break;
	    }
	    case TYPE_UVREF: {
		lua_rawgeti(L, SEEN_UPVALUE_IDX, result);
		if (lua_isnil(L, 1))
		    luaL_error(L, invalid_data);
		lua_rawgeti(L, -1, 1);
		lua_rawget(L, SEEN_OBJECT_IDX);
		lua_rawgeti(L, -2, 2);
		lua_upvaluejoin(L, -4, upvalueix, -2, lua_tointeger(L, -1));
		lua_pop(L, 3);
		break;
	    }
	    default:
		luaL_error(L, invalid_data);
	    }
	    upvalueix++;
	}
	if (**src != TABLE_END || !*available)
	    luaL_error(L, "Can't find end of table");
	--*available; ++*src;
	return;
    case CONSTRUCTOR:
	++*src;
	--*available;
	decode_recursive(L, src, available, seen_object_count,
			 seen_upvalue_count, merge_dupl_strs);
	lua_call(L, 0, 1);
	return;
    }
    
    switch (**src & 0xE0) {
    case TYPE_NUMBER: { // Number type.
	double result;
	int used = decode_num(*src, &result, *available);
	if (!used) {
	    luaL_error(L, end_of_data);
	    // This never runs, but the compiler flags the out-param otherwise.
	    return;
	}
	*available -= used;
	*src += used;
	lua_pushnumber(L, result);
	break;
    }
    case TYPE_STRING: {
	uint32_t len = decode_string_header(L, src, available);
	lua_pushlstring(L, *src, len);
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
	int used = decode_uint(*src, &result, &type, *available);
	if (!used) luaL_error(L, end_of_data);
	*available -= used;
	*src += used;
	lua_createtable(L, result, 0);
	lua_pushvalue(L, -1);
	lua_rawseti(L, SEEN_OBJECT_IDX, ++*seen_object_count);
	for (int i = 1; i <= result; i++) {
	    decode_recursive(L, src, available, seen_object_count,
			     seen_upvalue_count, merge_dupl_strs);
	    lua_rawseti(L, -2, i);
	}
	while (*available > 0 && **src != TABLE_END) {
	    decode_recursive(L, src, available, seen_object_count,
			     seen_upvalue_count, merge_dupl_strs);
	    decode_recursive(L, src, available, seen_object_count,
			     seen_upvalue_count, merge_dupl_strs);
	    lua_rawset(L, -3);
	}
	if (**src != TABLE_END || !*available)
	    luaL_error(L, "Can't find end of table");
	--*available; (*src)++;
	break;
    }
    case TYPE_REF: {
	uint32_t result;
	uint8_t type;
	int used = decode_uint(*src, &result, &type, *available);
	if (!used) luaL_error(L, end_of_data);
	*available -= used;
	*src += used;
	lua_rawgeti(L, SEEN_OBJECT_IDX, result);
	break;
    }
    default:
	luaL_error(L, invalid_data);
    }
	
}

static int decode(lua_State *L)
{
    luaL_checktype(L, 1, LUA_TSTRING);
    size_t len;
    uint8_t *src = (uint8_t *)lua_tolstring(L, 1, (size_t *)&len);
    bool merge_dupl_strs = false;
    
    switch (lua_gettop(L)) {
    case 1:
	lua_newtable(L);   // Empty initial seen objects
    }
    uint32_t seen_object_count = 0;
    uint32_t seen_upvalue_count = 0;

#ifdef MAGIC_COOKIE
    size_t magiclen;
    if (len < sizeof(magic_header) ||
	memcmp(magic_header, src, sizeof(magic_header)))
	luaL_error(L, "Bad magic header");
    
    len -= sizeof(magic_header);
    src += sizeof(magic_header);
#endif
    
    if (len > 0 && *src == MERGE_DUPL_STRS) {
	src++;
	len--;
	merge_dupl_strs = true;
    }

    // Make seen object table.
    lua_newtable(L);
    if (len > 0 && *src == ALLOCATE_REFS) {
	src++;
	len--;
	uint8_t type;
	int result = decode_uint(src, &seen_object_count, &type, len);
	if (!result) luaL_error(L, end_of_data);
	if (type != TYPE_UINT)
	    luaL_error(L, invalid_data);
	src += result;
	len -= result;
	for (unsigned int i = 1; i <= seen_object_count; i++) {
	    lua_rawgeti(L, 2, i);
	    if (lua_isnil(L, -1)) {
		lua_pop(L, 1);
		break;
	    }
	    lua_rawseti(L, 3, i);
	}
    }

    lua_newtable(L); // seen upvalues table
    
    decode_recursive(L, &src, &len, &seen_object_count,
		     &seen_upvalue_count, merge_dupl_strs);
    
    if (len) luaL_error(L, "Extra bytes");

    return 1;
}

static int clone(lua_State *L)
{
    encode(L);
    lua_replace(L, 1);
    lua_settop(L, 2);
    return decode(L);
}

LUALIB_API int luaopen_marshal(lua_State *L)
{
    // Detect endianness.  Bizarre byte sex is not supported.
    union { uint32_t in; uint8_t out[sizeof(uint32_t)]; } pun = { .in = 1 };
    big_endian = pun.out[0] == 0;

    static const luaL_Reg marshal_entry_pts[] = {
	{"decode", decode},
	{"clone", clone},
	{NULL, NULL}
    };
    lua_newtable(L);
    luaL_register(L, NULL, marshal_entry_pts);

    // LuaJIT doesn't expose the strip parameter, and
    // the overhead of the callout is minor.
    lua_getglobal(L, "string");
    lua_getfield(L, -1, "dump");
    lua_pushcclosure(L, encode, 1);
    lua_remove(L, -2);
    lua_setfield(L, -2, "encode");

    return 1;
}
