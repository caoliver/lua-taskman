#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#define STRBUFFSIZ (BUFSIZ > 16384 ? 8192 : BUFSIZ)

struct strbuff {
    lua_State *L;
    char buf[STRBUFFSIZ];
    char *base, *ptr;
    unsigned int len, in_use;
    int stack_index;
};

// The difference between these and luaL_Buffers is that one specifies
// the index of an (initially nil) stack slot to save the intermediate
// results.
void strbuff_buffinit(lua_State *L, int lua_index, struct strbuff *buf);

void strbuff_addlstring(struct strbuff *buf, const char *str, size_t len);

void strbuff_addchar(struct strbuff *buf, char ch);

void strbuff_pushresult(struct strbuff *buf);
