 /*********************************************************************/
 /* This is a substitute for luaL buffers.  It improves upon them by  */
 /* using an expanding userdata buffer rather than the stack.  Thus,   */
 /* one can use strbuffs with recursive structure walkers where the   */
 /* luaL_buffer functions would corrupt the stack.  The buffer starts */
 /* using an internal fixed size array, so for sufficiently short     */
 /* data, no lua allocation occurs until the final result is pushed.  */
 /*********************************************************************/

#include <string.h>
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#include "strbuff.h"

// Initialize the buffer and record the stack index of the
// work slot used to stash the userdata if it's needed.
void strbuff_buffinit(lua_State *L, int work_slot, struct strbuff *buf)
{
    buf->L = L;
    buf->ptr = buf->base = buf->buf;
    buf->in_use = 0;
    buf->len = STRBUFFSIZ;
    buf->stack_index = work_slot;
}

static void expand_buf(struct strbuff *buf, unsigned int needed)
{
    unsigned int new_size = buf->len;
    while (needed > new_size - buf->in_use)
	new_size = 2*new_size;
    char *new_ptr = lua_newuserdata(buf->L, new_size);
    memcpy(new_ptr, buf->base, buf->in_use);
    buf->base = new_ptr;
    buf->ptr = new_ptr + buf->in_use;
    buf->len = new_size;
    lua_replace(buf->L, buf->stack_index);
}

// As with luaL_addlstring.
void strbuff_addlstring(struct strbuff *buf, const char *str, size_t len)
{
    if (buf->in_use + len > buf->len)
	expand_buf(buf, len);
    memcpy(buf->ptr, str, len);
    buf->ptr += len;
    buf->in_use += len;
}

// As with luaL_addchar.
void strbuff_addchar(struct strbuff *buf, char ch)
{
    if (buf->in_use == buf->len)
	expand_buf(buf, 1);
    *buf->ptr++ = ch;
    buf->in_use++;
}

void strbuff_pushresult(struct strbuff *buf)
{
    lua_pushlstring(buf->L, buf->base, buf->in_use);
    lua_pushnil(buf->L);
    lua_replace(buf->L, buf->stack_index);
    return;
}
