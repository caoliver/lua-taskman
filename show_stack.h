void show_stack(lua_State *L)
{
    printf("TOP IS %d\n", lua_gettop(L));
    for (int i = lua_gettop(L); i > 0; i--)
	printf("\t%d:\t%s\n", i, lua_typename(L, lua_type(L, i)));
}
