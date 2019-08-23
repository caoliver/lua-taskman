extern void *allocate_twinmap(size_t *size, size_t *where);
extern void *allocate_map(size_t *size, size_t *where);
extern void free_twinmap(void *base, size_t size, size_t where);
extern void free_map(void *base, size_t size, size_t where);
