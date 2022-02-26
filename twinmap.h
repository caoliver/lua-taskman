#pragma once
extern bool twinmap_uses_long_addresses;
extern void *allocate_twinmap(size_t *size);
extern void free_twinmap(void *base, size_t size);
