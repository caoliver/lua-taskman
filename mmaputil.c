#define _GNU_SOURCE         /* See feature_test_macros(7) */
#include <unistd.h>
#include <stdint.h>
#include <sys/syscall.h>   /* For SYS_xxx definitions */
#include <err.h>
#include <sys/mman.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <fcntl.h>

#include "mmaputil.h"

#include "lua_head.h"

#ifndef MAP_32BIT
#define MAP_32BIT 0
#endif

static unsigned int page_mask, page_size;

const char *cdef_string =
    "uint8_t *allocate_twinmap(size_t *, size_t *where);"
    "uint8_t *allocate_map(size_t *, size_t *where);"
    "void free_twinmap(void *base, size_t size, size_t where);"
    "void free_map(void *base, size_t size, size_t where);"
   ;

static int mapfd;
static size_t offset = 0;

__attribute__((constructor)) static void load_time_init()
{
    page_size = sysconf(_SC_PAGESIZE);
    page_mask = page_size - 1;

    if ((mapfd = syscall(__NR_memfd_create, "shmbuf", 0)) < 0)
	err(1, NULL);
}

#define badmap (void *)-1

static pthread_mutex_t allocate_mutex = PTHREAD_MUTEX_INITIALIZER;

void *allocate_twinmap(size_t *size, size_t *where)
{
    pthread_mutex_lock(&allocate_mutex);
    *size = *size + page_mask & ~page_mask;
    size_t newsize = offset + *size;
    if (ftruncate(mapfd, newsize) < 0)
	err(1, NULL);

    void *buf = mmap(NULL, 2 * *size, PROT_NONE,
	       MAP_PRIVATE | MAP_ANONYMOUS | MAP_32BIT, -1, 0);
    if (buf == badmap ||
	mmap(buf, *size, PROT_READ | PROT_WRITE,
	     MAP_SHARED | MAP_FIXED, mapfd, offset) == (void *)-1 ||
	mmap(buf + *size, *size, PROT_READ | PROT_WRITE,
	     MAP_SHARED | MAP_FIXED, mapfd, offset) == (void *)-1)
	err(1, NULL);
    *where = offset;
    offset = newsize;
    pthread_mutex_unlock(&allocate_mutex);
    return buf;
}

void free_twinmap(void *base, size_t size, size_t where)
{
    munmap(base, size*2);
    fallocate(mapfd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, where, size);
}

void *allocate_map(size_t *size, size_t *where)
{
    pthread_mutex_lock(&allocate_mutex);
    *size = *size + page_mask & ~page_mask;
    size_t newsize = offset + *size;
    if (ftruncate(mapfd, newsize) < 0)
	err(1, NULL);

    void *buf = mmap(NULL, *size, PROT_READ | PROT_WRITE,
		     MAP_SHARED | MAP_32BIT, mapfd, offset);
    if (buf == badmap)
	err(1, NULL);
    *where = offset;
    offset = newsize;
    pthread_mutex_unlock(&allocate_mutex);
    return buf;
}

void free_map(void *base, size_t size, size_t where)
{
    munmap(base, size);
    fallocate(mapfd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, where, size);
}

