#define _GNU_SOURCE         /* See feature_test_macros(7) */
#include <unistd.h>
#include <sys/syscall.h>   /* For SYS_xxx definitions */
#include <err.h>
#include <sys/mman.h>
#include <fcntl.h>

#ifndef MAP_32BIT
#define MAP_32BIT 0
#endif

static unsigned int page_mask;

const char *cdef_string =
    "uint8_t *allocate_twinmap(size_t *);"
    "void free_twinmap(void *base, size_t size);"
   ;

__attribute__((constructor)) static void load_time_init()
{
    page_mask = (unsigned int)sysconf(_SC_PAGESIZE) - 1;
}

#define badmap (void *)-1

void *allocate_twinmap(size_t *size)
{
    *size = *size + page_mask & ~page_mask;

    int mapfd = syscall(__NR_memfd_create, "shmbuf", 0);
    if (mapfd < 0 ||
	ftruncate(mapfd, *size) < 0)
	err(1, NULL);
    
    void *buf = mmap(NULL, 2 * *size, PROT_NONE,
	       MAP_PRIVATE | MAP_ANONYMOUS | MAP_32BIT, -1, 0);

    if (buf == badmap ||
	mmap(buf, *size, PROT_READ | PROT_WRITE,
	     MAP_SHARED | MAP_FIXED, mapfd, 0) == badmap ||
	mmap(buf + *size, *size, PROT_READ | PROT_WRITE,
	     MAP_SHARED | MAP_FIXED, mapfd, 0) == badmap)
	err(1, NULL);

    close(mapfd);
    return buf;
}

void free_twinmap(void *base, size_t size)
{
    munmap(base, size*2);
}
