#pragma once
#include <stdint.h>

struct circbuf {
    uint32_t size, head, tail;
};

#ifdef INLINE_CBUF_CODE
#undef INLINE_CBUF_CODE
#define INLINE_CBUF_CODE inline __attribute__((always_inline)) static
#ifndef INCLUDE_CBUF_CODE
#define INCLUDE_CBUF_CODE
#endif
#else
#define INLINE_CBUF_CODE
extern void cb_reset(struct circbuf *cb);
extern void cb_init(struct circbuf *cb, uint32_t size);
extern uint32_t cb_occupied(struct circbuf *cb);
extern uint32_t cb_available(struct circbuf *cb);
extern void cb_produce(struct circbuf *cb, uint32_t used);
extern void cb_release(struct circbuf *cb, uint32_t discard);
extern uint32_t cb_head(struct circbuf *cb);
extern uint32_t cb_tail(struct circbuf *cb);
extern uint32_t cb_split_read(struct circbuf *cb, uint32_t length);
extern uint32_t cb_split_write(struct circbuf *cb, uint32_t length);
#endif

#if defined(INCLUDE_CBUF_CODE)

INLINE_CBUF_CODE void cb_reset(struct circbuf *cb)
{
    __sync_synchronize();
    cb->head = cb->tail = 0;
    __sync_synchronize();
}

INLINE_CBUF_CODE void cb_init(struct circbuf *cb, uint32_t size)
{
    cb->size = size;
    cb_reset(cb);
}

INLINE_CBUF_CODE uint32_t cb_occupied(struct circbuf *cb)
{
    return (cb->tail - cb->head + cb->size) % cb->size;
}

INLINE_CBUF_CODE uint32_t cb_available(struct circbuf *cb)
{
    return (cb->head - cb->tail - 1 + cb->size) % cb->size;
}

INLINE_CBUF_CODE void cb_produce(struct circbuf *cb, uint32_t used)
{
    __sync_synchronize();
    cb->tail = (cb->tail + used) % cb->size ;
    __sync_synchronize();
}

INLINE_CBUF_CODE void cb_release(struct circbuf *cb, uint32_t discard)
{
    __sync_synchronize();
    cb->head = (cb->head + discard) % cb->size;
    __sync_synchronize();
}

INLINE_CBUF_CODE uint32_t cb_head(struct circbuf *cb)
{
    return cb->head;
}

INLINE_CBUF_CODE uint32_t cb_tail(struct circbuf *cb)
{
    return cb->tail;
}

INLINE_CBUF_CODE uint32_t
cb_split_read(struct circbuf *cb, uint32_t length)
{
    return cb->head + length <= cb->size ? length : cb->size - cb->head;
}

INLINE_CBUF_CODE uint32_t
cb_split_write(struct circbuf *cb, uint32_t length)
{
    return cb->tail + length <= cb->size ? length : cb->size - cb->tail;
}

#endif
