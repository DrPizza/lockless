#ifndef INTERLOCKED_QUEUE__H
#define INTERLOCKED_QUEUE__H

#include "smr.h"

#ifdef __cplusplus
extern "C"
{
#endif

#ifndef __cplusplus
#define nullptr NULL
typedef char bool;
#define false 0
#define true 1
#endif

typedef struct interlocked_queue interlocked_queue_t;

typedef void     (*destructor_t)(const void*);

interlocked_queue_t* new_interlocked_queue(destructor_t value_destructor);
void delete_interlocked_queue(interlocked_queue_t* q);

void interlocked_queue_push(interlocked_queue_t* q, void* data);
bool interlocked_queue_pop(interlocked_queue_t* q, void** output);
bool interlocked_queue_is_empty(const interlocked_queue_t* q);
long interlocked_queue_depth(const interlocked_queue_t* q);

#ifdef __cplusplus
}
#endif

#endif
