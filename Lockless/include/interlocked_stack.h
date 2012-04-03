#ifndef INTERLOCKED_STACK__H
#define INTERLOCKED_STACK__H

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

typedef struct interlocked_stack interlocked_stack_t;

typedef void     (*destructor_t)(const void*);

interlocked_stack_t* new_interlocked_stack(destructor_t value_destructor);
void delete_interlocked_stack(interlocked_stack_t* s);

void interlocked_stack_push(interlocked_stack_t* s, void* data);
bool interlocked_stack_pop(interlocked_stack_t* s, void** output);
long interlocked_stack_depth(const interlocked_stack_t* s);
bool interlocked_stack_is_empty(const interlocked_stack_t* s);

#ifdef __cplusplus
}
#endif

#endif
