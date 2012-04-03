#ifndef INTERLOCKED_SET__H
#define INTERLOCKED_SET__H

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

typedef struct interlocked_kv_list interlocked_kv_list_t;

typedef int      (*key_cmp     )(const void*, const void*);
typedef void     (*destructor_t)(const void*);

interlocked_kv_list_t* new_interlocked_kv_list(key_cmp cmp, destructor_t key_destructor, destructor_t value_destructor);
void delete_interlocked_kv_list(interlocked_kv_list_t* s);

bool interlocked_kv_list_insert(interlocked_kv_list_t* s, const void* key, void*  value);
bool interlocked_kv_list_delete(interlocked_kv_list_t* s, const void* key);
bool interlocked_kv_list_find  (interlocked_kv_list_t* s, const void* key, void** value);
bool interlocked_kv_list_is_empty(const interlocked_kv_list_t* s);

#ifdef __cplusplus
}
#endif

#endif
