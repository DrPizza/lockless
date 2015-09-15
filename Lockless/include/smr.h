#ifndef SMR__H
#define SMR__H

#include <SDKDDKVer.h>
#include <Windows.h>

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

#pragma warning(disable : 4324) // warning C4234: structure was padded due to __declspec(align())
#define CACHE_LINE 64
#define CACHE_ALIGN __declspec(align(CACHE_LINE))

// return true if the memory should be freed with smr_free after the finalizer is called
typedef bool (*finalizer_function_t)(void* finalizer_context, void* node_data);

void* smr_alloc(size_t size);
void smr_retire(void* ptr);
void smr_retire_with_finalizer(void* ptr, finalizer_function_t finalizer, void* finalizer_context);
void smr_free(void* ptr);
void smr_clean();
void smr_unsafe_full_clean();

void report_remaining_objects();

void* allocate_hazard_pointers(LONG count, void* volatile** pointers);
void deallocate_hazard_pointers(void* key);

bool cas(volatile LONG* addr, LONG expected_value, LONG new_value);
bool casp(void* volatile* addr, void* expected_value, void* new_value);
bool tas(volatile LONG* addr);

#ifdef __cplusplus
}
#endif

#endif
