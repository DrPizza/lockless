#include "stdafx.hpp"

#include "smr.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <atomic>

#pragma warning(disable : 4200) // warning C4200: nonstandard extension used : zero-sized array in struct/union
#pragma warning(disable : 4204) // warning C4204: nonstandard extension used : non-constant aggregate initializer
#pragma warning(disable : 4324) // warning C4234: structure was padded due to __declspec(align())
#define CACHE_LINE 64
#define CACHE_ALIGN __declspec(align(CACHE_LINE))

bool cas(volatile LONG* addr, LONG expected_value, LONG new_value)
{
	LONG previous_value = InterlockedCompareExchange(addr, new_value, expected_value);
	return expected_value == previous_value;
}

bool casp(void* volatile* addr, void* expected_value, void* new_value)
{
	void* previous_value = InterlockedCompareExchangePointer(addr, new_value, expected_value);
	return expected_value == previous_value;
}

bool tas(volatile LONG* addr)
{
	return !cas(addr, 0L, 1L);
}

static DWORD thr_slot = TLS_OUT_OF_INDEXES;

static CACHE_ALIGN volatile std::atomic<size_t> total_hazard_pointers = ATOMIC_VAR_INIT(0);
static CACHE_ALIGN volatile std::atomic<size_t> total_thread_records  = ATOMIC_VAR_INIT(0);

struct retired_data_t
{
	void* node;
	finalizer_function_t finalizer;
	void* finalizer_context;
};

void dispose_retired_data(retired_data_t node)
{
	bool needs_free = true;
	if(node.finalizer != nullptr)
	{
		needs_free = node.finalizer(node.finalizer_context, node.node);
	}
	if(needs_free)
	{
		_aligned_free(node.node);
	}
}

struct retired_list_t
{
#ifdef _DEBUG
	char type[16];
#endif
	size_t retired_count;
	size_t maximum_size;
	retired_data_t retired_items[0];
};

retired_list_t* new_retired_list()
{
	size_t size = total_hazard_pointers.load();
	size = std::max(size, static_cast<size_t>(1));
	retired_list_t* rl = static_cast<retired_list_t*>(smr_alloc(sizeof(retired_list_t) + (size * sizeof(retired_data_t))));
	std::memset(rl, 0, sizeof(retired_list_t) + (size * sizeof(retired_data_t)));
	rl->maximum_size = size;
#ifdef _DEBUG
	strcat_s(rl->type, sizeof(rl->type), "retired_list");
#endif
	return rl;
}

void retired_list_delete(retired_list_t* l)
{
	// this structure is thread private so doesn't need a deferred free,
	// and since the thread may be being destroyed, I can't use the deferred
	// mechanism anyway.
	_aligned_free(l);
}

void retired_list_push(retired_list_t** l, retired_data_t val)
{
	if((*l)->retired_count == (*l)->maximum_size)
	{
		retired_list_t* old_list = *l;
		retired_list_t* new_list = new_retired_list();
		new_list->retired_count = old_list->retired_count;
		std::memcpy(new_list->retired_items, old_list->retired_items, old_list->retired_count * sizeof(retired_data_t));
		*l = new_list;
		retired_list_delete(old_list);
	}
	(*l)->retired_items[(*l)->retired_count++] = val;
}

retired_data_t retired_list_pop(retired_list_t* l)
{
	if(l->retired_count != 0)
	{
		return l->retired_items[--(l->retired_count)];
	}
	else
	{
		retired_data_t zero = {0};
		return zero;
	}
}

void retired_list_clear(retired_list_t* l)
{
	l->retired_count = 0;
	memset(l->retired_items, 0, l->maximum_size * sizeof(retired_data_t));
}

bool retired_data_compare(const retired_data_t& lhs, const retired_data_t& rhs)
{
	return reinterpret_cast<size_t>(lhs.node) < reinterpret_cast<size_t>(rhs.node);
}

bool retired_list_contains(retired_list_t* l, retired_data_t val)
{
	return std::binary_search(l->retired_items, l->retired_items + l->retired_count, val, retired_data_compare);
}

LONG retired_list_count(retired_list_t* l)
{
	return l->retired_count;
}

struct hazard_pointer_record_t
{
#ifdef _DEBUG
	char type[16];
#endif
	hazard_pointer_record_t* next;
	std::atomic<bool> active = ATOMIC_VAR_INIT(false);
	LONG count;
	void* volatile hazard_pointers[0];
};

CACHE_ALIGN std::atomic<hazard_pointer_record_t*> head_hpr = ATOMIC_VAR_INIT(nullptr);

hazard_pointer_record_t* new_hpr(LONG count)
{
	hazard_pointer_record_t* hpr = static_cast<hazard_pointer_record_t*>(smr_alloc(sizeof(hazard_pointer_record_t) + (count * sizeof(void* volatile))));
	std::memset(hpr, 0, sizeof(hazard_pointer_record_t) + (count * sizeof(void* volatile)));
	hpr->count = count;
#ifdef _DEBUG
	strcat_s(hpr->type, sizeof(hpr->type), "hzardpointerrec");
#endif
	return hpr;
}

struct hpr_cache_t
{
#ifdef _DEBUG
	char type[16];
#endif
	hazard_pointer_record_t* record;
	hpr_cache_t* next;
};

hazard_pointer_record_t* allocate_hpr(LONG count);

hpr_cache_t* new_hpr_cache(LONG count)
{
	hpr_cache_t* hc = static_cast<hpr_cache_t*>(smr_alloc(sizeof(hpr_cache_t)));
	std::memset(hc, 0, sizeof(hpr_cache_t));
	hc->record = allocate_hpr(count);
#ifdef _DEBUG
	strcat_s(hc->type, sizeof(hc->type), "hpr_cache");
#endif
	return hc;
}

void retire_hpr(hazard_pointer_record_t* hprec);

void delete_hpr_cache(hpr_cache_t* hc)
{
	if(hc->record)
	{
		retire_hpr(hc->record);
	}
	// this structure is thread private so doesn't need a deferred free,
	// and since the thread is being destroyed, I can't use the deferred
	// mechanism anyway.
	_aligned_free(hc);
}

struct thread_hpr_record_t
{
#ifdef _DEBUG
	char type[16];
#endif
	// chaining/reclamation
	thread_hpr_record_t* next;
	std::atomic<bool> active = ATOMIC_VAR_INIT(false);
	// actual data
	retired_list_t* retired_list;
	hpr_cache_t* cache;
};

thread_hpr_record_t* new_thr()
{
	thread_hpr_record_t* thr = static_cast<thread_hpr_record_t*>(smr_alloc(sizeof(thread_hpr_record_t)));
	std::memset(thr, 0, sizeof(thread_hpr_record_t));
	thr->retired_list = new_retired_list();
#ifdef _DEBUG
	strcat_s(thr->type, sizeof(thr->type), "thread_hpr_rec");
#endif
	return thr;
}

static CACHE_ALIGN std::atomic<thread_hpr_record_t*> head_thr = ATOMIC_VAR_INIT(nullptr);

bool atomic_test_and_set(std::atomic<bool>& b)
{
	bool expected = false;
	b.compare_exchange_strong(expected, true);
	return expected;
}

hazard_pointer_record_t* allocate_hpr(LONG count)
{
	hazard_pointer_record_t* hprec = nullptr;
	hazard_pointer_record_t* oldhead = nullptr;
	for(hprec = head_hpr.load(); hprec != nullptr; hprec = hprec->next)
	{
		if(hprec->active.load())
		{
			continue;
		}
		if(atomic_test_and_set(hprec->active))
		{
			continue;
		}
		if(hprec->count < count)
		{
			hprec->active.store(false);
			continue;
		}
		return hprec;
	}
	total_hazard_pointers.fetch_add(count);
	hprec = new_hpr(count);
	hprec->active.store(true);
	do
	{
		oldhead = head_hpr.load();
		hprec->next = oldhead;
	}
	while(!head_hpr.compare_exchange_strong(oldhead, hprec));
	return hprec;
}

void retire_hpr(hazard_pointer_record_t* hprec)
{
	for(int i = 0; i < hprec->count; ++i)
	{
		hprec->hazard_pointers[i] = nullptr;
	}
	hprec->active.store(0);
}

thread_hpr_record_t* allocate_thr()
{
	thread_hpr_record_t* threc = nullptr;
	thread_hpr_record_t* oldhead = nullptr;
	for(threc = head_thr.load(); threc != nullptr; threc = threc->next)
	{
		if(threc->active.load())
		{
			continue;
		}
		if(atomic_test_and_set(threc->active))
		{
			continue;
		}
		return threc;
	}
	total_thread_records.fetch_add(1);
	threc = new_thr();
	threc->active.store(true);
	do
	{
		oldhead = head_thr.load();
		threc->next = oldhead;
	}
	while(!head_thr.compare_exchange_strong(oldhead, threc));
	return threc;
}

thread_hpr_record_t* get_mythrec()
{
	thread_hpr_record_t* thr = (thread_hpr_record_t*)TlsGetValue(thr_slot);
	if(nullptr == thr)
	{
		TlsSetValue(thr_slot, allocate_thr());
		thr = (thread_hpr_record_t*)TlsGetValue(thr_slot);
	}
	return thr;
}

void retire_thr(thread_hpr_record_t* thr)
{
	for(hpr_cache_t* cache = thr->cache; cache != nullptr;)
	{
		hpr_cache_t* next = cache->next;
		delete_hpr_cache(cache);
		cache = next;
	}
	thr->cache = nullptr;
	thr->active = 0;
	retired_list_clear(thr->retired_list);
}

void scan(hazard_pointer_record_t* head)
{
	retired_list_t* potentially_hazardous = new_retired_list();

	for(hazard_pointer_record_t* hprec = head; hprec != nullptr; hprec = hprec->next)
	{
		for(int i = 0; i < hprec->count; ++i)
		{
			MemoryBarrier();
			if(hprec->hazard_pointers[i] != nullptr)
			{
				retired_data_t v = { hprec->hazard_pointers[i] };
				retired_list_push(&potentially_hazardous, v);
			}
		}
	}
	std::sort(potentially_hazardous->retired_items, potentially_hazardous->retired_items + potentially_hazardous->retired_count, retired_data_compare);

	retired_list_t* tmplist = get_mythrec()->retired_list;
	get_mythrec()->retired_list = new_retired_list();

	retired_data_t node = retired_list_pop(tmplist);
	while(node.node != nullptr)
	{
		if(retired_list_contains(potentially_hazardous, node))
		{
			retired_list_push(&(get_mythrec()->retired_list), node);
		}
		else
		{
			dispose_retired_data(node);
		}
		node = retired_list_pop(tmplist);
	}
	retired_list_delete(tmplist);
	retired_list_delete(potentially_hazardous);
}

LONG R(long hh)
{
#ifdef _DEBUG
	return 0;
#else
	return 2 * hh;
#endif
}

void help_scan()
{
	for(thread_hpr_record_t* threc = head_thr; threc != nullptr; threc = threc->next)
	{
		if(threc->active.load())
		{
			continue;
		}
		if(atomic_test_and_set(threc->active))
		{
			continue;
		}
		while(retired_list_count(threc->retired_list) > 0)
		{
			retired_data_t node = retired_list_pop(threc->retired_list);
			retired_list_push(&(get_mythrec()->retired_list), node);
			hazard_pointer_record_t* head = head_hpr;
			if(retired_list_count(get_mythrec()->retired_list) >= R(total_hazard_pointers.load()))
			{
				scan(head);
			}
		}
		threc->active.store(false);
	}
}

void retire_node(retired_data_t node)
{
	hazard_pointer_record_t* head = head_hpr;
	retired_list_push(&(get_mythrec()->retired_list), node);
	if(retired_list_count(get_mythrec()->retired_list) >= R(total_hazard_pointers.load()))
	{
		scan(head);
		help_scan();
	}
}

void* smr_alloc(size_t size)
{
	void* value = _aligned_malloc(size, CACHE_LINE);
	memset(value, 0, size);
	return value;
}

void smr_free(void* ptr)
{
	_aligned_free(ptr);
}

void smr_retire(void* ptr)
{
	retired_data_t node = { ptr };
	retire_node(node);
}

void smr_retire_with_finalizer(void* ptr, finalizer_function_t finalizer, void* finalizer_context)
{
	retired_data_t node = { ptr, finalizer, finalizer_context };
	retire_node(node);
}

void smr_clean()
{
	hazard_pointer_record_t* head = head_hpr;
	scan(head);
}

void* allocate_hazard_pointers(LONG count, void* volatile** pointers)
{
	hpr_cache_t* cache = get_mythrec()->cache;
	hazard_pointer_record_t* hprec = nullptr;
	LONG i;

	for(; cache != nullptr; cache = cache->next)
	{
		if(cache->record != nullptr && !cache->record->active.load() && cache->record->count >= count)
		{
			hprec = cache->record;
			break;
		}
	}
	if(!hprec)
	{
		hpr_cache_t* new_cache = new_hpr_cache(count);
		cache = get_mythrec()->cache;
		if(nullptr == cache)
		{
			get_mythrec()->cache = new_cache;
		}
		else
		{
			while(cache->next != nullptr)
			{
				cache = cache->next;
			}
			cache->next = new_cache;
		}
		hprec = new_cache->record;
	}
	hprec->active.store(true);
	for(i = 0; i < count; ++i)
	{
		pointers[i] = &hprec->hazard_pointers[i];
	}
	return hprec;
}

void deallocate_hazard_pointers(void* key) {
	retire_hpr(static_cast<hazard_pointer_record_t*>(key));
}

void report_remaining_objects()
{
	if(head_thr.load() != nullptr && head_hpr.load() != nullptr)
	{
		thread_hpr_record_t* my_threc = get_mythrec();
		printf("this thread's thread record: 0x%p\n", my_threc);
		for(hpr_cache_t* cache = my_threc->cache; cache != nullptr; cache = cache->next)
		{
			printf("\tcache: 0x%p\n", cache);
			for(hazard_pointer_record_t* hpr = cache->record; hpr != nullptr; hpr = hpr->next)
			{
				printf("\t\thpr: 0x%p with %d hazard pointers starting at 0x%p\n", hpr, hpr->count, hpr->hazard_pointers);
			}
		}
		printf("\tretired list: 0x%p with %d of %d elements\n", my_threc->retired_list, my_threc->retired_list->retired_count, my_threc->retired_list->maximum_size);
		printf("------\n");
		printf("global thread record chain\n");
		for(thread_hpr_record_t* threc = head_thr; threc != nullptr; threc = threc->next)
		{
			printf("thread record: 0x%p\n", threc);
			for(hpr_cache_t* cache = threc->cache; cache != nullptr; cache = cache->next)
			{
				printf("\tcache: 0x%p\n", cache);
				for(hazard_pointer_record_t* hpr = cache->record; hpr != nullptr; hpr = hpr->next)
				{
					printf("\t\thpr: 0x%p with %d hazard pointers starting at 0x%p\n", hpr, hpr->count, hpr->hazard_pointers);
				}
			}
			printf("\tretired list: 0x%p with %d of %d elements\n", threc->retired_list, threc->retired_list->retired_count, threc->retired_list->maximum_size);
		}
		printf("------\n");
		printf("global hazard pointer chain\n");
		for(hazard_pointer_record_t* hpr = head_hpr; hpr != nullptr; hpr = hpr->next)
		{
			printf("\thpr: 0x%p with %d hazard pointers starting at 0x%p\n", hpr, hpr->count, hpr->hazard_pointers);
		}
	}
}

void smr_unsafe_full_clean()
{
	for(thread_hpr_record_t* threc = head_thr.load(); threc != nullptr;)
	{
		thread_hpr_record_t* next = threc->next;
		for(hpr_cache_t* cache = threc->cache; cache != nullptr;)
		{
			hpr_cache_t* next_cache = cache->next;
			_aligned_free(cache);
			cache = next_cache;
		}
		_aligned_free(threc->retired_list);
		_aligned_free(threc);
		threc = next;
	}
	head_thr.store(nullptr);
	total_thread_records = 0;
	for(hazard_pointer_record_t* hpr = head_hpr.load(); hpr != nullptr;)
	{
		hazard_pointer_record_t* next = hpr->next;
		_aligned_free(hpr);
		hpr = next;
	}
	head_hpr.store(nullptr);
	total_hazard_pointers.store(0);
}

void NTAPI on_tls_callback(void* dll, DWORD reason, void* reserved)
{
	UNREFERENCED_PARAMETER(dll);
	UNREFERENCED_PARAMETER(reserved);
	switch(reason)
	{
	case DLL_PROCESS_ATTACH:
		thr_slot = TlsAlloc();
		// fallthrough, the first thread only gets a process notification, not a thread notification.
	case DLL_THREAD_ATTACH:
		break;
	case DLL_THREAD_DETACH:
		// no point in cleaning up if I never got things dirty to start with
		if(head_thr.load() != nullptr && head_hpr.load() != nullptr)
		{
			retire_thr(get_mythrec());
		}
		break;
	case DLL_PROCESS_DETACH:
		// the first thread only gets a process notification, not a thread notification.
		if(head_thr.load() != nullptr && head_hpr.load() != nullptr)
		{
			retire_thr(get_mythrec());
		}
		TlsFree(thr_slot);
		break;
	}
}

#ifdef _M_IX86
#pragma comment(linker, "/INCLUDE:__tls_used")
#pragma comment(linker, "/INCLUDE:__xl_q")
#else
#pragma comment(linker, "/INCLUDE:_tls_used")
#pragma comment(linker, "/INCLUDE:_xl_q")
#endif

#ifdef _M_X64
#pragma const_seg(".CRT$XLQ")
EXTERN_C const
#else
#pragma data_seg(".CRT$XLQ")
EXTERN_C
#endif

PIMAGE_TLS_CALLBACK _xl_q = on_tls_callback;

#ifdef _M_X64
#pragma const_seg()
#else
#pragma data_seg()
#endif
