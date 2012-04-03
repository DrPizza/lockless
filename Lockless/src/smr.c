#include "stdafx.h"

#include "smr.h"

#pragma warning(disable : 4204) // warning C4204: nonstandard extension used : non-constant aggregate initializer
#pragma warning(disable : 4324) // warning C4234: structure was padded due to __declspec(align())
#define CACHE_LINE 64
#define CACHE_ALIGN __declspec(align(CACHE_LINE))

static DWORD thr_slot = TLS_OUT_OF_INDEXES;

typedef struct retired_data
{
	void* node;
	finalizer_function_t finalizer;
	void* finalizer_context;
} retired_data_t;

void dispose_retired_data(retired_data_t node)
{
	if(node.finalizer != nullptr)
	{
		node.finalizer(node.finalizer_context, node.node);
	}
	_aligned_free(node.node);
}

typedef struct retired_list_node
{
	SLIST_ENTRY list_entry;
	retired_data_t retired_data;
} retired_list_node_t;

typedef struct retired_list
{
	SLIST_HEADER* list;
} retired_list_t;

retired_list_t* new_retired_list()
{
	retired_list_t* rl = smr_alloc(sizeof(retired_list_t));
	memset(rl, 0, sizeof(retired_list_t));
	rl->list = smr_alloc(sizeof(SLIST_HEADER));
	InitializeSListHead(rl->list);
	return rl;
}

void delete_retired_list(retired_list_t* l)
{
	retired_list_node_t* h = nullptr;
	for(h = (retired_list_node_t*)InterlockedFlushSList(l->list); h != nullptr; )
	{
		retired_list_node_t* n = (retired_list_node_t*)(h->list_entry.Next);
		_aligned_free(h);
		h = n;
	}
	_aligned_free(l->list);
	_aligned_free(l);
}

void retired_list_push(retired_list_t* l, retired_data_t val)
{
	retired_list_node_t* new_node = smr_alloc(sizeof(retired_list_node_t));
	new_node->retired_data = val;
	InterlockedPushEntrySList(l->list, &new_node->list_entry);
}

retired_data_t retired_list_pop(retired_list_t* l)
{
	retired_list_node_t* old_node = (retired_list_node_t*)InterlockedPopEntrySList(l->list);
	if(old_node != nullptr)
	{
		retired_data_t rv = old_node->retired_data;
		_aligned_free(old_node);
		return rv;
	}
	else
	{
		retired_data_t zero = {0};
		return zero;
	}
}

bool retired_list_contains(retired_list_t* l, retired_data_t val)
{
	bool rv = false;
	retired_list_node_t* h = nullptr;
	// this is a pretty silly way of doing it
	for(h = (retired_list_node_t*)InterlockedFlushSList(l->list); h != nullptr; h = (retired_list_node_t*)(h->list_entry.Next))
	{
		if(h->retired_data.node == val.node)
		{
			rv = true;
		}
		InterlockedPushEntrySList(l->list, &h->list_entry);
	}
	return rv;
}

void retired_list_swap(retired_list_t* l, retired_list_t* r)
{
	retired_list_t tmp;
	tmp.list = l->list;
	l->list = r->list;
	r->list = tmp.list;
}

USHORT retired_list_count(retired_list_t* l)
{
	return QueryDepthSList(l->list);
}

typedef struct hazard_pointer_record
{
	struct hazard_pointer_record* next;
	volatile LONG active;
	LONG count;
	void* volatile hazard_pointers[1];
} hazard_pointer_record_t;

hazard_pointer_record_t* new_hpr(LONG count)
{
	hazard_pointer_record_t* hpr = smr_alloc(sizeof(hazard_pointer_record_t) + ((count - 1)* sizeof(void* volatile)));
	memset(hpr, 0, sizeof(hazard_pointer_record_t) + ((count - 1) * sizeof(void* volatile)));
	hpr->count = count;
	return hpr;
}

CACHE_ALIGN hazard_pointer_record_t* volatile head_hpr = nullptr;
CACHE_ALIGN volatile LONG total_hazard_pointers = 0;
CACHE_ALIGN volatile LONG total_thread_records = 0;

typedef struct hpr_cache
{
	hazard_pointer_record_t* record;
	struct hpr_cache* next;
} hpr_cache_t;

hazard_pointer_record_t* allocate_hpr(LONG count);

hpr_cache_t* new_hpr_cache(LONG count)
{
	hpr_cache_t* hc = smr_alloc(sizeof(hpr_cache_t));
	memset(hc, 0, sizeof(hpr_cache_t));
	hc->record = allocate_hpr(count);
	return hc;
}

void retire_hpr(hazard_pointer_record_t* hprec);

void delete_hpr_cache(hpr_cache_t* hc)
{
	if(hc->record)
	{
		retire_hpr(hc->record);
	}
	smr_free(hc);
}

typedef struct thread_hpr_record
{
	// chaining/reclamation
	struct thread_hpr_record* next;
	volatile LONG active;
	// actual data
	retired_list_t* retired_list;
	hpr_cache_t* cache;
} thread_hpr_record_t;

thread_hpr_record_t* new_thr()
{
	thread_hpr_record_t* thr = smr_alloc(sizeof(thread_hpr_record_t));
	memset(thr, 0, sizeof(thread_hpr_record_t));
	thr->retired_list = new_retired_list();
	return thr;
}

CACHE_ALIGN thread_hpr_record_t* volatile head_thr = nullptr;
volatile LONG maximum_threads = 0;

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

hazard_pointer_record_t* allocate_hpr(LONG count)
{
	hazard_pointer_record_t* hprec = nullptr;
	hazard_pointer_record_t* oldhead = nullptr;
	LONG old_count = 0;
	for(hprec = head_hpr; hprec != nullptr; hprec = hprec->next)
	{
		if(hprec->active)
		{
			continue;
		}
		if(tas(&hprec->active))
		{
			continue;
		}
		if(hprec->count < count)
		{
			hprec->active = 0;
			continue;
		}
		return hprec;
	}
	do
	{
		old_count = total_hazard_pointers;
	}
	while(!cas(&total_hazard_pointers, old_count, old_count + count));
	hprec = new_hpr(count);
	hprec->active = 1;
	do
	{
		oldhead = head_hpr;
		hprec->next = oldhead;
	}
	while(!casp((void* volatile*)&head_hpr, oldhead, hprec));
	return hprec;
}

void retire_hpr(hazard_pointer_record_t* hprec)
{
	int i = 0;
	for(i = 0; i < hprec->count; ++i)
	{
		hprec->hazard_pointers[i] = nullptr;
	}
	hprec->active = 0;
}

thread_hpr_record_t* allocate_thr()
{
	thread_hpr_record_t* threc = nullptr;
	thread_hpr_record_t* oldhead = nullptr;
	LONG old_count = 0;
	for(threc = head_thr; threc != nullptr; threc = threc->next)
	{
		if(threc->active)
		{
			continue;
		}
		if(tas(&threc->active))
		{
			continue;
		}
		return threc;
	}
	do
	{
		old_count = total_thread_records;
	}
	while(!cas(&total_thread_records, old_count, old_count + 1));
	threc = new_thr();
	threc->active = 1;
	do
	{
		oldhead = head_thr;
		threc->next = oldhead;
	}
	while(!casp((void* volatile*)&head_thr, oldhead, threc));
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
	hpr_cache_t* cache = thr->cache;
	for(; cache != nullptr;)
	{
		hpr_cache_t* next = cache->next;
		delete_hpr_cache(cache);
		cache = next;
	}
	thr->cache = nullptr;
	thr->active = 0;
}

void scan(hazard_pointer_record_t* head)
{
	hazard_pointer_record_t* hprec = head;
	retired_list_t* potentially_hazardous = new_retired_list();
	retired_list_t* tmplist = new_retired_list();
	retired_data_t node;

	for(; hprec != nullptr; hprec = hprec->next)
	{
		int i = 0;
		for(; i < hprec->count; ++i)
		{
			MemoryBarrier();
			if(hprec->hazard_pointers[i] != nullptr)
			{
				retired_data_t v = { hprec->hazard_pointers[i] };
				retired_list_push(potentially_hazardous, v);
			}
		}
	}
	retired_list_swap(tmplist, get_mythrec()->retired_list);
	node = retired_list_pop(tmplist);
	while(node.node != nullptr)
	{
		if(retired_list_contains(potentially_hazardous, node))
		{
			retired_list_push(get_mythrec()->retired_list, node);
		}
		else
		{
			dispose_retired_data(node);
		}
		node = retired_list_pop(tmplist);
	}
	delete_retired_list(tmplist);
	delete_retired_list(potentially_hazardous);
}

LONG R(long hh)
{
#ifdef _DEBUG
	return 1;
#else
	return 2 * hh;
#endif
}

void help_scan()
{
	thread_hpr_record_t* threc = nullptr;
	hazard_pointer_record_t* head = nullptr;
	for(threc = head_thr; threc != nullptr; threc = threc->next)
	{
		if(threc->active)
		{
			continue;
		}
		if(tas(&threc->active))
		{
			continue;
		}
		while(retired_list_count(threc->retired_list) > 0)
		{
			retired_data_t node = retired_list_pop(threc->retired_list);
			retired_list_push(get_mythrec()->retired_list, node);
			head = head_hpr;
			if(retired_list_count(get_mythrec()->retired_list) >= R(total_hazard_pointers))
			{
				scan(head);
			}
		}
		threc->active = 0;
	}
}

void retire_node(retired_data_t node)
{
	hazard_pointer_record_t* head = head_hpr;
	retired_list_push(get_mythrec()->retired_list, node);
	if(retired_list_count(get_mythrec()->retired_list) >= R(total_hazard_pointers))
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
	retired_data_t node = { ptr };
	retire_node(node);
}

void smr_free_with_finalizer(void* ptr, finalizer_function_t finalizer, void* finalizer_context)
{
	retired_data_t node = { ptr, finalizer, finalizer_context };
	retire_node(node);
}

void* allocate_hazard_pointers(LONG count, void* volatile** pointers)
{
	hpr_cache_t* cache = get_mythrec()->cache;
	hazard_pointer_record_t* hprec = nullptr;
	LONG i;

	for(; cache != nullptr; cache = cache->next)
	{
		if(cache->record != nullptr && !cache->record->active && cache->record->count >= count)
		{
			hprec = cache->record;
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
	hprec->active = true;
	for(i = 0; i < count; ++i)
	{
		pointers[i] = &hprec->hazard_pointers[i];
	}
	return hprec;
}

void deallocate_hazard_pointers(void* key) {
	retire_hpr(key);
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
		retire_thr(get_mythrec());
		break;
	case DLL_PROCESS_DETACH:
		// the first thread only gets a process notification, not a thread notification.
		retire_thr(get_mythrec());
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
