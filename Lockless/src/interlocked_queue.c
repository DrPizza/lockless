#include "stdafx.h"

#include "interlocked_queue.h"

typedef struct interlocked_queue_node
{
	struct interlocked_queue_node* next;
	void* data;
} interlocked_queue_node_t;

interlocked_queue_node_t* new_interlocked_queue_node()
{
	interlocked_queue_node_t* n = smr_alloc(sizeof(interlocked_queue_node_t));
	n->data = nullptr;
	n->next = nullptr;
	return n;
}

typedef struct interlocked_queue
{
	CACHE_ALIGN interlocked_queue_node_t* head;
	CACHE_ALIGN interlocked_queue_node_t* tail;

	destructor_t value_destructor;
} interlocked_queue_t;

interlocked_queue_t* new_interlocked_queue(destructor_t value_destructor)
{
	interlocked_queue_t* q = smr_alloc(sizeof(interlocked_queue_t));
	memset(q, 0, sizeof(interlocked_queue_t));
	q->head = q->tail = new_interlocked_queue_node();
	q->value_destructor = value_destructor;
	return q;
}

void delete_interlocked_queue(interlocked_queue_t* q)
{
	void* value;
	while(interlocked_queue_pop(q, &value))
	{
		q->value_destructor(value);
	}
	smr_retire(q->head);
	q->head = nullptr;
	q->tail = nullptr;
	smr_retire(q);
}

void interlocked_queue_push(interlocked_queue_t* q, void* data)
{
	interlocked_queue_node_t* node = new_interlocked_queue_node();
	interlocked_queue_node_t* t = nullptr;
	interlocked_queue_node_t* next = nullptr;
	void* volatile* hazards[1] = { nullptr };
	void* key = allocate_hazard_pointers(1, hazards);
	if(!hazards[0]) { RaiseException(ERROR_NOT_ENOUGH_MEMORY, 0, 0, nullptr); return; }

	node->data = data;

	for(;;)
	{
		t = q->tail;
		*hazards[0] = t;
		MemoryBarrier();

		if(q->tail != t)
		{
			continue;
		}
		next = t->next;
		if(q->tail != t)
		{
			continue;
		}
		if(next != nullptr)
		{
			casp((void* volatile*)&q->tail, t, next);
			continue;
		}
		if(casp((void* volatile*)&t->next, nullptr, node))
		{
			break;
		}
	}
	casp((void* volatile*)&q->tail, t, node);
	deallocate_hazard_pointers(key);
}

bool interlocked_queue_pop(interlocked_queue_t* q, void** output)
{
	interlocked_queue_node_t* h = nullptr;
	interlocked_queue_node_t* t = nullptr;
	interlocked_queue_node_t* next = nullptr;
	void* data = nullptr;
	void* volatile* hazards[2] = { nullptr };
	void* key = allocate_hazard_pointers(2, hazards);
	if(!hazards[0] || !hazards[1]) { RaiseException(ERROR_NOT_ENOUGH_MEMORY, 0, 0, nullptr); return false; }

	for(;;)
	{
		h = q->head;
		*hazards[0] = h;
		MemoryBarrier();
		if(q->head != h)
		{
			continue;
		}
		t = q->tail;
		next = h->next;
		*hazards[1] = next;
		MemoryBarrier();
		if(q->head != h)
		{
			continue;
		}
		if(next == nullptr)
		{
			*hazards[0] = nullptr;
			*hazards[1] = nullptr;
			if(output) { *output = nullptr; }
			return false;
		}
		if(h == t)
		{
			casp((void* volatile*)&q->tail, t, next);
			continue;
		}
		data = next->data;
		if(casp((void* volatile*)&q->head, h, next))
		{
			break;
		}
	}

	h->next = nullptr;
	smr_retire(h);
	if(output) { *output = data; }
	deallocate_hazard_pointers(key);
	return data != nullptr;
}

#define MAX_RETRIES	3

long interlocked_queue_depth(const interlocked_queue_t* q)
{
	interlocked_queue_node_t* h = nullptr;
	interlocked_queue_node_t* next = nullptr;

	long count = 0;
	long retry_count = 0;
	void* volatile* hazards[2] = { nullptr };
	void* key = allocate_hazard_pointers(2, hazards);
	if(!hazards[0] || !hazards[1]) { RaiseException(ERROR_NOT_ENOUGH_MEMORY, 0, 0, nullptr); return 0; }

	do
	{
		h = q->head;
		if(h == q->tail)
		{
			return count;
		}
		*hazards[0] = h;
		MemoryBarrier();
	}
	while(q->head != h && retry_count++ < MAX_RETRIES);

	while(h != nullptr)
	{
		next = h->next;
		*hazards[1] = next;
		MemoryBarrier();
		if(next != h->next) // musta been delinked, nothing we can do to recover, so bail
		{
			break;
		}
		++count;
		h = next;
		*hazards[0] = h;
		MemoryBarrier();
	}

	deallocate_hazard_pointers(key);

	return --count;
}

bool interlocked_queue_is_empty(const interlocked_queue_t* q)
{
	return q->head == q->tail;
}
