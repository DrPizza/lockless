#include "stdafx.h"

#include "interlocked_stack.h"

typedef struct interlocked_stack_node
{
	struct interlocked_stack_node* next;
	void* data;
} interlocked_stack_node_t;

interlocked_stack_node_t* new_interlocked_stack_node()
{
	interlocked_stack_node_t* n = smr_alloc(sizeof(interlocked_stack_node_t));
	memset(n, 0, sizeof(interlocked_stack_node_t));
	return n;
}

typedef struct interlocked_stack
{
	CACHE_ALIGN interlocked_stack_node_t* top;
	destructor_t value_destructor;
} interlocked_stack_t;

interlocked_stack_t* new_interlocked_stack(destructor_t value_destructor)
{
	interlocked_stack_t* s = smr_alloc(sizeof(interlocked_stack_t));
	memset(s, 0, sizeof(interlocked_stack_t));
	s->value_destructor = value_destructor;
	return s;
}

void delete_interlocked_stack(interlocked_stack_t* s)
{
	void* value;
	while(interlocked_stack_pop(s, &value))
	{
		s->value_destructor(value);
	}
	smr_free(s);
}

void interlocked_stack_push(interlocked_stack_t* s, void* data)
{
	interlocked_stack_node_t* node = new_interlocked_stack_node();
	interlocked_stack_node_t* t = nullptr;
	node->data = data;

	for(;;)
	{
		t = s->top;
		node->next = t;
		if(casp((void* volatile*)&s->top, t, node))
		{
			break;
		}
	}
}

bool interlocked_stack_pop(interlocked_stack_t* s, void** output)
{
	interlocked_stack_node_t* next = nullptr;
	interlocked_stack_node_t* t = nullptr;
	void* data = nullptr;
	void* volatile* hazards[1] = { nullptr };
	void* key = allocate_hazard_pointers(1, hazards);
	if(!hazards[0]) { RaiseException(ERROR_NOT_ENOUGH_MEMORY, 0, 0, nullptr); return false; }

	for(;;)
	{
		t = s->top;
		if(t == nullptr)
		{
			if(output) { *output = nullptr; }
			deallocate_hazard_pointers(key);
			return false;
		}
		*hazards[0] = t;
		MemoryBarrier();
		if(s->top != t)
		{
			continue;
		}
		next = t->next;
		if(casp((void* volatile*)&s->top, t, next))
		{
			break;
		}
	}
	data = t->data;
	t->next = nullptr; // make delinking detectable, otherwise this can be pointing off at no-man's land
	MemoryBarrier();
	deallocate_hazard_pointers(key);
	smr_free(t);
	if(output) { *output = data; }
	return true;
}

#define MAX_RETRIES	3

long interlocked_stack_depth(const interlocked_stack_t* s)
{
	interlocked_stack_node_t* t = nullptr;
	interlocked_stack_node_t* next = nullptr;
	long count = 0;
	long retry_count = 0;
	void* volatile* hazards[2] = { nullptr };
	void* key = allocate_hazard_pointers(2, hazards);
	if(!hazards[0] || !hazards[1]) { RaiseException(ERROR_NOT_ENOUGH_MEMORY, 0, 0, nullptr); return 0; }

	do
	{
		t = s->top;
		if(t == nullptr)
		{
			return count;
		}
		*hazards[0] = t;
		MemoryBarrier();
	}
	while(s->top != t && retry_count++ < MAX_RETRIES);

	while(t != nullptr)
	{
		next = t->next;
		*hazards[1] = next;
		MemoryBarrier();
		if(next != t->next) // musta been delinked, nothing we can do to recover, so bail
		{
			break;
		}
		++count;
		t = next;
		*hazards[0] = t;
		MemoryBarrier();
	}

	deallocate_hazard_pointers(key);
	return count;
}

bool interlocked_stack_is_empty(const interlocked_stack_t* s)
{
	return s->top == nullptr;
}
