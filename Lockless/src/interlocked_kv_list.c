#include "stdafx.h"

#include "interlocked_kv_list.h"

typedef struct interlocked_kv_list_node {
	struct interlocked_kv_list_node* next;
	const void* key;
	void* value;
} interlocked_kv_list_node_t;

typedef struct interlocked_kv_list_node_destructors {
	destructor_t key_destructor;
	destructor_t value_destructor;
} interlocked_kv_list_node_destructors_t;

interlocked_kv_list_node_t* new_interlocked_kv_list_node(const void* key, void* value) {
	interlocked_kv_list_node_t* n = smr_alloc(sizeof(interlocked_kv_list_node_t));
	n->next = nullptr;
	n->key = key;
	n->value = value;
	return n;
}

typedef CACHE_ALIGN struct per_thread_vars {
	CACHE_ALIGN interlocked_kv_list_node_t** prev;
	CACHE_ALIGN interlocked_kv_list_node_t* current;
	CACHE_ALIGN interlocked_kv_list_node_t* next;
} per_thread_vars_t;

typedef struct interlocked_kv_list {
	interlocked_kv_list_node_t* head;

	key_cmp cmp;
	interlocked_kv_list_node_destructors_t destructors;
} interlocked_kv_list_t;

interlocked_kv_list_t* new_interlocked_kv_list(key_cmp cmp, destructor_t key_destructor, destructor_t value_destructor) {
	interlocked_kv_list_t* s = smr_alloc(sizeof(interlocked_kv_list_t));
	s->head = nullptr;
	s->cmp = cmp;
	s->destructors.key_destructor = key_destructor;
	s->destructors.value_destructor = value_destructor;
	return s;
}

void delete_interlocked_kv_list(interlocked_kv_list_t* s) {
	void* volatile* hazards[1] = { nullptr };
	void* key = allocate_hazard_pointers(1, hazards);
	if(!hazards[0]) { RaiseException(ERROR_NOT_ENOUGH_MEMORY, 0, 0, nullptr); }

	while(s->head != nullptr) {
		interlocked_kv_list_node_t* h = s->head;
		*hazards[0] = h;
		if(s->head != h) {
			continue;
		}
		interlocked_kv_list_delete(s, h->key);
	}
	deallocate_hazard_pointers(key);
	smr_retire(s);
}

void* mark_as_deleted(void* ptr) {
	return (void*)((size_t)ptr & 1);
}

void* mark_as_undeleted(void* ptr) {
	return (void*)((size_t)ptr & ~1);
}

bool test_if_deleted(void* ptr) {
	return ((size_t)ptr & 1) == 1;
}

bool find(interlocked_kv_list_node_t** head, const void* key, key_cmp cmp, per_thread_vars_t* v) {
	void* volatile* hazards[2] = { nullptr };
	void* hkey = allocate_hazard_pointers(2, hazards);
	if(!hazards[0] || !hazards[1]) { RaiseException(ERROR_NOT_ENOUGH_MEMORY, 0, 0, nullptr); return false; }

try_again:
	v->prev = head;
	v->current = *v->prev;
	while(v->current != nullptr) {
		*hazards[0] = v->current;
		if(*v->prev != v->current) {
			goto try_again;
		}
		v->next = v->current->next;
		if(test_if_deleted(v->next)) {
			if(!casp((void* volatile*)v->prev, v->current, mark_as_undeleted(v->next))) {
				goto try_again;
			}
			smr_retire(v->current);
			v->current = mark_as_undeleted(v->next);
		} else {
			void* volatile* tmp;
			const void* current_key = v->current->key;
			if(*v->prev != v->current) {
				goto try_again;
			}
			if(cmp(current_key, key) >= 0) {
				*hazards[0] = nullptr;
				*hazards[1] = nullptr;
				return cmp(current_key, key) == 0;
			}
			v->prev = &(v->current->next);
			tmp = hazards[0];
			hazards[0] = hazards[1];
			hazards[1] = tmp;
			v->current = v->next;
		}
	}
	deallocate_hazard_pointers(hkey);
	return false;
}

bool insert(interlocked_kv_list_node_t** head, interlocked_kv_list_node_t* node, key_cmp cmp, per_thread_vars_t* v) {
	for(;;) {
		if(find(head, node->key, cmp, v)) {
			return false;
		}
		node->next = v->current;
		if(casp((void* volatile*)v->prev, v->current, node)) {
			return true;
		}
	}
}

bool interlocked_kv_list_insert(interlocked_kv_list_t* s, const void* key, void* value) {
	interlocked_kv_list_node_t* node = new_interlocked_kv_list_node(key, value);
	per_thread_vars_t v = {0};
	return insert(&s->head, node, s->cmp, &v);
}

bool finalize_node(void* context, void* ptr) {
	interlocked_kv_list_node_t* node = ptr;
	interlocked_kv_list_node_destructors_t* d = context;

	d->key_destructor(node->key);
	d->value_destructor(node->value);
	smr_retire(d);
	return true;
}

bool del(interlocked_kv_list_node_t** head, const void* key, key_cmp cmp, interlocked_kv_list_node_destructors_t* destructors, per_thread_vars_t* v) {
	for(;;) {
		if(!find(head, key, cmp, v)) {
			return false;
		}
		if(!casp((void* volatile*)&v->current->next, v->next, mark_as_deleted(v->next))) {
			continue;
		}
		if(casp((void* volatile*)v->prev, v->current, v->next)) {
			interlocked_kv_list_node_destructors_t* destructor_copy = smr_alloc(sizeof(interlocked_kv_list_node_destructors_t));
			destructor_copy->key_destructor = destructors->key_destructor;
			destructor_copy->value_destructor = destructors->value_destructor;
			smr_retire_with_finalizer(v->current, &finalize_node, destructor_copy);
		} else {
			find(head, key, cmp, v);
		}
		return true;
	}
}

bool interlocked_kv_list_delete(interlocked_kv_list_t* s, const void* key) {
	per_thread_vars_t v = {0};
	return del(&s->head, key, s->cmp, &s->destructors, &v);
}

bool interlocked_kv_list_find(interlocked_kv_list_t* s, const void* key, void** value) {
	per_thread_vars_t v = {0};
	if(find(&s->head, key, s->cmp, &v)) {
		*value = v.current->value;
		return true;
	} else {
		*value = nullptr;
		return false;
	}
}

bool interlocked_kv_list_is_empty(const interlocked_kv_list_t* s) {
	return s->head == nullptr;
}
