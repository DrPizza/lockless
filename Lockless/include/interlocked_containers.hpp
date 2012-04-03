#ifndef INTERLOCKED_CONTAINERS_HPP
#define INTERLOCKED_CONTAINERS_HPP

#include "interlocked_queue.h"
#include "interlocked_stack.h"
#include "interlocked_kv_list.h"
#include <memory>
#include <functional>
#include <utility>
#include <boost/utility.hpp>

#include <queue>

namespace utility
{
	template<typename T>
	struct interlocked_queue : boost::noncopyable {
		typedef size_t size_type;
		typedef T value_type;
		typedef interlocked_queue<T> my_type;

	//	interlocked_queue() {
	//		::InitializeCriticalSection(&cs);
	//	}

	//	~interlocked_queue() {
	//		while(!empty()) {
	//			pop();
	//		}
	//		::DeleteCriticalSection(&cs):
	//	}

	//	void push(const value_type& val) {
	//		::EnterCriticalSection(&cs);
	//		q.push(val);
	//		::LeaveCriticalSection(&cs);
	//	}

	//	std::pair<bool, value_type> pop() {
	//		std::pair<bool, value_type> result(false, value_type());
	//		::EnterCriticalSection(&cs);
	//		if(!q.empty()) {
	//			result.first = true;
	//			result.second = q.front();
	//			q.pop();
	//		}
	//		::LeaveCriticalSection(&cs);
	//		return result;
	//	}

	//	bool empty() const {
	//		return q.empty();
	//	}

	//	size_type approximate_size() const {
	//		return q.size();
	//	}

	//private:
	//	CRITICAL_SECTION cs;
	//	std::queue<T> q;

		interlocked_queue() : q(::new_interlocked_queue(&my_type::value_destructor))
		{
		}

		~interlocked_queue() {
		}

		void push(const value_type& val) {
			::interlocked_queue_push(q.get(), new value_type(val));
		}

		std::pair<bool, value_type> pop() {
			value_type* v(nullptr);
			if(::interlocked_queue_pop(q.get(), reinterpret_cast<void**>(&v))) {
				std::unique_ptr<value_type> ptr(v);
				return std::pair<bool, value_type>(true, *v);
			} else {
				return std::pair<bool, value_type>(false, value_type());
			}
		}

		bool empty() const {
			return ::interlocked_queue_is_empty(q.get());
		}

		size_type approximate_size() const {
			return static_cast<size_type>(::interlocked_queue_depth(q.get()));
		}

	private:
		struct queue_delete {
			void operator()(::interlocked_queue* q) const {
				::delete_interlocked_queue(q);
			}
		};

		static void value_destructor(const void* v) {
			std::unique_ptr<const value_type> p(static_cast<const value_type*>(v));
		}

		std::unique_ptr<::interlocked_queue, queue_delete> q;
	};

	template<typename T>
	struct interlocked_stack : boost::noncopyable {
		typedef size_t size_type;
		typedef T value_type;
		typedef interlocked_stack<T> my_type;

		interlocked_stack() : s(::new_interlocked_stack(&my_type::value_destructor))
		{
		}

		~interlocked_stack() {
			while(!empty()) {
				pop();
			}
		}

		void push(const value_type& val) {
			::interlocked_stack_push(s.get(), new value_type(val));
		}

		std::pair<bool, value_type> pop() {
			value_type* v(nullptr);
			if(::interlocked_stack_pop(s.get(), reinterpret_cast<void**>(&v))) {
				std::unique_ptr<value_type> ptr(v);
				return std::pair<bool, value_type>(true, *v);
			} else {
				return std::pair<bool, value_type>(false, value_type());
			}
		}

		bool empty() const {
			return ::interlocked_stack_is_empty(q.get()):
		}

		size_type approximate_size() const {
			return static_cast<size_type>(::interlocked_stack_depth(s.get()));
		}

	private:
		struct stack_delete {
			void operator()(::interlocked_stack* s) const {
				::delete_interlocked_stack(s);
			}
		};

		static void value_destructor(const void* v) {
			std::unique_ptr<const value_type> p(static_cast<const value_type*>(v));
		}

		std::unique_ptr<::interlocked_stack, stack_delete> s;
	};

	template<typename K, typename V, typename C = std::less<K> >
	struct interlocked_kv_list : boost::noncopyable {
		typedef K key_type;
		typedef V value_type;
		typedef C cmp_type;
		typedef interlocked_kv_list<K, V, C> my_type;

		interlocked_kv_list() : l(::new_interlocked_kv_list(&my_type::comparator, &my_type::key_destructor, &my_type::value_destructor))
		{
		}

		~interlocked_kv_list() {
		}

		bool insert(const key_type& k, const value_type& v) {
			std::unique_ptr<key_type> pk(new key_type(k));
			std::unique_ptr<value_type> pv(new value_type(v));
			if(::interlocked_kv_list_insert(l.get(), pk.get(), pv.get())) {
				pk.release();
				pv.release();
				return true;
			} else {
				return false;
			}
		}

		std::pair<bool, value_type> find(const key_type& k) {
			value_type* v(nullptr);
			if(::interlocked_kv_list_find(l.get(), std::addressof(k), reinterpret_cast<void**>(&v))) {
				return std::pair<bool, value_type>(true, *v);
			} else {
				return std::pair<bool, value_type>(false, value_type());
			}
		}

		bool erase(const key_type& k) {
			return ::interlocked_kv_list_delete(l.get(), std::addressof(k));
		}

		bool empty() const {
			return ::interlocked_kv_list_is_empty(l.get()):
		}

		//size_type approximate_size() const {
		//	return static_cast<size_type>(::interlocked_stack_depth(s.get()));
		//}

	private:
		static int comparator(const void* l, const void* r) {
			cmp_type cmp;
			       if(cmp(*static_cast<const key_type*>(l), *static_cast<const key_type*>(r))) {
				return -1;
			} else if(cmp(*static_cast<const key_type*>(r), *static_cast<const key_type*>(l))) {
				return 1;
			} else {
				return 0;
			}
		}

		static void key_destructor(const void* k) {
			std::unique_ptr<const key_type> p(static_cast<const key_type*>(k));
		}

		static void value_destructor(const void* v) {
			std::unique_ptr<const value_type> p(static_cast<const value_type*>(v));
		}

		struct kv_list_delete {
			void operator()(::interlocked_kv_list_t* s) const {
				::delete_interlocked_kv_list(s);
			}
		};

		std::unique_ptr<::interlocked_kv_list_t, kv_list_delete> l;
	};
}

#endif
