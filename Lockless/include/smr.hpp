#ifndef SMR_HPP
#define SMR_HPP

#include <exception>
#include <new>
#include <memory>
#include <type_traits>

namespace smr {
	namespace detail {
#include "smr.h"
	}

	namespace util {
		template<typename T>
		inline bool cas(T* volatile* addr, T* expected_value, T* new_value)
		{
			T* previous_value = static_cast<T*>(InterlockedCompareExchangePointer(reinterpret_cast<void* volatile*>(addr), new_value, expected_value));
			return expected_value == previous_value;
		}

		inline bool cas(int volatile* addr, int expected_value, int new_value)
		{
			int previous_value = InterlockedCompareExchange(reinterpret_cast<long volatile*>(addr), static_cast<long>(new_value), static_cast<long>(expected_value));
			return expected_value == previous_value;
		}

		inline bool cas(unsigned int volatile* addr, unsigned int expected_value, unsigned int new_value)
		{
			unsigned int previous_value = InterlockedCompareExchange(addr, new_value, expected_value);
			return expected_value == previous_value;
		}

		inline bool cas(long volatile* addr, long expected_value, long new_value)
		{
			long previous_value = InterlockedCompareExchange(addr, new_value, expected_value);
			return expected_value == previous_value;
		}

		inline bool cas(unsigned long volatile* addr, unsigned long expected_value, unsigned long new_value)
		{
			unsigned long previous_value = InterlockedCompareExchange(addr, new_value, expected_value);
			return expected_value == previous_value;
		}

		inline bool cas(long long volatile* addr, long long expected_value, long long new_value)
		{
			long long previous_value = InterlockedCompareExchange64(addr, new_value, expected_value);
			return expected_value == previous_value;
		}

		inline bool cas(unsigned long long volatile* addr, unsigned long long expected_value, unsigned long long new_value)
		{
			unsigned long long previous_value = InterlockedCompareExchange(addr, new_value, expected_value);
			return expected_value == previous_value;
		}
	}


	template<size_t N>
	struct hazard_pointers {
		hazard_pointers() {
			key = detail::allocate_hazard_pointers(N, hazards);
			for(size_t i(0); i < N; ++i) {
				if(!hazards[i]) {
					throw std::bad_alloc();
				}
			}
		}
	
		~hazard_pointers() {
			detail::deallocate_hazard_pointers(key);
		}

		void* volatile& operator[](size_t idx) {
			return *hazards[idx];
		}

	private:
		void* volatile* hazards[N];
		void* key;
	};

	// non-owning pointer that uses a hazard pointer to prevent the pointee from being
	// deleted out from under us. While the raw pointer value is preserved, the hazard
	// *target* is 16 byte aligned. I need the low bits to encode some data, but don't
	// want that to interfere with hazard tracking.
	template<typename T>
	struct stable_pointer {
		typedef T* pointer_type;
		typedef const T* const_pointer_type;
		typedef T& reference_type;
		typedef const T& const_reference_type;
		typedef stable_pointer<T> my_type;
		typedef typename std::remove_const<T>::type bare_type;

		explicit stable_pointer(T* volatile* location) : pointer(nullptr), hazard(new hazard_pointers<1>()) {
			(*this) = location;
		}

		stable_pointer() : pointer(nullptr), hazard(new hazard_pointers<1>()) {
		}

		my_type& operator=(T* volatile* location) {
			for(;;) {
				pointer = *location;
				(*hazard)[0] = align_pointer(const_cast<bare_type*>(pointer));
				if(*location == pointer) {
					break;
				}
			}
			return *this;
		}

		// assign a pointer known to be unique and valid and/or persistent
		void unshared_assign(pointer_type ptr) {
			pointer = ptr;
			(*hazard)[0] = align_pointer(ptr);
		}

		pointer_type& get_pointer() {
			return pointer;
		}

		pointer_type get_pointer() const {
			return pointer;
		}

		void* volatile& get_hazard_pointer() {
			return (*hazard)[0];
		}

		reference_type operator*() {
			return *pointer;
		}

		const_reference_type operator*() const {
			return *pointer;
		}

		pointer_type operator->() {
			return pointer;
		}

		const_pointer_type operator->() const {
			return pointer;
		}
	
		bool operator==(const my_type& rhs) const {
			return pointer == rhs.pointer;
		}
	
		bool operator!=(const my_type& rhs) const {
			return !(*this == rhs);
		}
	
		bool operator==(std::nullptr_t) const {
			return pointer == nullptr;
		}
	
		bool operator!=(std::nullptr_t) const {
			return !(pointer == nullptr);
		}
	
		bool operator==(const_pointer_type rhs) const {
			return pointer == rhs;
		}

		bool operator!=(const_pointer_type rhs) const {
			return !(pointer == rhs);
		}

	private:
		bare_type* align_pointer(bare_type* p) const
		{
			size_t val = reinterpret_cast<size_t>(p);
			val &= ~0xf;
			return reinterpret_cast<bare_type*>(val);
		}
	
		pointer_type pointer;
		std::shared_ptr<hazard_pointers<1> > hazard;
	};

	template<typename T>
	stable_pointer<T> make_stable_pointer(T* volatile* location) {
		return stable_pointer<T>(location);
	}

	template<typename T>
	stable_pointer<T> make_unshared_stable_pointer(T* value) {
		stable_pointer<T> p;
		p.unshared_assign(value);
		return p;
	}

	struct smr_t {};
	extern smr_t smr;

	struct smr_destructible {
		typedef detail::finalizer_function_t finalizer_function_t;
		virtual finalizer_function_t get_finalizer() const = 0;

		virtual ~smr_destructible() {
		}
	};

	inline void smr_destroy(smr_destructible* s) {
		detail::smr_retire_with_finalizer(s, s->get_finalizer(), nullptr);
	}

	inline void smr_destroy(void* ptr) {
		detail::smr_retire(ptr);
	}

	typedef detail::finalizer_function_t finalizer_function_t;
	inline void smr_destroy(void* ptr, finalizer_function_t fin, void* ctxt) {
		detail::smr_retire_with_finalizer(ptr, fin, ctxt);
	}

	template<typename T>
	inline void smr_destroy(const stable_pointer<T>& ptr, finalizer_function_t fin, void* ctxt) {
		typedef typename std::remove_const<T>::type bare_type;
		detail::smr_retire_with_finalizer(const_cast<bare_type*>(ptr.get_pointer()), fin, ctxt);
	}
}

inline void* operator new(size_t sz, const smr::smr_t&) {
	return smr::detail::smr_alloc(sz);
}

inline void* operator new[](size_t sz, const smr::smr_t&) {
	return smr::detail::smr_alloc(sz);
}

inline void operator delete(void* ptr, const smr::smr_t&) {
	smr::detail::smr_free(ptr);
}

inline void operator delete[](void* ptr, const smr::smr_t&) {
	smr::detail::smr_free(ptr);
}

#endif
