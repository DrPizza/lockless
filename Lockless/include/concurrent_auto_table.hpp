#ifndef CONCURRENT_AUTO_TABLE__HPP
#define CONCURRENT_AUTO_TABLE__HPP

#include "smr.hpp"

#include <limits>
#include <new>
#include <ctime>
#include <unordered_map>

#include <boost/noncopyable.hpp>

template<typename T>
struct array : smr::smr_destructible, boost::noncopyable {
	typedef T element_type;
	typedef array<element_type> array_type;

	array(size_t length_) : length(length_), values(new (smr::smr) element_type[length]) {
	}

	static bool finalize(void*, void* ptr) {
		array_type* a = static_cast<array_type*>(ptr);
		a->~array_type();
		::operator delete(a, smr::smr);
		return false;
	}

	virtual smr::smr_destructible::finalizer_function_t get_finalizer() const {
		return &finalize;
	}

	const size_t length;
	element_type* const values;

protected:
	static bool finalize_elements(void* ctxt, void* ptr) {
		element_type* elements = static_cast<element_type*>(ptr);
		size_t length = reinterpret_cast<size_t>(ctxt);
		for(size_t i(0); i < length; ++i) {
			elements[i].~element_type();
		}
		::operator delete[](elements, smr::smr);
		return false;
	}

	~array() {
		smr::smr_destroy(values, &finalize_elements, reinterpret_cast<void*>(length));
	}
};

// http://high-scale-lib.cvs.sourceforge.net/viewvc/high-scale-lib/high-scale-lib/org/cliffc/high_scale_lib/

// An auto-resizing table of integer_types, supporting low-contention CAS
// operations. Updates are done with CAS's to no particular table element.
// The intent is to support highly scalable counters, r/w locks, and other
// structures where the updates are associative, loss-free (no-brainer), and
// otherwise happen at such a high volume that the cache contention for
// CAS'ing a single word is unacceptable.
// 
// This API is overkill for simple counters (e.g. no need for the 'mask')
// and is untested as an API for making a scalable r/w lock and so is likely
// to change!
template<typename T>
struct concurrent_auto_table : smr::smr_destructible, boost::noncopyable {
	typedef T integer_type;
	typedef concurrent_auto_table<integer_type> table_type;
	typedef std::hash<integer_type> hash_type;
	typedef array<integer_type> array_type;

	concurrent_auto_table() : _cat(new (smr::smr) CAT(nullptr, 4, integer_type())){
	}

	virtual smr::smr_destructible::finalizer_function_t get_finalizer() const {
		return &finalize;
	}

	static bool finalize(void*, void* ptr) {
		table_type* a = static_cast<table_type*>(ptr);
		a->~table_type();
		::operator delete(a, smr::smr);
		return false;
	}

	// Add the given value to current counter value. Concurrent updates will
	// not be lost, but addAndGet or getAndAdd are not implemented because the
	// total counter value (i.e., {@link #get}) is not atomically updated.
	// Updates are striped across an array of counters to avoid cache contention
	// and has been tested with performance scaling linearly up to 768 CPUs.
	void add(integer_type x) {
		add_if_mask(x, integer_type());
	}

	void decrement() {
		add_if_mask(static_cast<integer_type>(-1), integer_type());
	}

	void increment() {
		add_if_mask(static_cast<integer_type>(1), integer_type());
	}

	// Atomically set the sum of the striped counters to specified value.
	// Rather more expensive than a simple store, in order to remain atomic.
	void set(integer_type x) {
		CAT* newcat = new (smr::smr) CAT(nullptr, 4, x);

		smr::hazard_pointers<1> hazards;

		CAT* cat = nullptr;
		for(;;) {
			cat = _cat;
			hazards[0] = _cat;
			if(cat != _cat) {
				continue;
			}
			// Spin until CAS works
			if(!smr::util::cas(&_cat, cat, newcat)) {
				continue;
			}
			smr::smr_destroy(cat);
			break;
		}
	}

	// Current value of the counter. Since other threads are updating furiously
	// the value is only approximate, but it includes all counts made by the
	// current thread. Requires a pass over the internally striped counters.
	integer_type get() const {
		smr::hazard_pointers<1> hazards;

		CAT* cat = nullptr;
		for(;;) {
			cat = _cat;
			hazards[0] = cat;
			if(cat == _cat) {
				break;
			}
		}
		return cat->sum(integer_type());
	}

	// A cheaper {@link #get}. Updated only once/millisecond, but as fast as a
	// simple load instruction when not updating.
	integer_type estimate_get() const {
		smr::hazard_pointers<1> hazards;

		CAT* cat = nullptr;
		for(;;) {
			cat = _cat;
			hazards[0] = cat;
			if(cat == _cat) {
				break;
			}
		}
		return cat->estimate_sum(integer_type());
	}

protected:
	~concurrent_auto_table() {
		smr::smr_destroy(_cat);
	}

private:
	friend struct CAT;
	struct CAT : smr::smr_destructible, boost::noncopyable {
		CAT(CAT* next, size_t sz, integer_type init) : _next(next), _t(new (smr::smr) array_type(sz)), _sum_cache(std::numeric_limits<integer_type>::min()) {
			_t->values[0] = init;
		}

		virtual smr::smr_destructible::finalizer_function_t get_finalizer() const {
			return &finalize;
		}

		static bool finalize(void*, void* ptr) {
			CAT* c = static_cast<CAT*>(ptr);
			c->~CAT();
			::operator delete(c, smr::smr);
			return false;
		}

		// Only add 'x' to some slot in table, hinted at by 'hash', if bits under
		// the mask are all zero. The sum can overflow or 'x' can contain bits in
		// the mask. Value is CAS'd so no counts are lost. The CAS is attempted
		// ONCE.
		integer_type add_if_mask(integer_type x, integer_type mask, size_t hash, concurrent_auto_table* master) {
			smr::hazard_pointers<1> hazards;

			array_type* t = nullptr;
			// get a stable read of the array
			for(;;) {
				t = _t;
				hazards[0] = _t;
				if(t == _t) {
					break;
				}
			}
			size_t idx = hash & (t->length - 1);
			// Peel loop; try once fast
			integer_type old = t->values[idx];
			bool ok = CAS(t->values, idx, old & ~mask, old + x);
			if(_sum_cache != std::numeric_limits<integer_type>::min()) {
				_sum_cache = std::numeric_limits<integer_type>::min(); // Blow out cache
			}
			if(ok) {
				return old; // Got it
			}
			if((old & mask) != 0) {
				return old; // Failed for bit-set under mask
			}
			// Try harder
			size_t cnt = 0;
			for(;;) {
				old = t->values[idx];
				if((old & mask) != 0) {
					return old; // Failed for bit-set under mask
				}
				if(CAS(t->values, idx, old, old + x)) {
					break; // Got it!
				}
				++cnt;
			}
			if(cnt < MAX_SPIN) {
				return old; // Allowable spin loop count
			}
			if(t->length >= 1024 * 1024) {
				return old; // too big already
			}

			// Too much contention; double array size in an effort to reduce contention
			size_t r = _resizers;
			size_t newbytes = (t->length << 1) << sizeof(integer_type); // word to bytes
			while(!smr::util::cas(&_resizers, r, r + newbytes)) {
				r = _resizers;
			}
			r += newbytes;
			if(master->_cat != this) {
				return old; // Already doubled, don't bother
			}
			if((r >> 17) != 0) { // Already too much allocation attempts?
				// TODO - use a wait with timeout, so we'll wakeup as soon as the new
				// table is ready, or after the timeout in any case. Annoyingly, this
				// breaks the non-blocking property - so for now we just briefly sleep.
				YieldProcessor();
				if(master->_cat != this) {
					return old;
				}
			}

			CAT* newcat = new (smr::smr) CAT(this, t->length * 2, integer_type());
			if(!master->CAS_cat(this, newcat)) {
				newcat->_next = nullptr;
				smr::smr_destroy(newcat);
			}
			return old;
		}

		// Return the current sum of all things in the table, stripping off mask
		// before the add. Writers can be updating the table furiously, so the
		// sum is only locally accurate.
		integer_type sum(integer_type mask) const {
			integer_type sum = _sum_cache;
			if(sum != std::numeric_limits<integer_type>::min()) {
				return sum;
			}
			sum = _next == nullptr ? integer_type() : _next->sum(mask); // Recursively get cached sum
			
			smr::hazard_pointers<1> hazards;

			array_type* t = nullptr;
			// get a stable read of the array
			for(;;) {
				t = _t;
				hazards[0] = _t;
				if(t == _t) {
					break;
				}
			}
			for(size_t i = 0; i < t->length; ++i) {
				sum += t->values[i] & ~mask;
			}
			_sum_cache = sum; // Cache includes recursive counts
			return sum;
		}

		// Fast fuzzy version. Used a cached value until it gets old, then re-up
		// the cache.
		integer_type estimate_sum(integer_type mask) const {
			// For short tables, just do the work
			if(_t->length <= 64) {
				return sum(mask);
			}
			// For bigger tables, periodically freshen a cached value
			std::clock_t millis = std::clock();
			if(_fuzzy_time != millis) { // Time marches on?
				_fuzzy_sum_cache = sum(mask); // Get sum the hard way
				_fuzzy_time = millis; // Indicate freshness of cached value
			}
			return _fuzzy_sum_cache; // Return cached sum
		}

		// Update all table slots with CAS.
		void all_or(integer_type mask) {
			smr::hazard_pointers<1> hazards;

			array_type* t = nullptr;
			// get a stable read of the array
			for(;;) {
				t = _t;
				hazards[0] = _t;
				if(t == _t) {
					break;
				}
			}

			for(size_t i = 0; i < t->length; ++i) {
				bool done = false;
				while(!done) {
					integer_type old = t->values[i];
					done = CAS(t->values, i, old, old | mask);
				}
			}
			if(_next != nullptr) {
				_next->all_or(mask);
			}
			if(_sum_cache != std::numeric_limits<integer_type>::min()) {
				_sum_cache = std::numeric_limits<integer_type>::min(); // Blow out cache
			}
		}

		void all_and(integer_type mask) {
			smr::hazard_pointers<1> hazards;

			array_type* t = nullptr;
			// get a stable read of the array
			for(;;) {
				t = _t;
				hazards[0] = _t;
				if(t == _t) {
					break;
				}
			}

			for(size_t i = 0; i < t->length; ++i) {
				bool done = false;
				while(!done) {
					integer_type old = t->values[i];
					done = CAS(t->values, i, old, old & mask);
				}
			}
			if(_next != nullptr) {
				_next->all_and(mask);
			}
			if(_sum_cache != std::numeric_limits<integer_type>::min()) {
				_sum_cache = std::numeric_limits<integer_type>::min(); // Blow out cache
			}
		}

		// Set/stomp all table slots. No CAS.
		void all_set(integer_type val) {
			smr::hazard_pointers<1> hazards;

			array_type* t = nullptr;
			// get a stable read of the array
			for(;;) {
				t = _t;
				hazards[0] = _t;
				if(t == _t) {
					break;
				}
			}

			for(size_t i = 0; i < t->length; ++i) {
				t->values[i] = val;
			}

			if(_next != nullptr) {
				_next->all_set(val);
			}
			if(_sum_cache != std::numeric_limits<integer_type>::min()) {
				_sum_cache = std::numeric_limits<integer_type>::min(); // Blow out cache
			}
		}

	protected:
		~CAT() {
			if(_next != nullptr) {
				smr::smr_destroy(_next);
			}
			smr::smr_destroy(_t);
		}

	private:
		static bool CAS(integer_type* A, size_t idx, integer_type old, integer_type nnn) {
			return smr::util::cas(&A[idx], old, nnn);
		}

		CAT* _next;

		mutable volatile integer_type _sum_cache;
		mutable volatile integer_type _fuzzy_sum_cache;
		mutable volatile std::clock_t _fuzzy_time;
		volatile size_t _resizers; // count of threads attempting a resize

		static const size_t MAX_SPIN = 2;

		array_type* const volatile _t; // Power-of-2 array of integer_types
	};

	hash_type hasher;
	// Hash spreader
	size_t hash() {
		size_t h = hasher(::GetCurrentThreadId());
		h ^= (h >> 20) ^ (h >> 12); // Bit spreader, borrowed from Doug Lea
		h ^= (h >>  7) ^ (h >>  4);
		return h << 2; // Pad out cache lines. The goal is to avoid cache-line contention
	}

	// The underlying array of concurrently updated long counters
	CACHE_ALIGN CAT* volatile _cat;

	// Only add 'x' to some slot in table, hinted at by 'hash', if bits under
	// the mask are all zero. The sum can overflow or 'x' can contain bits in
	// the mask. Value is CAS'd so no counts are lost. The CAS is retried until
	// it succeeds or bits are found under the mask. Returned value is the old
	// value - which WILL have zero under the mask on success and WILL NOT have
	// zero under the mask for failure.
	integer_type add_if_mask(integer_type x, integer_type mask) {
		smr::hazard_pointers<1> hazards;

		CAT* cat = nullptr;
		for(;;) {
			cat = _cat;
			hazards[0] = _cat;
			if(cat == _cat) {
				break;
			}
		}
		return cat->add_if_mask(x, mask, hash(), this);
	}

	bool CAS_cat(CAT* oldcat, CAT* newcat) {
		return smr::util::cas(&_cat, oldcat, newcat);
	}
};

#endif
