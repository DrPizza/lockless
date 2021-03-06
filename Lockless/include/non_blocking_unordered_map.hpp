#ifndef NON_BLOCKING_UNORDERED_MAP_HPP
#define NON_BLOCKING_UNORDERED_MAP_HPP

#include "smr.hpp"

#include "concurrent_auto_table.hpp"

#include <cassert>
#include <boost/noncopyable.hpp>
#include <boost/optional.hpp>

// http://high-scale-lib.cvs.sourceforge.net/viewvc/high-scale-lib/high-scale-lib/org/cliffc/high_scale_lib/

// A lock-free alternate implementation of {@link java.util.concurrent.ConcurrentHashMap}
// with better scaling properties and generally lower costs to mutate the Map.
// It provides identical correctness properties as ConcurrentHashMap. All
// operations are non-blocking and multi-thread safe, including all update
// operations. {@link NonBlockingHashMap} scales substatially better than
// {@link java.util.concurrent.ConcurrentHashMap} for high update rates, even with a
// large concurrency factor. Scaling is linear up to 768 CPUs on a 768-CPU
// Azul box, even with 100% updates or 100% reads or any fraction in-between.
// Linear scaling up to all cpus has been observed on a 32-way Sun US2 box,
// 32-way Sun Niagra box, 8-way Intel box and a 4-way Power box.
// 
// This class obeys the same functional specification as {@link
// java.util.Hashtable}, and includes versions of methods corresponding to
// each method of <tt>Hashtable</tt>. However, even though all operations are
// thread-safe, operations do <em>not</em> entail locking and there is
// <em>not</em> any support for locking the entire table in a way that
// prevents all access. This class is fully interoperable with
// <tt>Hashtable</tt> in programs that rely on its thread safety but not on
// its synchronization details.
// 
// <p> Operations (including <tt>put</tt>) generally do not block, so may
// overlap with other update operations (including other <tt>puts</tt> and
// <tt>removes</tt>). Retrievals reflect the results of the most recently
// <em>completed</em> update operations holding upon their onset. For
// aggregate operations such as <tt>putAll</tt>, concurrent retrievals may
// reflect insertion or removal of only some entries. Similarly, Iterators
// and Enumerations return elements reflecting the state of the hash table at
// some point at or since the creation of the iterator/enumeration. They do
// <em>not</em> throw {@link ConcurrentModificationException}. However,
// iterators are designed to be used by only one thread at a time.
// 
// <p> Very full tables, or tables with high reprobe rates may trigger an
// internal resize operation to move into a larger table. Resizing is not
// terribly expensive, but it is not free either; during resize operations
// table throughput may drop somewhat. All threads that visit the table
// during a resize will 'help' the resizing but will still be allowed to
// complete their operation before the resize is finished (i.e., a simple
// 'get' operation on a million-entry table undergoing resizing will not need
// to block until the entire million entries are copied).
// 
// <p>This class and its views and iterators implement all of the
// <em>optional</em> methods of the {@link Map} and {@link Iterator}
// interfaces.
// 
// <p> Like {@link Hashtable} but unlike {@link HashMap}, this class
// does <em>not</em> allow <tt>null</tt> to be used as a key or value.
template<typename Key, typename Value, typename H = std::hash<Key>, typename KE = std::equal_to<Key>, typename VE = std::equal_to<Value> >
struct non_blocking_unordered_map : boost::noncopyable, smr::smr_destructible {
	typedef Key key_type;
	typedef Value value_type;
	typedef H hash_type;
	typedef KE key_equal;
	typedef VE value_equal;
	typedef non_blocking_unordered_map<key_type, value_type, hash_type, key_equal, value_equal> map_type;
	typedef boost::optional<value_type> result_type;

	typedef array<void*> kv_array_type;
	typedef array<size_t> hash_array_type;
	typedef concurrent_auto_table<size_t> counter_t;

	non_blocking_unordered_map(size_t initial_size = MIN_SIZE) : _reprobes(new (smr::smr) counter_t()), _size(new (smr::smr) counter_t()) {
		initial_size = std::min(initial_size, static_cast<size_t>(1024U * 1024U));
		size_t i = MIN_SIZE_LOG;
		for(; (static_cast<size_t>(1U) << i) < (initial_size << static_cast<size_t>(2U)); ++i) {
		}
		_kvs = new (smr::smr) kv_array_type(((1 << i) << 1) + 2);
		_kvs.load()->values[0] = new (smr::smr) CHM();
		_kvs.load()->values[1] = new (smr::smr) hash_array_type(1 << i);
		_last_resize_milli = std::clock();
	}

	size_t size() const {
		return _size->get();
	}

	bool is_empty() const {
		return size() == 0;
	}

	bool containsKey(const key_type& key) {
		return get(key) != nullptr;
	}

	result_type put(const key_type& key, const value_type& val) {
		key_type* k = new (smr::smr) key_type(key);
		value_type* v = new (smr::smr) value_type(val);
		smr::stable_pointer<value_type> r = putIfMatch(k, v, NO_MATCH_OLD());
		return do_cleanup(r, k, v);
	}

	result_type putIfAbsent(const key_type& key, const value_type& val) {
		key_type* k = new (smr::smr) key_type(key);
		value_type* v = new (smr::smr) value_type(val);
		smr::stable_pointer<value_type> r = putIfMatch(k, v, TOMBSTONE());
		return do_cleanup(r, k, v);
	}

	result_type remove(const key_type& key) {
		key_type* k = new (smr::smr) key_type(key);
		smr::stable_pointer<value_type> r = putIfMatch(k, TOMBSTONE(), NO_MATCH_OLD());
		return do_cleanup(r, k, TOMBSTONE());
	}

	bool remove(const key_type& key, const value_type& val) {
		smr::stable_pointer<value_type> r = putIfMatch(&key, TOMBSTONE(), &val);
		bool clean_key = false, clean_value = false, clean_return = false;
		r = untwiddle_bits(r, clean_key, clean_value, clean_return);

		if(r == nullptr) {
			return false;
		} else {
			if(clean_return) {
				smr::smr_destroy(r, &finalize_value, nullptr);
			}
			return true;
		}
	}

	result_type replace(const key_type& key, const value_type& val) {
		key_type* k = new (smr::smr) key_type(key);
		value_type* v = new (smr::smr) value_type(val);
		smr::stable_pointer<value_type> r = putIfMatch(k, v, MATCH_ANY());
		return do_cleanup(r, k, v);
	}

	bool replace(const key_type& key, const value_type& oldValue, const value_type& newValue) {
		key_type* k = new (smr::smr) key_type(key);
		value_type* v = new (smr::smr) value_type(newValue);
		smr::stable_pointer<value_type> r = putIfMatch(k, v, &oldValue);
		bool clean_key = false, clean_value = false, clean_return = false;
		r = untwiddle_bits(r, clean_key, clean_value, clean_return);

		if(clean_key) {
			smr::smr_destroy(k, &finalize_key, nullptr);
		}
		if(clean_value) {
			smr::smr_destroy(v, &finalize_value, nullptr);
		}

		if(r == nullptr) {
			return false;
		} else {
			if(clean_return) {
				smr::smr_destroy(r, &finalize_value, nullptr);
			}
			return true;
		}
	}

	void clear() {
		map_type* replacement = new (smr::smr) map_type(MIN_SIZE);
		smr::stable_pointer<kv_array_type> kvs(_kvs);
		smr::stable_pointer<kv_array_type> rep(replacement->_kvs);
		while(!CAS_kvs(kvs, rep)) {
			kvs = _kvs;
		}
		smr::smr_destroy(kvs, &finalize_kvs, nullptr);
	}

	result_type get(const key_type& key) {
		size_t fullhash = map_type::hash(key);
		smr::stable_pointer<kv_array_type> kvs(_kvs);
		smr::stable_pointer<value_type> V(get_impl(this, kvs, &key, fullhash));
		assert(!is_prime(V.get_pointer()));
		if(V.get_pointer() == nullptr) {
			return result_type();
		} else {
			return result_type(*V);
		}
	}

	static bool finalize(void*, void* ptr) {
		map_type* a = static_cast<map_type*>(ptr);
		a->~map_type();
		::operator delete(a, smr::smr);
		return false;
	}

	virtual smr::smr_destructible::finalizer_function_t get_finalizer() const {
		return &finalize;
	}

protected:
	~non_blocking_unordered_map() {
		smr::smr_destroy(_kvs, &finalize_kvs, nullptr);
		smr::smr_destroy(_size);
		smr::smr_destroy(_reprobes.load());
	}

private:
	smr::stable_pointer<value_type> putIfMatch(const key_type* const key, value_type* const newVal, const value_type* const oldVal) {
		smr::stable_pointer<kv_array_type> kvs(_kvs);
		smr::stable_pointer<value_type> res = putIfMatch(this, kvs, key, newVal, oldVal);
		return res;
	}

	static bool finalize_key(void*, void* ptr) {
		key_type* k = static_cast<key_type*>(ptr);
		k->~key_type();
		::operator delete(k, smr::smr);
		return false;
	}

	static bool finalize_value(void*, void* ptr) {
		value_type* m = static_cast<value_type*>(ptr);
		m->~value_type();
		::operator delete(m, smr::smr);
		return false;
	}

	static bool finalize_kvs(void* ctxt, void* ptr) {
		kv_array_type* arr = static_cast<kv_array_type*>(ptr);
		bool shallow_finalize = reinterpret_cast<size_t>(ctxt) != 0;
		smr::stable_pointer<kv_array_type> sk(&arr);
		size_t count = map_type::len(sk);
		for(size_t i(0); i < count; ++i) {
			smr::stable_pointer<value_type>     v(map_type::val(sk, i));
			smr::stable_pointer<const key_type> k(map_type::key(sk, i));
			if(( shallow_finalize && map_type::is_prime(k.get_pointer()) && map_type::unprime(k.get_pointer()) != TOMBSTONEK())
			|| (!shallow_finalize && k.get_pointer() != nullptr && k.get_pointer() != TOMBSTONEK()))
			{
				smr::smr_destroy(map_type::unprime(const_cast<key_type*>(k.get_pointer())), &finalize_key, nullptr);
			}
			if(!shallow_finalize && v.get_pointer() != nullptr && v.get_pointer() != map_type::TOMBSTONE() && v.get_pointer() != map_type::TOMBPRIME())
			{
				smr::smr_destroy(map_type::unprime(v.get_pointer()), &finalize_value, nullptr);
			}
		}
		smr::smr_destroy(hashes(sk));
		smr::smr_destroy(chm(sk));
		kv_array_type::finalize(nullptr, arr);
		return false;
	}

	static bool keyeq(const key_type* const K, const key_type* const key, hash_array_type* const hashes, const size_t hash, const size_t fullhash) {
		key_equal ke;
		return K == key ||
		       ((hashes->values[hash] == 0U || hashes->values[hash] == fullhash) &&
		        K != TOMBSTONEK() &&
		        ke(*key, *K));
	}

	static bool valeq(const value_type* const V, const value_type* const value) {
		value_equal ve;
		return map_type::unprime(V) == map_type::unprime(value) ||
		       ((V     != map_type::TOMBPRIME() && V     != map_type::TOMBSTONE() && V     != nullptr) &&
		        (value != map_type::TOMBPRIME() && value != map_type::TOMBSTONE() && value != nullptr) && 
		        ve(*value, *V));
	}

	smr::stable_pointer<value_type> get_impl(map_type* const topmap, const smr::stable_pointer<kv_array_type> kvs, const key_type* const key, const size_t fullhash) {
		const size_t           len    = map_type::len(kvs);
		CHM*             const chm    = map_type::chm(kvs);
		hash_array_type* const hashes = map_type::hashes(kvs);

		size_t idx = fullhash & (len - 1);
		size_t reprobe_cnt = 0;
		for(;;) {
			smr::stable_pointer<const key_type   > K = map_type::key(kvs, idx);
			smr::stable_pointer<      value_type> V = map_type::val(kvs, idx);
			if(K == nullptr) {
				return smr::stable_pointer<value_type>();
			}

			smr::stable_pointer<kv_array_type> newkvs(chm->_newkvs);
			if(map_type::keyeq(map_type::unprime(K.get_pointer()), key, hashes, idx, fullhash)) {
				if(!map_type::is_prime(V.get_pointer())) {
					return V == map_type::TOMBSTONE() ? smr::stable_pointer<value_type>() : V;
				}
				return get_impl(topmap, chm->copy_slot_and_check(topmap, kvs, idx, key), key, fullhash);
			}

			if(++reprobe_cnt >= reprobe_limit(len) || K == map_type::TOMBSTONEK()) {
				return newkvs == nullptr ? smr::stable_pointer<value_type>() : get_impl(topmap, topmap->help_copy(newkvs), key, fullhash);
			}
			idx = (idx + 1) & (len - 1);
		}
	}

	enum struct clean_bits : size_t {
		key = 0x1,
		val = 0x2,
		ret = 0x4,
		mask = key | val | ret,
	};

	static smr::stable_pointer<value_type> twiddle_bits(smr::stable_pointer<value_type> ptr, bool clean_key, bool clean_value, bool clean_return) {
		size_t raw = reinterpret_cast<size_t>(ptr.get_pointer());
		raw &= ~static_cast<size_t>(clean_bits::mask);
		if(raw == reinterpret_cast<size_t>(TOMBSTONE())) {
			raw = 0;
		}
		if(clean_key) {
			raw |= static_cast<size_t>(clean_bits::key);
		}
		if(clean_value) {
			raw |= static_cast<size_t>(clean_bits::val);
		}
		if(clean_return) {
			raw |= static_cast<size_t>(clean_bits::ret);
		}
		ptr.get_pointer() = reinterpret_cast<smr::stable_pointer<value_type>::pointer_type>(raw);
		return ptr;
	}

	static smr::stable_pointer<value_type> untwiddle_bits(smr::stable_pointer<value_type> ptr, bool& clean_key, bool& clean_value, bool& clean_return) {
		size_t raw = reinterpret_cast<size_t>(ptr.get_pointer());
		clean_key    = (raw & static_cast<size_t>(clean_bits::key)) != 0;
		clean_value  = (raw & static_cast<size_t>(clean_bits::val)) != 0;
		clean_return = (raw & static_cast<size_t>(clean_bits::ret)) != 0;
		raw &= ~static_cast<size_t>(clean_bits::mask);
		ptr.get_pointer() = reinterpret_cast<smr::stable_pointer<value_type>::pointer_type>(raw);
		return ptr;
	}

	static result_type do_cleanup(smr::stable_pointer<value_type> r, key_type* k, value_type* v) {
		bool clean_key = false, clean_value = false, clean_return = false;
		r = untwiddle_bits(r, clean_key, clean_value, clean_return);
		if(clean_key) {
			smr::smr_destroy(k, &finalize_key, nullptr);
		}
		if(clean_value) {
			smr::smr_destroy(v, &finalize_value, nullptr);
		}
	
		if(r.get_pointer() == nullptr) {
			return result_type();
		} else {
			result_type res = *r;
			if(clean_return) {
				smr::smr_destroy(r, &finalize_value, nullptr);
			}
			return res;
		}
	}

	static smr::stable_pointer<value_type> putIfMatch(map_type* const topmap, smr::stable_pointer<kv_array_type> kvs, const key_type* const key, value_type* putval, const value_type* const expVal) {
		assert(putval != nullptr);
		assert(!is_prime(putval));
		assert(!is_prime(expVal));
		const size_t           fullhash = map_type::hash(*key);
		const size_t           len      = map_type::len(kvs);
		CHM*             const chm      = map_type::chm(kvs);
		hash_array_type* const hashes   = map_type::hashes(kvs);

		size_t idx = fullhash & (len - 1);

		bool clean_key    = true;
		bool clean_value  = true;
		bool clean_return = true;

		size_t reprobe_cnt = 0;
		smr::stable_pointer<const key_type> K;
		smr::stable_pointer<value_type>    V;
		smr::stable_pointer<kv_array_type> newkvs;
		for(;;) {
			V = map_type::val(kvs, idx); // Get old value (before volatile read below!)
			K = map_type::key(kvs, idx); // Get current key
			if(K == nullptr) { // Slot is free?
				// Found an empty Key slot - which means this Key has never been in
				// this table.  No need to put a Tombstone - the Key is not here!
				if(putval == map_type::TOMBSTONE()) {
					clean_value  = false; // both the input value
					clean_return = false; // and the return value are tombstones and do not need cleaning
					return twiddle_bits(smr::make_unshared_stable_pointer(putval), clean_key, clean_value, clean_return); // Not-now & never-been in this table
				}
				// Claim the null key-slot
				if(CAS_key(kvs, idx, nullptr, key)) { // Claim slot for Key
					chm->_slots->increment();        // Raise key-slots-used count
					hashes->values[idx] = fullhash;  // Memoize fullhash
					clean_key = false;               // ownership of the key has been transferred into the map
					break;                           // Got it!
				}
				// CAS to claim the key-slot failed
				K = map_type::key(kvs, idx);
				assert(K != nullptr);
			}
		
			// Key slot was not null, there exists a Key here

			// We need a volatile-read here to preserve happens-before semantics on
			// newly inserted Keys.  If the Key body was written just before inserting
			// into the table a Key-compare here might read the uninitalized Key body.
			// Annoyingly this means we have to volatile-read before EACH key compare.
			newkvs = chm->_newkvs; // VOLATILE READ before key compare
			if(keyeq(map_type::unprime(K.get_pointer()), key, hashes, idx, fullhash)) {
				break; // Got it!
			}

			// get and put must have the same key lookup logic!  Lest 'get' give
			// up looking too soon.
			if(++reprobe_cnt >= reprobe_limit(len) || // too many probes or
			   K == TOMBSTONEK()) { // found a TOMBSTONE key, means no more keys
				// We simply must have a new table to do a 'put'.  At this point a
				// 'get' will also go to the new table (if any).  We do not need
				// to claim a key slot (indeed, we cannot find a free one to claim!).
				newkvs = chm->resize(topmap, kvs);
				if(expVal != nullptr) {
					topmap->help_copy(newkvs); // help along an existing copy
				}
				return putIfMatch(topmap, newkvs, key, putval, expVal);
			}
			idx = (idx + 1) & (len - 1); // Reprobe!
		} // End of spinning till we get a Key slot

		// Found the proper Key slot, now update the matching Value slot.  We
		// never put a null, so Value slots monotonically move from null to
		// not-null (deleted Values use Tombstone).  Thus if 'V' is null we
		// fail this fast cutout and fall into the check for table-full.
		if(valeq(map_type::unprime(V.get_pointer()), putval)) {
		//if(V == putval) {
			if(putval == TOMBSTONE() || putval == TOMBPRIME()) {
				clean_value = false; // fake values don't need cleaning
			}
			clean_return = false; // in any case, we're not giving up ownership
			return twiddle_bits(V, clean_key, clean_value, clean_return); // Fast cutout for no-change
		}
	
		// See if we want to move to a new table (to avoid high average re-probe
		// counts).  We only check on the initial set of a Value from null to
		// not-null (i.e., once per key-insert).  Of course we got a 'free' check
		// of newkvs once per key-compare (not really free, but paid-for by the
		// time we get here).
		if(newkvs == nullptr && // New table-copy already spotted?
		  // Once per fresh key-insert check the hard way
		  ((V == nullptr && chm->tableFull(reprobe_cnt, len)) ||
		  // Or we found a Prime, but the JMM allowed reordering such that we
		  // did not spot the new table (very rare race here: the writing
		  // thread did a CAS of _newkvs then a store of a Prime.  This thread
		  // reads the Prime, then reads _newkvs - but the read of Prime was so
		  // delayed (or the read of _newkvs was so accelerated) that they
		  // swapped and we still read a null _newkvs.  The resize call below
		  // will do a CAS on _newkvs forcing the read.
		  map_type::is_prime(V.get_pointer()))) {
			newkvs = chm->resize(topmap, kvs); // Force the new table copy to start
		}
		// See if we are moving to a new table.
		// If so, copy our slot and retry in the new table.
		if(newkvs != nullptr) {
			return putIfMatch(topmap, chm->copy_slot_and_check(topmap, kvs, idx, expVal), key, putval, expVal);
		}

		// ok, there's no resize going on, so let's update in-place
		//if(K != nullptr && K != key && !map_type::is_prime(K.get_pointer())) {
		//	if(CAS_key(kvs, idx, K.get_pointer(), map_type::prime(K.get_pointer()))) {
		//		// won the race to mark this as prime
		//		// we and only we can replace the key pointer
		//		if(CAS_key(kvs, idx, map_type::prime(K.get_pointer()), key)) {
		//			smr::smr_destroy(K, &finalize_key, nullptr);
		//		}
		//	}
		//}

		// We are finally prepared to update the existing table
		for(;;) {
			assert(!is_prime(V.get_pointer()));
			// Must match old, and we do not?  Then bail out now.  Note that either V
			// or expVal might be TOMBSTONE.  Also V can be null, if we've never
			// inserted a value before.  expVal can be null if we are called from
			// copy_slot.
			value_equal veq;
			if(expVal != map_type::NO_MATCH_OLD() && // Do we care about expected-Value at all?
			   V != expVal &&                        // No instant match already?
			   (expVal != map_type::MATCH_ANY() || V == map_type::TOMBSTONE() || V == nullptr) &&
			   !(V == nullptr && expVal == map_type::TOMBSTONE()) &&                          // Match on null/TOMBSTONE combo
			   (expVal == nullptr || expVal == map_type::TOMBSTONE() || !veq(*expVal, *V))) { // Expensive equals check at the last
				clean_return = false; // not giving up ownership
				return twiddle_bits(V, clean_key, clean_value, clean_return);
			}

			if(CAS_val(kvs, idx, V.get_pointer(), putval)) {
				clean_value = false; // ownership of the new value has been transferred into the map
				if(expVal != nullptr) {
					if( (V == nullptr || V == map_type::TOMBSTONE()) && putval != map_type::TOMBSTONE()) {
						topmap->_size->increment();
					}
					if(!(V == nullptr || V == map_type::TOMBSTONE()) && putval == map_type::TOMBSTONE()) {
						topmap->_size->decrement();
					}
				}
				if(V == nullptr || V == map_type::TOMBSTONE()) {
					clean_return = false; // fake values don't need cleaning
				}
				return twiddle_bits((V == nullptr && expVal != nullptr) ? smr::make_unshared_stable_pointer(map_type::TOMBSTONE())
				                                                        : V,
				                    clean_key, clean_value, clean_return);
			}
			V = val(kvs, idx);
			if(map_type::is_prime(V.get_pointer())) {
				return putIfMatch(topmap, chm->copy_slot_and_check(topmap, kvs, idx, expVal), key, putval, expVal);
			}
		}
	}

	smr::stable_pointer<kv_array_type> help_copy(smr::stable_pointer<kv_array_type> helper) {
		smr::stable_pointer<kv_array_type> topkvs(_kvs);
		CHM* topchm = map_type::chm(topkvs);
		if(topchm->_newkvs.load() == nullptr) {
			return helper;
		}
		topchm->help_copy_impl(this, topkvs, false);
		return helper;
	}

	friend struct CHM;
	// The control structure for the NonBlockingHashMap
	struct CHM : boost::noncopyable, smr::smr_destructible {
		friend struct map_type;

		size_t slots() const {
			return _slots->get();
		}

		CHM() : _slots(new (smr::smr) counter_t()), _newkvs(nullptr), _resizers(0), _copyIdx(0), _copyDone(0) {
		}
	
		static bool finalize(void*, void* ptr) {
			CHM* c = static_cast<CHM*>(ptr);
			c->~CHM();
			::operator delete(c, smr::smr);
			return false;
		}

		virtual smr::smr_destructible::finalizer_function_t get_finalizer() const {
			return &finalize;
		}

	protected:
		~CHM() {
			smr::smr_destroy(_slots);
		}

	private:
		// These next 2 fields are used in the resizing heuristics, to judge when
		// it is time to resize or copy the table. Slots is a count of used-up
		// key slots, and when it nears a large fraction of the table we probably
		// end up reprobing too much. Last-resize-milli is the time since the
		// last resize; if we are running back-to-back resizes without growing
		// (because there are only a few live keys but many slots full of dead
		// keys) then we need a larger table to cut down on the churn.
		
		// Count of used slots, to tell when table is full of dead unusable slots
		counter_t* const _slots;

		// New mappings, used during resizing.
		// The 'new KVs' array - created during a resize operation. This
		// represents the new table being copied from the old one. It's the
		// volatile variable that is read as we cross from one table to the next,
		// to get the required memory orderings. It monotonically transits from
		// null to set (once).
		std::atomic<kv_array_type*> _newkvs;
		// Set the _next field if we can.
		bool CAS_newkvs(smr::stable_pointer<kv_array_type> newkvs) {
			while(_newkvs.load() == nullptr) {
				kv_array_type* expected = nullptr;
				if(_newkvs.compare_exchange_strong(expected, newkvs.get_pointer())) {
					return true;
				}
			}
			return false;
		}

		// Sometimes many threads race to create a new very large table. Only 1
		// wins the race, but the losers all allocate a junk large table with
		// hefty allocation costs. Attempt to control the overkill here by
		// throttling attempts to create a new table. I cannot really block here
		// (lest I lose the non-blocking property) but late-arriving threads can
		// give the initial resizing thread a little time to allocate the initial
		// new table. The Right Long Term Fix here is to use array-lets and
		// incrementally create the new very large array. In C I'd make the array
		// with malloc (which would mmap under the hood) which would only eat
		// virtual-address and not real memory - and after Somebody wins then we
		// could in parallel initialize the array. Java does not allow
		// un-initialized array creation (especially of ref arrays!).
		std::atomic<size_t> _resizers;

		// Heuristic to decide if this table is too full, and we should start a
		// new table. Note that if a 'get' call has reprobed too many times and
		// decided the table must be full, then always the estimate_sum must be
		// high and we must report the table is full. If we do not, then we might
		// end up deciding that the table is not full and inserting into the
		// current table, while a 'get' has decided the same key cannot be in this
		// table because of too many reprobes. The invariant is:
		// slots.estimate_sum >= max_reprobe_cnt >= reprobe_limit(len)
		bool tableFull(size_t reprobe_cnt, size_t len) {
			return
				// Do the cheap check first: we allow some number of reprobes always
				reprobe_cnt >= REPROBE_LIMIT && 
				// More expensive check: see if the table is > 1/4 full.
				_slots->estimate_get() >= map_type::reprobe_limit(len);
		}

		// Resizing after too many probes. "How Big???" heuristics are here.
		// Callers will (not this routine) will 'help_copy' any in-progress copy.
		// Since this routine has a fast cutout for copy-already-started, callers
		// MUST 'help_copy' lest we have a path which forever runs through
		// 'resize' only to discover a copy-in-progress which never progresses.
		smr::stable_pointer<kv_array_type> resize(map_type* topmap, smr::stable_pointer<kv_array_type> kvs) {
			assert(chm(kvs) == this);

			// Check for resize already in progress, probably triggered by another thread
			smr::stable_pointer<kv_array_type> newkvs(_newkvs);
			if(newkvs.get_pointer() != nullptr) {
				return newkvs;
			}

			// No copy in-progress, so start one. First up: compute new table size.
			size_t oldlen = map_type::len(kvs); // Old count of K,V pairs allowed
			size_t sz = topmap->size(); // Get current table count of active K,V pairs
			size_t newsz = sz; // First size estimate

			// Heuristic to determine new size. We expect plenty of dead-slots-with-keys
			// and we need some decent padding to avoid endless reprobing.
			if(sz >= (oldlen >> 2U)) { // If we are >25% full of keys then...
				newsz = oldlen << 1U; // Double size
				if(sz >= (oldlen >> 1U)) { // If we are >50% full of keys then...
					newsz = oldlen << 2U; // Double double size
				}
			}

			// Last (re)size operation was very recent? Then double again; slows
			// down resize operations for tables subject to a high key churn rate.
			std::clock_t tm = std::clock();
			size_t q = 0U;
			if(newsz <= oldlen && // New table would shrink or hold steady?
			   tm <= topmap->_last_resize_milli + 1000L && // Recent resize (less than 1 sec ago)
			   (q = _slots->estimate_get()) >= (sz << 1U)) {// 1/2 of keys are dead?
				newsz = oldlen << 1U;
			}

			// Do not shrink, ever
			if(newsz < oldlen) {
				newsz = oldlen;
			}

			// Convert to power-of-2
			size_t log2 = MIN_SIZE_LOG;
			for(; (static_cast<size_t>(1U) << log2) < newsz; ++log2) { // Compute log2 of size
			}

			// Now limit the number of threads actually allocating memory to a
			// handful - lest we have 750 threads all trying to allocate a giant
			// resized array.
			size_t r = _resizers.load();
			while(!_resizers.compare_exchange_strong(r, r + 1)) {
				r = _resizers.load();
			}

			// Size calculation: 2 words (K+V) per table entry, plus a handful.
			int megs = ((((1 << log2) << 1) + 2) * sizeof(void*) /*word to bytes*/) >> 20 /*megs*/;
			if(r >= 2 && megs > 0) { // Already 2 guys trying; wait and see
				newkvs = _newkvs; // Between dorking around, another thread did it
				if(newkvs.get_pointer() != nullptr) { // See if resize is already in progress
					return newkvs; // Use the new table already
				}
				// For now, sleep a tad and see if the 2 guys already trying to make
				// the table actually get around to making it happen.
				YieldProcessor();
			}

			// Last check, since the 'new' below is expensive and there is a chance
			// that another thread slipped in a new thread while we ran the heuristic.
			newkvs = _newkvs;
			if(newkvs.get_pointer() != nullptr) { // See if resize is already in progress
				return newkvs; // Use the new table already
			}

			// Double size for K,V pairs, add 1 for CHM
			newkvs.unshared_assign(new (smr::smr) kv_array_type(((1 << log2) << 1) + 2)); // This can get expensive for big arrays
			newkvs->values[0] = new (smr::smr) CHM(); // CHM in slot 0
			newkvs->values[1] = new (smr::smr) hash_array_type(1 << log2); // hashes in slot 1

			if(_newkvs.load() != nullptr) {
				smr::smr_destroy(newkvs.get_pointer(), &map_type::finalize_kvs, nullptr);
				return smr::stable_pointer<kv_array_type>(_newkvs);
			}
			
			// The new table must be CAS'd in so only 1 winner amongst duplicate
			// racing resizing threads. Extra CHM's will be GC'd.
			if(!CAS_newkvs(newkvs)) {
				smr::smr_destroy(newkvs.get_pointer(), &map_type::finalize_kvs, nullptr);
				newkvs = _newkvs; // Reread new table
			}
			return newkvs;
		}

		// The next part of the table to copy. It monotonically transits from zero
		// to _kvs.length. Visitors to the table can claim 'work chunks' by
		// CAS'ing this field up, then copying the indicated indices from the old
		// table to the new table. Workers are not required to finish any chunk;
		// the counter simply wraps and work is copied duplicately until somebody
		// somewhere completes the count.
		std::atomic<size_t> _copyIdx;
		// Work-done reporting. Used to efficiently signal when we can move to
		// the new table. From 0 to len(oldkvs) refers to copying from the old
		// table to the new.
		std::atomic<size_t> _copyDone;

		// Help along an existing resize operation. We hope its the top-level
		// copy (it was when we started) but this CHM might have been promoted out
		// of the top position.
		void help_copy_impl(map_type* topmap, smr::stable_pointer<kv_array_type> oldkvs, bool copy_all) {
			assert(chm(oldkvs) == this);
			smr::stable_pointer<kv_array_type> newkvs(_newkvs);
			assert(newkvs != nullptr);
			size_t oldlen = map_type::len(oldkvs); // Total amount to copy
			const size_t MIN_COPY_WORK = std::min(oldlen, static_cast<size_t>(1024U)); // Limit per-thread work

			size_t panic_start = ~0U;
			size_t copyidx = ~0U;
			while(_copyDone.load() < oldlen) { // Still needing to copy?
				// Carve out a chunk of work. The counter wraps around so every
				// thread eventually tries to copy every slot repeatedly.
				
				// We "panic" if we have tried TWICE to copy every slot - and it still
				// has not happened. i.e., twice some thread somewhere claimed they
				// would copy 'slot X' (by bumping _copyIdx) but they never claimed to
				// have finished (by bumping _copyDone). Our choices become limited:
				// we can wait for the work-claimers to finish (and become a blocking
				// algorithm) or do the copy work ourselves. Tiny tables with huge
				// thread counts trying to copy the table often 'panic'.
				if(panic_start == ~0U) { // No panic?
					copyidx = _copyIdx.load();
					while(copyidx < (oldlen << 1U) && // 'panic' check
					      !_copyIdx.compare_exchange_strong(copyidx, copyidx + MIN_COPY_WORK)) {
						copyidx = _copyIdx.load(); // Re-read
					}
					if(!(copyidx < (oldlen << 1))) { // Panic!
						panic_start = copyidx; // Record where we started to panic-copy
					}
				}
				
				// We now know what to copy. Try to copy.
				size_t workdone = 0;
				for(size_t i = 0; i < MIN_COPY_WORK; ++i) {
					if(copy_slot(topmap, (copyidx + i) & (oldlen - 1), oldkvs, newkvs)) { // Made an oldtable slot go dead?
						++workdone; // Yes!
					}
				}
				if(workdone > 0) { // Report work-done occasionally
					copy_check_and_promote(topmap, oldkvs, workdone);// See if we can promote
				}
				
				copyidx += MIN_COPY_WORK;
				// Uncomment these next 2 lines to turn on incremental table-copy.
				// Otherwise this thread continues to copy until it is all done.
				if(!copy_all && panic_start == ~0) { // No panic?
					return; // Then done copying after doing MIN_COPY_WORK
				}
			}
	
			// Extra promotion check, in case another thread finished all copying
			// then got stalled before promoting.
			copy_check_and_promote(topmap, oldkvs, 0);// See if we can promote
		}

		// Copy slot 'idx' from the old table to the new table. If this thread
		// confirmed the copy, update the counters and check for promotion.
		//
		// Returns the result of reading the volatile _newkvs, mostly as a
		// convenience to callers. We come here with 1-shot copy requests
		// typically because the caller has found a Prime, and has not yet read
		// the _newkvs volatile - which must have changed from null-to-not-null
		// before any Prime appears. So the caller needs to read the _newkvs
		// field to retry his operation in the new table, but probably has not
		// read it yet.
		smr::stable_pointer<kv_array_type> copy_slot_and_check(map_type* topmap, const smr::stable_pointer<kv_array_type> oldkvs, size_t idx, const key_type* const should_help) {
			assert(chm(oldkvs) == this);
			smr::stable_pointer<kv_array_type> newkvs(_newkvs);
			// We're only here because the caller saw a Prime, which implies a
			// table-copy is in progress.
			assert(newkvs != nullptr);
			if(copy_slot(topmap, idx, oldkvs, newkvs)) { // Copy the desired slot
				copy_check_and_promote(topmap, oldkvs, 1); // Record the slot copied
			}
			// Generically help along any copy (except if called recursively from a helper)
			return (should_help == nullptr) ? newkvs : topmap->help_copy(newkvs);
		}
	
		void copy_check_and_promote(map_type* topmap, const smr::stable_pointer<kv_array_type> oldkvs, size_t workdone) {
			assert(chm(oldkvs) == this);
			size_t oldlen = len(oldkvs);
			// We made a slot unusable and so did some of the needed copy work
			size_t copyDone = _copyDone.load();
			assert((copyDone + workdone) <= oldlen);
			if(workdone > 0) {
				while(!_copyDone.compare_exchange_strong(copyDone, copyDone + workdone)) {
					copyDone = _copyDone.load(); // Reload, retry
					assert((copyDone + workdone) <= oldlen);
				}
			}
			// Check for copy being ALL done, and promote. Note that we might have
			// nested in-progress copies and manage to finish a nested copy before
			// finishing the top-level copy. We only promote top-level copies.
			smr::stable_pointer<kv_array_type> newkvs(_newkvs);
			smr::stable_pointer<kv_array_type> topkvs(topmap->_kvs);
			if(copyDone + workdone == oldlen && // Ready to promote this table?
			   topkvs == oldkvs && // Looking at the top-level table?
			   // Attempt to promote
			   topmap->CAS_kvs(oldkvs, newkvs)) {
				smr::smr_destroy(oldkvs, &finalize_kvs, reinterpret_cast<void*>(true));
				topmap->_last_resize_milli = std::clock(); // Record resize time for next check
			}
		}

		// Copy one K/V pair from oldkvs[i] to newkvs. Returns true if we can
		// confirm that the new table guaranteed has a value for this old-table
		// slot. We need an accurate confirmed-copy count so that we know when we
		// can promote (if we promote the new table too soon, other threads may
		// 'miss' on values not-yet-copied from the old table). We don't allow
		// any direct updates on the new table, unless they first happened to the
		// old table - so that any transition in the new table from null to
		// not-null must have been from a copy_slot (or other old-table overwrite)
		// and not from a thread directly writing in the new table. Thus we can
		// count null-to-not-null transitions in the new table.
		bool copy_slot(map_type* topmap, size_t idx, const smr::stable_pointer<kv_array_type> oldkvs, smr::stable_pointer<kv_array_type> newkvs) {
			// Blindly set the key slot from null to TOMBSTONE, to eagerly stop
			// fresh put's from inserting new values in the old table when the old
			// table is mid-resize. We don't need to act on the results here,
			// because our correctness stems from box'ing the Value field. Slamming
			// the Key field is a minor speed optimization.
			smr::stable_pointer<const key_type> key;
			while((key = map_type::key(oldkvs, idx)) == nullptr) {
				map_type::CAS_key(oldkvs, idx, nullptr, map_type::TOMBSTONEK());
			}
			
			// Prevent new values from appearing in the old table.
			// Box what we see in the old table, to prevent further updates.
			smr::stable_pointer<value_type> oldval = map_type::val(oldkvs, idx); // Read OLD table
			while(!map_type::is_prime(oldval.get_pointer())) {
				smr::stable_pointer<value_type> box = (oldval == nullptr || oldval == map_type::TOMBSTONE()) ? smr::make_unshared_stable_pointer(map_type::TOMBPRIME())
				                                                                                             : smr::make_unshared_stable_pointer(map_type::prime(oldval.get_pointer()));
				if(CAS_val(oldkvs, idx, oldval.get_pointer(), box.get_pointer()) ) { // CAS down a box'd version of oldval
					// If we made the Value slot hold a TOMBPRIME, then we both
					// prevented further updates here but also the (absent)
					// oldval is vaccuously available in the new table. We
					// return with true here: any thread looking for a value for
					// this key can correctly go straight to the new table and
					// skip looking in the old table.
					if(box == map_type::TOMBPRIME()) {
						if(oldval == map_type::TOMBSTONE()) {
							// since the value is deleted, we bail early
							// this is our opportunity to remove an unused key
							// so let's prime the key, so we can pick it out later
							smr::stable_pointer<const key_type> prime_key = smr::make_unshared_stable_pointer<const key_type>(map_type::prime(key.get_pointer()));
							map_type::CAS_key(oldkvs, idx, key.get_pointer(), prime_key.get_pointer());
						}
						return true;
					}
					// Otherwise we boxed something, but it still needs to be
					// copied into the new table.
					oldval = box; // Record updated oldval
					break; // Break loop; oldval is now boxed by us
				}
				oldval = val(oldkvs, idx); // Else try, try again
			}
			if(oldval == map_type::TOMBPRIME()) {
				return false; // Copy already complete here!
			}

			// Copy the value into the new table, but only if we overwrite a null.
			// If another value is already in the new table, then somebody else
			// wrote something there and that write is happens-after any value that
			// appears in the old table. If putIfMatch does not find a null in the
			// new table - somebody else should have recorded the null-not_null
			// transition in this copy.
			smr::stable_pointer<value_type> old_unboxed = smr::make_unshared_stable_pointer(map_type::unprime(oldval.get_pointer()));
			assert(old_unboxed.get_pointer() != TOMBSTONE());
			bool copied_into_new = (map_type::putIfMatch(topmap, newkvs, key.get_pointer(), old_unboxed.get_pointer(), nullptr) == nullptr);
			
			// ---
			// Finally, now that any old value is exposed in the new table, we can
			// forever hide the old-table value by slapping a TOMBPRIME down. This
			// will stop other threads from uselessly attempting to copy this slot
			// (i.e., it's a speed optimization not a correctness issue).
			while(!CAS_val(oldkvs, idx, oldval.get_pointer(), map_type::TOMBPRIME())) {
				oldval = map_type::val(oldkvs, idx);
			}
			return copied_into_new;
		}
	};

	// Adding a 'prime' bit onto Values
	template<typename T>
	static T* prime(T* t) {
		return reinterpret_cast<T*>(reinterpret_cast<size_t>(t) | 1);
	}

	template<typename T>
	static T* unprime(T* t) {
		return reinterpret_cast<T*>(reinterpret_cast<size_t>(t) & ~1);
	}

	template<typename T>
	static bool is_prime(T* t) {
		return (reinterpret_cast<size_t>(t) & 1) == 1;
	}

	// Helper function to spread lousy hashCodes
	static size_t hash(const key_type& key) {
		hash_type hasher;
		size_t h = hasher(key); // The real hashCode call
		// Spread bits to regularize both segment and index locations,
		// using variant of single-word Wang/Jenkins hash.
		h += (h << 15) ^ 0xffffcd7d;
		h ^= (h >> 10);
		h += (h << 3);
		h ^= (h >> 6);
		h += (h << 2) + (h << 14);
		return h ^ (h >> 16);
	}
	
	// Slot 0 is always used for a 'CHM' entry below to hold the interesting
	// bits of the hash table. Slot 1 holds full hashes as an array of ints.
	// Slots {2,3}, {4,5}, etc hold {Key,Value} pairs. The entire hash table
	// can be atomically replaced by CASing the _kvs field.
	//
	// Why is CHM buried inside the _kvs Object array, instead of the other way
	// around? The CHM info is used during resize events and updates, but not
	// during standard 'get' operations. I assume 'get' is much more frequent
	// than 'put'. 'get' can skip the extra indirection of skipping through the
	// CHM to reach the _kvs array.
	std::atomic<kv_array_type*> _kvs;
	static __forceinline CHM*             chm   (const smr::stable_pointer<kv_array_type>& kvs) { return static_cast<CHM*            >(kvs->values[0].load()); }
	static __forceinline hash_array_type* hashes(const smr::stable_pointer<kv_array_type>& kvs) { return static_cast<hash_array_type*>(kvs->values[1].load()); }
	// Number of K,V pairs in the table
	static __forceinline size_t           len   (const smr::stable_pointer<kv_array_type>& kvs) { return (kvs->length - 2) >> 1; }
	bool CAS_kvs(const smr::stable_pointer<kv_array_type>& oldkvs, const smr::stable_pointer<kv_array_type>& newkvs) {
		kv_array_type* old = oldkvs.get_pointer();
		return _kvs.compare_exchange_strong(old, newkvs.get_pointer());
	}

	counter_t* const _size;

	static const size_t REPROBE_LIMIT = 10; // Too many reprobes then force a table-resize

	// Time since last resize
	std::clock_t _last_resize_milli;
	
	static const size_t MIN_SIZE_LOG = 3;
	static const size_t MIN_SIZE = (1 << MIN_SIZE_LOG);

	// No-Match-Old - putIfMatch does updates only if it matches the old value,
	// and NO_MATCH_OLD basically counts as a wildcard match.
	static value_type* NO_MATCH_OLD() { return reinterpret_cast<value_type*>(0x2); }
	// Match-Any-not-null - putIfMatch does updates only if it find a real old
	// value.
	static value_type* MATCH_ANY   () { return reinterpret_cast<value_type*>(0x4); }
	// This K/V pair has been deleted (but the Key slot is forever claimed).
	// The same Key can be reinserted with a new value later.
	static value_type* TOMBSTONE   () { return reinterpret_cast<value_type*>(0x8); }
	static key_type  * TOMBSTONEK  () { return reinterpret_cast<key_type  *>(0x8); }
	// Prime'd or box'd version of TOMBSTONE. This K/V pair was deleted, then a
	// table resize started. The K/V pair has been marked so that no new
	// updates can happen to the old table (and since the K/V pair was deleted
	// nothing was copied to the new table).
	static value_type* TOMBPRIME   () { return prime(TOMBSTONE()); }

	// Access K,V for a given idx
	//
	// Note that these are static, so that the caller is forced to read the _kvs
	// field only once, and share that read across all key/val calls - lest the
	// _kvs field move out from under us and back-to-back key & val calls refer
	// to different _kvs arrays.
	static smr::stable_pointer<const key_type> key(const smr::stable_pointer<kv_array_type>& kvs, size_t idx) {
		return smr::stable_pointer<const key_type>(kvs->values[(idx << 1) + 2]);
	}
	static smr::stable_pointer<    value_type> val(const smr::stable_pointer<kv_array_type>& kvs, size_t idx) {
		return smr::stable_pointer<    value_type>(kvs->values[(idx << 1) + 3]);
	}
	static bool CAS_key(const smr::stable_pointer<kv_array_type>& kvs, size_t idx, const key_type* old, const key_type* key) {
		void* old_k = const_cast<key_type*>(old);
		return kvs->values[(idx << 1) + 2].compare_exchange_strong(old_k, const_cast<key_type*>(key));
//		return smr::util::cas(reinterpret_cast<key_type**>(&kvs->values[(idx << 1) + 2]), const_cast<key_type*>(old), const_cast<key_type*>(key));
	}
	static bool CAS_val(const smr::stable_pointer<kv_array_type>& kvs, size_t idx, value_type* old, value_type* val) {
		void* old_v = old;
		return kvs->values[(idx << 1) + 3].compare_exchange_strong(old_v, val);
		//return smr::util::cas(reinterpret_cast<value_type**>(&kvs->values[(idx << 1) + 3]), old, val);
	}

	std::atomic<counter_t*> _reprobes;
public:
	// Get and clear the current count of reprobes. Reprobes happen on key
	// collisions, and a high reprobe rate may indicate a poor hash function or
	// weaknesses in the table resizing function.
	// @return the count of reprobes since the last call to {@link #reprobes}
	// or since the table was created.
	size_t reprobes() {
		smr::stable_pointer<counter_t> rep(_reprobes);
		size_t r = rep->get();

		counter_t* next = new (smr::smr) counter_t();
		if(_reprobes.compare_exchange_strong(rep.get_pointer(), next)) {
			smr::smr_destroy(rep.get_pointer());
		} else {
			smr::smr_destroy(next);
		}

		return r;
	}

private:
	static size_t reprobe_limit(size_t len) {
		return REPROBE_LIMIT + (len >> 2);
	}
};

#endif
