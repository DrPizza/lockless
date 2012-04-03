// LocklessTest.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"

#include "concurrent_auto_table.hpp"
#include "non_blocking_unordered_map.hpp"

typedef concurrent_auto_table<unsigned __int64> counter_t;

struct thread_info {
	size_t processor_id;
	HANDLE begin;
	counter_t* counters;
};

static const unsigned __int64 target = 1000 * 1000 * 50;

DWORD WINAPI thread_proc(void* data) {
	thread_info* ti = static_cast<thread_info*>(data);
	::SetThreadAffinityMask(::GetCurrentThread(), 1 << ti->processor_id);
	::WaitForSingleObject(ti->begin, INFINITE);
	for(size_t i(0); i < target; ++i) {
		ti->counters->increment();
	}
	return 0;
}

static unsigned __int64 cnt = 0;

DWORD WINAPI naive_thread_proc(void* data) {
	thread_info* ti = static_cast<thread_info*>(data);
	::SetThreadAffinityMask(::GetCurrentThread(), 1 << ti->processor_id);
	::WaitForSingleObject(ti->begin, INFINITE);
	for(size_t i(0); i < target; ++i) {
		::InterlockedIncrement(&cnt);
	}
	return 0;
}

int main(int, char*[])
{
	//_CrtSetBreakAlloc(181L);

	typedef non_blocking_unordered_map<std::string, std::string> map_type;
	non_blocking_unordered_map<std::string, std::string>* m(new (smr::smr) map_type());
	boost::optional<std::string> r;
	m->putIfAbsent("foo", "bar");
	m->putIfAbsent("foo", "bar");

	m->put("foo", "bar");
	r = m->get("foo");
	std::cout << *r << std::endl;
	r = m->get("foo");
	std::cout << *r << std::endl;
	m->replace("foo", "bar", "baz");
	r = m->get("foo");
	std::cout << *r << std::endl;
	m->replace("foo", "baz", "qux");
	r = m->get("foo");
	std::cout << *r << std::endl;
	m->remove("foo");
	r = m->get("foo");
	std::cout << r << std::endl;
	m->put("foo", "bar");
	r = m->get("foo");
	std::cout << *r << std::endl;
	m->remove("foo", "bar");
	r = m->get("foo");
	std::cout << r << std::endl;

	m->put("0", "1");
	m->put("1", "1");
	m->put("2", "1");
	m->put("3", "1");
	m->put("4", "1");
	m->put("5", "1");
	m->put("6", "1");
	m->put("7", "1");
	m->put("8", "1");
	m->put("9", "1");
	m->put("a", "1");
	m->put("b", "1");
	m->put("c", "1");
	m->put("d", "1");
	m->put("e", "1");
	m->put("f", "1");
	m->put("10", "1");
	m->put("11", "1");
	m->put("12", "1");
	m->put("13", "1");
	m->put("14", "1");
	m->put("15", "1");
	m->put("16", "1");
	m->put("17", "1");
	m->put("18", "1");
	m->put("19", "1");
	m->put("1a", "1");
	m->put("1b", "1");
	m->put("1c", "1");
	m->put("1d", "1");
	m->put("1e", "1");
	m->put("1f", "1");
	m->put("20", "1");
	m->put("21", "1");
	m->put("22", "1");
	m->put("23", "1");
	m->put("24", "1");
	m->put("25", "1");
	m->put("26", "1");
	m->put("27", "1");
	m->put("28", "1");
	m->put("29", "1");
	m->put("2a", "1");
	m->put("2b", "1");
	m->put("2c", "1");
	m->put("2d", "1");
	m->put("2e", "1");
	m->put("2f", "1");
	m->put("30", "1");
	m->put("31", "1");
	m->put("32", "1");
	m->put("33", "1");
	m->put("34", "1");
	m->put("35", "1");
	m->put("36", "1");
	m->put("37", "1");
	m->put("38", "1");
	m->put("39", "1");
	m->put("3a", "1");
	m->put("3b", "1");
	m->put("3c", "1");
	m->put("3d", "1");
	m->put("3e", "1");
	m->put("3f", "1");

	std::cout << m->size() << std::endl;

	smr::smr_destroy(m);
	return 0;

	::SetPriorityClass(::GetCurrentProcess(), BELOW_NORMAL_PRIORITY_CLASS);

	LARGE_INTEGER frequency = {0};
	LARGE_INTEGER start = {0}, end = {0};
	::QueryPerformanceFrequency(&frequency);

	counter_t* counters = new (smr::smr) counter_t();
	HANDLE begin = ::CreateEventW(nullptr, TRUE, FALSE, nullptr);

	::SYSTEM_INFO si = {0};
	::GetSystemInfo(&si);
	
	std::vector<HANDLE> threads(si.dwNumberOfProcessors);
	std::vector<thread_info> infos(si.dwNumberOfProcessors);
	for(DWORD i(0); i < si.dwNumberOfProcessors; ++i) {
		infos[i].processor_id = i;
		infos[i].begin = begin;
		infos[i].counters = counters;

		threads[i] = ::CreateThread(nullptr, 0, &thread_proc, &infos[i], 0, nullptr);
	}
	::QueryPerformanceCounter(&start);
	::SetEvent(begin);
	::WaitForMultipleObjects(si.dwNumberOfProcessors, &threads[0], TRUE, INFINITE);
	::QueryPerformanceCounter(&end);
	for(DWORD i(0); i < si.dwNumberOfProcessors; ++i) {
		::CloseHandle(threads[i]);
	}
	std::cout << "expected count: " << static_cast<unsigned __int64>(si.dwNumberOfProcessors) * target << " actual count: " << counters->get() << " time: " << static_cast<double>(end.QuadPart - start.QuadPart) / frequency.QuadPart << std::endl;
	::ResetEvent(begin);
	for(DWORD i(0); i < si.dwNumberOfProcessors; ++i) {
		infos[i].processor_id = i;
		infos[i].begin = begin;
		infos[i].counters = counters;

		threads[i] = ::CreateThread(nullptr, 0, &naive_thread_proc, &infos[i], 0, nullptr);
	}
	::QueryPerformanceCounter(&start);
	::SetEvent(begin);
	::WaitForMultipleObjects(si.dwNumberOfProcessors, &threads[0], TRUE, INFINITE);
	::QueryPerformanceCounter(&end);
	for(DWORD i(0); i < si.dwNumberOfProcessors; ++i) {
		::CloseHandle(threads[i]);
	}
	std::cout << "expected count: " << static_cast<unsigned __int64>(si.dwNumberOfProcessors) * target << " actual count: " << cnt << " time: " << static_cast<double>(end.QuadPart - start.QuadPart) / frequency.QuadPart << std::endl;

	::CloseHandle(begin);
	smr::smr_destroy(counters);
	return 0;
}
