// LocklessTest.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"

#include "stacktrace.hpp"

#ifdef _DEBUG
#define SET_CRT_DEBUG_FIELD(a) _CrtSetDbgFlag((a) | _CrtSetDbgFlag(_CRTDBG_REPORT_FLAG))
#define CLEAR_CRT_DEBUG_FIELD(a) _CrtSetDbgFlag(~(a) & _CrtSetDbgFlag(_CRTDBG_REPORT_FLAG))
#define USES_MEMORY_CHECK	\
_CrtMemState before = {0}, after = {0}, difference = {0};\
_CrtSetReportMode(_CRT_WARN, _CRTDBG_MODE_FILE);\
_CrtSetReportFile(_CRT_WARN, _CRTDBG_FILE_STDOUT);\
_CrtSetReportMode(_CRT_ERROR, _CRTDBG_MODE_FILE);\
_CrtSetReportFile(_CRT_ERROR, _CRTDBG_FILE_STDOUT);\
_CrtSetReportMode(_CRT_ASSERT, _CRTDBG_MODE_FILE);\
_CrtSetReportFile(_CRT_ASSERT, _CRTDBG_FILE_STDOUT);\
SET_CRT_DEBUG_FIELD(_CRTDBG_LEAK_CHECK_DF);

//SET_CRT_DEBUG_FIELD(_CRTDBG_DELAY_FREE_MEM_DF);\
//SET_CRT_DEBUG_FIELD(_CRTDBG_CHECK_EVERY_16_DF)

#define MEM_CHK_BEFORE	\
_CrtMemCheckpoint(&before)

#define MEM_CHK_AFTER	\
_CrtMemCheckpoint(&after);\
do\
{\
	if(TRUE == _CrtMemDifference(&difference, &before, &after))\
	{\
		_CrtMemDumpStatistics(&difference);\
	}\
}\
while(0)

#else // _DEBUG
#define SET_CRT_DEBUG_FIELD(a) ((void) 0)
#define CLEAR_CRT_DEBUG_FIELD(a) ((void) 0)
#define USES_MEMORY_CHECK
#define MEM_CHK_BEFORE
#define MEM_CHK_AFTER
#endif // _DEBUG

#include "concurrent_auto_table.hpp"
#include "non_blocking_unordered_map.hpp"

typedef concurrent_auto_table<unsigned __int64> counter_t;

struct thread_info {
	size_t processor_id;
	HANDLE begin;
	counter_t* counters;
};

static const __declspec(align(64)) unsigned __int64 target = 1000 * 1000 * 1;

DWORD WINAPI thread_proc(void* data) {
	thread_info* ti = static_cast<thread_info*>(data);
	::SetThreadAffinityMask(::GetCurrentThread(), 1 << ti->processor_id);
	::WaitForSingleObject(ti->begin, INFINITE);
	for(size_t i(0); i < target; ++i) {
		ti->counters->increment();
	}
	return 0;
}

static unsigned __int64 interlocked_counter = 0;

DWORD WINAPI naive_thread_proc(void* data) {
	thread_info* ti = static_cast<thread_info*>(data);
	::SetThreadAffinityMask(::GetCurrentThread(), 1 << ti->processor_id);
	::WaitForSingleObject(ti->begin, INFINITE);
	for(size_t i(0); i < target; ++i) {
		::InterlockedIncrement(&interlocked_counter);
	}
	return 0;
}

DWORD WINAPI test_thread(void*)
{
	for(int i = 0; i < 64; ++i)
	{
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
	
		m->put("k-00", "v-00");
		m->put("k-01", "v-01");
		m->put("k-02", "v-02");
		m->put("k-03", "v-03");
		m->put("k-04", "v-04");
		m->put("k-05", "v-05");
		m->put("k-06", "v-06");
		m->put("k-07", "v-07");
		m->put("k-08", "v-08");
		m->put("k-09", "v-09");
		m->put("k-0a", "v-0a");
		m->put("k-0b", "v-0b");
		m->put("k-0c", "v-0c");
		m->put("k-0d", "v-0d");
		m->put("k-0e", "v-0e");
		m->put("k-0f", "v-0f");
		m->put("k-10", "v-10");
		m->put("k-11", "v-11");
		m->put("k-12", "v-12");
		m->put("k-13", "v-13");
		m->put("k-14", "v-14");
		m->put("k-15", "v-15");
		m->put("k-16", "v-16");
		m->put("k-17", "v-17");
		m->put("k-18", "v-18");
		m->put("k-19", "v-19");
		m->put("k-1a", "v-1a");
		m->put("k-1b", "v-1b");
		m->put("k-1c", "v-1c");
		m->put("k-1d", "v-1d");
		m->put("k-1e", "v-1e");
		m->put("k-1f", "v-1f");
		m->put("k-20", "v-20");
		m->put("k-21", "v-21");
		m->put("k-22", "v-22");
		m->put("k-23", "v-23");
		m->put("k-24", "v-24");
		m->put("k-25", "v-25");
		m->put("k-26", "v-26");
		m->put("k-27", "v-27");
		m->put("k-28", "v-28");
		m->put("k-29", "v-29");
		m->put("k-2a", "v-2a");
		m->put("k-2b", "v-2b");
		m->put("k-2c", "v-2c");
		m->put("k-2d", "v-2d");
		m->put("k-2e", "v-2e");
		m->put("k-2f", "v-2f");
		m->put("k-30", "v-30");
		m->put("k-31", "v-31");
		m->put("k-32", "v-32");
		m->put("k-33", "v-33");
		m->put("k-34", "v-34");
		m->put("k-35", "v-35");
		m->put("k-36", "v-36");
		m->put("k-37", "v-37");
		m->put("k-38", "v-38");
		m->put("k-39", "v-39");
		m->put("k-3a", "v-3a");
		m->put("k-3b", "v-3b");
		m->put("k-3c", "v-3c");
		m->put("k-3d", "v-3d");
		m->put("k-3e", "v-3e");
		m->put("k-3f", "v-3f");

		std::cout << m->size() << std::endl;
		if(i == 32)
		{
			//DebugBreak();
		}
		smr::smr_destroy(m);
		smr::detail::smr_clean();
	}
	return 0;
}

int main(int, char*[])
{
	::SymSetOptions(SYMOPT_DEFERRED_LOADS | SYMOPT_CASE_INSENSITIVE);
	::SymInitializeW(::GetCurrentProcess(), nullptr, TRUE);

	USES_MEMORY_CHECK;
	MEM_CHK_BEFORE;
	//_CrtSetBreakAlloc(179);
	HANDLE test = CreateThread(nullptr, 0, &test_thread, nullptr, 0, nullptr);
	WaitForSingleObject(test, INFINITE);
	MEM_CHK_AFTER;

	_CrtDumpMemoryLeaks();

	char ch;
	std::cin >> ch;

	return 0;
	for(;;)
	{
		interlocked_counter = 0;
		::SetPriorityClass(::GetCurrentProcess(), BELOW_NORMAL_PRIORITY_CLASS);

		LARGE_INTEGER frequency = { 0 };
		LARGE_INTEGER start = { 0 }, end = { 0 };
		::QueryPerformanceFrequency(&frequency);

		counter_t* counters = new (smr::smr) counter_t();
		HANDLE begin = ::CreateEventW(nullptr, TRUE, FALSE, nullptr);

		::SYSTEM_INFO si = { 0 };
		::GetSystemInfo(&si);

		const int load_multiplier = 4;
		const int thread_count = load_multiplier * si.dwNumberOfProcessors;
	
		std::vector<HANDLE> threads(thread_count);
		std::vector<thread_info> infos(thread_count);
		for(int i(0); i < thread_count; ++i)
		{
			infos[i].processor_id = i;
			infos[i].begin = begin;
			infos[i].counters = counters;

			threads[i] = ::CreateThread(nullptr, 0, &thread_proc, &infos[i], 0, nullptr);
		}
		::QueryPerformanceCounter(&start);
		::SetEvent(begin);
		::WaitForMultipleObjects(thread_count, &threads[0], TRUE, INFINITE);
		::QueryPerformanceCounter(&end);
		for(int i(0); i < thread_count; ++i)
		{
			::CloseHandle(threads[i]);
		}
		std::cout << "expected count: " << static_cast<unsigned __int64>(thread_count) * target << " actual count: " << counters->get() << " time: " << static_cast<double>(end.QuadPart - start.QuadPart) / frequency.QuadPart << std::endl;
		::ResetEvent(begin);
		for(int i(0); i < thread_count; ++i)
		{
			infos[i].processor_id = i;
			infos[i].begin = begin;
			infos[i].counters = counters;

			threads[i] = ::CreateThread(nullptr, 0, &naive_thread_proc, &infos[i], 0, nullptr);
		}
		::QueryPerformanceCounter(&start);
		::SetEvent(begin);
		::WaitForMultipleObjects(thread_count, &threads[0], TRUE, INFINITE);
		::QueryPerformanceCounter(&end);
		for(int i(0); i < thread_count; ++i)
		{
			::CloseHandle(threads[i]);
		}
		std::cout << "expected count: " << static_cast<unsigned __int64>(thread_count) * target << " actual count: " << interlocked_counter << " time: " << static_cast<double>(end.QuadPart - start.QuadPart) / frequency.QuadPart << std::endl;

		::CloseHandle(begin);
		smr::smr_destroy(counters);
	}
	return 0;
}
