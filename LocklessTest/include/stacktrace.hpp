#pragma once

#include <sdkddkver.h>

#define NOMINMAX
#define STRICT

#include <windows.h>

#define DBGHELP_TRANSLATE_TCHAR
#include <dbghelp.h>
#pragma comment(lib, "dbghelp.lib")

#include <vector>

extern "C"
{
	void* _ReturnAddress();
	void* _AddressOfReturnAddress();
}

#pragma intrinsic(_ReturnAddress)
#pragma intrinsic(_AddressOfReturnAddress)

namespace tchar
{

#ifdef SYMBOL_INFO
#undef SYMBOL_INFO
#endif

	template<typename T>
	struct SYMBOL_INFO;

	template<>
	struct SYMBOL_INFO<wchar_t> : ::SYMBOL_INFOW
	{
	};

	template<>
	struct SYMBOL_INFO<char> : ::SYMBOL_INFO
	{
	};

#ifdef SymFromAddr
#undef SymFromAddr
#endif

	inline BOOL SymFromAddr(__in HANDLE hProcess, __in DWORD64 Address, __out_opt PDWORD64 Displacement, __inout SYMBOL_INFO<wchar_t>* Symbol)
	{
		return ::SymFromAddrW(hProcess, Address, Displacement, Symbol);
	}

	inline BOOL SymFromAddr(__in HANDLE hProcess, __in DWORD64 Address, __out_opt PDWORD64 Displacement, __inout SYMBOL_INFO<char>* Symbol)
	{
		return ::SymFromAddr(hProcess, Address, Displacement, Symbol);
	}

#ifdef UnDecorateSymbolName
#undef UnDecorateSymbolName
#endif

	inline DWORD WINAPI UnDecorateSymbolName(__in PCWSTR name, __out_ecount(maxStringLength) PWSTR outputString, __in DWORD maxStringLength, __in DWORD flags)
	{
		return ::UnDecorateSymbolNameW(name, outputString, maxStringLength, flags);
	}

	inline DWORD WINAPI UnDecorateSymbolName(__in PCSTR name, __out_ecount(maxStringLength) PSTR outputString, __in DWORD maxStringLength, __in DWORD flags)
	{
		return ::UnDecorateSymbolName(name, outputString, maxStringLength, flags);
	}

#ifdef IMAGEHLP_LINE64
#undef IMAGEHLP_LINE64
#endif

	template<typename T>
	struct IMAGEHLP_LINE64;

	template<>
	struct IMAGEHLP_LINE64<wchar_t> : ::IMAGEHLP_LINEW64
	{
	};

	template<>
	struct IMAGEHLP_LINE64<CHAR> : ::IMAGEHLP_LINE64
	{
	};

#ifdef SymGetLineFromAddr64
#undef SymGetLineFromAddr64
#endif

	inline BOOL SymGetLineFromAddr64(IN HANDLE hProcess, IN DWORD64 qwAddr, OUT PDWORD pdwDisplacement, OUT IMAGEHLP_LINE64<wchar_t>* Line64)
	{
		return ::SymGetLineFromAddrW64(hProcess, qwAddr, pdwDisplacement, Line64);
	}

	inline BOOL SymGetLineFromAddr64(IN HANDLE hProcess, IN DWORD64 qwAddr, OUT PDWORD pdwDisplacement, OUT IMAGEHLP_LINE64<char>* Line64)
	{
		return ::SymGetLineFromAddr64(hProcess, qwAddr, pdwDisplacement, Line64);
	}

#ifdef GetModuleFileName
#undef GetModuleFileName
#endif

	inline DWORD WINAPI GetModuleFileName(__in_opt HMODULE hModule, __out_ecount_part(nSize, return + 1) LPWCH lpFilename, __in DWORD nSize)
	{
		return ::GetModuleFileNameW(hModule, lpFilename, nSize);
	}

	inline DWORD WINAPI GetModuleFileName(__in_opt HMODULE hModule, __out_ecount_part(nSize, return + 1) LPCH lpFilename, __in DWORD nSize)
	{
		return ::GetModuleFileNameA(hModule, lpFilename, nSize);
	}

}

template<typename T>
struct SymbolInfo
{
	SymbolInfo() : displacement(0), symbolInfo(0) {}
	SymbolInfo(HANDLE process, void* programCounter) : displacement(0)
	{
		buffer.resize(sizeof(tchar::SYMBOL_INFO<T>) + (MAX_SYM_NAME * sizeof(TCHAR)));
		symbolInfo = reinterpret_cast<tchar::SYMBOL_INFO<T>*>(&(buffer[0]));
		symbolInfo->SizeOfStruct = sizeof(tchar::SYMBOL_INFO<T>);
		symbolInfo->MaxNameLen = MAX_SYM_NAME;
		if(TRUE != tchar::SymFromAddr(process, reinterpret_cast<DWORD64>(programCounter), &displacement, symbolInfo))
		{
			throw new std::runtime_error("something went wrong");
		}
	}
	SymbolInfo(const SymbolInfo<T>& rhs) : buffer(rhs.buffer), displacement(rhs.displacement), symbolInfo(reinterpret_cast<SYMBOL_INFO*>(&(buffer[0])))
	{
	}
	const SymbolInfo<T>& operator=(const SymbolInfo<T>& rhs)
	{
		SymbolInfo<T> tmp(rhs);
		swap(tmp);
		return *this;
	}
	void swap(SymbolInfo<T>& rhs) throw()
	{
		buffer.swap(rhs.buffer);
		std::swap(symbolInfo, rhs.symbolInfo);
		std::swap(displacement, rhs.displacement);
	}
	ULONG TypeIndex() const { return symbolInfo->TypeIndex; }
	ULONG Index() const { return symbolInfo->Index; }
	ULONG Size() const { return symbolInfo->Size; }
	ULONG64 ModBase() const { return symbolInfo->ModBase; }
	ULONG Flags() const { return symbolInfo->Flags; }
	ULONG64 Value() const { return symbolInfo->Value; }
	ULONG64 Address() const { return symbolInfo->Address; }
	ULONG Register() const { return symbolInfo->Register; }
	ULONG Scope() const { return symbolInfo->Scope; }
	ULONG Tag() const { return symbolInfo->Tag; }
	ULONG NameLen() const { return symbolInfo->NameLen; }
	std::basic_string<T> Name() const { return symbolInfo->Name; }
	std::basic_string<T> UndecoratedName() const
	{
		T undecoratedName[MAX_SYM_NAME] = {0};
		::UnDecorateSymbolName(symbolInfo->Name, undecoratedName, MAX_SYM_NAME, UNDNAME_COMPLETE);
		return undecoratedName;
	}
	DWORD64 Displacement() const { return displacement; }
private:
	std::vector<unsigned char> buffer;
	DWORD64 displacement;
	tchar::SYMBOL_INFO<T>* symbolInfo;
};

template<typename T>
struct ImageLine
{
	ImageLine() : displacement(0), lineInfo() {}
	ImageLine(HANDLE process, void* programCounter) : displacement(0)
	{
		std::memset(&lineInfo, 0, sizeof(lineInfo));
		lineInfo.SizeOfStruct = sizeof(tchar::IMAGEHLP_LINE64<T>);
		if(TRUE != ::SymGetLineFromAddr64(process, reinterpret_cast<DWORD64>(programCounter), &displacement, &lineInfo))
		{
			if(ERROR_INVALID_ADDRESS == GetLastError())
			{
				
			}
			else
			{
				throw new std::runtime_error("something went wrong");
			}
		}
	}
	ImageLine(const ImageLine<T>& rhs) : displacement(rhs.displacement), lineInfo(rhs.lineInfo)
	{
	}
	const ImageLine<T>& operator=(const ImageLine<T>& rhs)
	{
		ImageLine tmp(rhs);
		swap(tmp);
		return *this;
	}
	void swap(ImageLine<T>& rhs) throw()
	{
		std::swap(displacement, rhs.displacement);
		std::swap(lineInfo, rhs.lineInfo);
	}
	void* Key() const { return lineInfo.Key; }
	DWORD LineNumber() const { return lineInfo.LineNumber; }
	std::basic_string<T> FileName() const
	{
		const static T unknown[] = { '<', 'u', 'n', 'k', 'n', 'o', 'w', 'n', '>', '\0' };
		return lineInfo.FileName ? lineInfo.FileName : std::basic_string<T>(unknown);
	}
	DWORD64 Address() const { return lineInfo.Address; }
private:
	DWORD displacement;
	tchar::IMAGEHLP_LINE64<T> lineInfo;
};

template<typename T>
inline std::basic_string<T> module_name(void* address)
{
	MEMORY_BASIC_INFORMATION mbi = {0};
	::VirtualQuery(address, &mbi, sizeof(MEMORY_BASIC_INFORMATION));
	T buffer[1 << 15];
	tchar::GetModuleFileName(reinterpret_cast<HMODULE>(mbi.AllocationBase), buffer, sizeof(buffer) / sizeof(T));
	return std::basic_string<T>(buffer);
}

template<typename T>
void print_caller_info(std::basic_ostream<T>& os, void* address, void* address_of_return_address)
{
	CONTEXT context = {0};
	context.ContextFlags = CONTEXT_CONTROL;
#if defined(_M_IX86)
	RtlCaptureContext(&context);
	//context.Eip = reinterpret_cast<DWORD32>(address);
	//context.Ebp = reinterpret_cast<DWORD32>(address_of_return_address) + sizeof(void*);
	//// I know this is wrong.  The stack pointer is somewhere miles away.  I probably ought to do something about that.
	//context.Esp = reinterpret_cast<DWORD32>(&context) - (sizeof(CONTEXT) + sizeof(STACKFRAME64) + sizeof(DWORD) + sizeof(SymbolInfo<T>) + sizeof(SymbolInfo<T>) + sizeof(ImageLine<T>));
#elif defined(_M_X64)
	RtlCaptureContext(&context);
	//context.Rip = reinterpret_cast<DWORD64>(address);
	//context.Rbp = reinterpret_cast<DWORD64>(address_of_return_address) + sizeof(void*);
	//// This isn't really where the stack pointer, but it should be close enough.  Of course, the stack itself doesn't necessarily exist now.
	//context.Rsp = context.Rbp;
#else
#error Unsupported architecture
#endif
	STACKFRAME64 stackFrame = {0};
	stackFrame.AddrPC.Offset = reinterpret_cast<DWORD64>(static_cast<void (__cdecl *)(std::basic_ostream<T,std::char_traits<T>> &,void *,void *)>(&print_caller_info<T>));
	stackFrame.AddrPC.Mode = AddrModeFlat;
	stackFrame.AddrFrame.Offset = reinterpret_cast<DWORD64>(address_of_return_address) + sizeof(void*);
	stackFrame.AddrFrame.Mode = AddrModeFlat;
	stackFrame.AddrStack.Offset = reinterpret_cast<DWORD64>(&context);
	stackFrame.AddrStack.Mode = AddrModeFlat;
#if defined(_M_IX86)
	DWORD machine_type(IMAGE_FILE_MACHINE_I386);
#elif defined(_M_X64)
	DWORD machine_type(IMAGE_FILE_MACHINE_AMD64);
#else
#error Unsupported architecture
#endif
	size_t depth = 0;
	while(FALSE != ::StackWalk64(machine_type, ::GetCurrentProcess(), ::GetCurrentThread(), &stackFrame, &context, 0, &SymFunctionTableAccess64, &SymGetModuleBase64, 0)
	&& stackFrame.AddrPC.Offset != 0)
	{
		for(size_t i = 0; i < depth; ++i)
		{
			os << ' ';
		}
		// the public symbol is the full decorated name, which I can undecorate and make pretty to properly identify the function
		// the global (or whatever) symbol is not the full decorated name but rather a dumbed down version of it.
		// _I_ want to have public if possible, else global.  Unfortunately, I can't do that in a single call; I can have global if possible else public
		// and I can have public only.  So I'll get both symbols and see which is better.
		DWORD symbolOptions(::SymGetOptions());
		::SymSetOptions(symbolOptions | SYMOPT_PUBLICS_ONLY);
		SymbolInfo<T> publicSymbols(::GetCurrentProcess(), reinterpret_cast<void*>(stackFrame.AddrPC.Offset));
		::SymSetOptions(symbolOptions);
		SymbolInfo<T> globalSymbols(::GetCurrentProcess(), reinterpret_cast<void*>(stackFrame.AddrPC.Offset));
		ImageLine<T> lineInfo(::GetCurrentProcess(), reinterpret_cast<void*>(stackFrame.AddrPC.Offset));
		os << module_name<T>(reinterpret_cast<void*>(publicSymbols.ModBase())) << T('!') << lineInfo.FileName() << T('(') << std::dec << lineInfo.LineNumber() << T(')') << T('!');
		os << (publicSymbols.Name()[0] == L'?' ? publicSymbols.UndecoratedName() : globalSymbols.Name()) << T('@') << std::hex << publicSymbols.Displacement() << std::dec << std::endl;
		++depth;
	}
}
