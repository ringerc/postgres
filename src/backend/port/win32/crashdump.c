/*-------------------------------------------------------------------------
 *
 * win32_crashdump.c
 *         Automatic crash dump creation for PostgreSQL on Windows
 *
 * The crashdump feature traps unhandled win32 exceptions produced by the
 * backend, and tries to produce a Windows MiniDump crash
 * dump for later debugging and analysis. The machine performing the dump
 * doesn't need any special debugging tools; the user only needs to send
 * the dump to somebody who has the same version of PostgreSQL and has debugging
 * tools.
 *
 * crashdump module originally by Craig Ringer <ringerc@ringerc.id.au>
 *
 * LIMITATIONS
 * ===========
 * This *won't* work in hard OOM situations or stack overflows.
 *
 * For those, it'd be necessary to take a much more complicated approach where
 * the handler switches to a new stack (if it can) and forks a helper process
 * to debug its self.
 *
 * POSSIBLE FUTURE WORK
 * ====================
 * For bonus points, the crash dump format permits embedding of user-supplied data.
 * If there's anything else (postgresql.conf? Last few lines of a log file?) that
 * should always be supplied with a crash dump, it could potentially be added,
 * though at the cost of a greater chance of the crash dump failing.
 *
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/port/win32/crashdump.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#define VC_EXTRALEAN
#include <windows.h>
#include <string.h>
#include <dbghelp.h>

/*
 * Much of the following code is based on CodeProject and MSDN examples,
 * particularly
 * http://www.codeproject.com/KB/debug/postmortemdebug_standalone1.aspx
 *
 * Useful MSDN articles:
 *
 * http://msdn.microsoft.com/en-us/library/ff805116(v=VS.85).aspx
 * http://msdn.microsoft.com/en-us/library/ms679294(VS.85).aspx
 *
 * Other useful articles on working with minidumps:
 * http://www.debuginfo.com/articles/effminidumps.html
 */

typedef LPAPI_VERSION (WINAPI *IMAGEHLPAPIVERSION)(void);
typedef BOOL (WINAPI *MINIDUMPWRITEDUMP)(HANDLE hProcess, DWORD dwPid, HANDLE hFile, MINIDUMP_TYPE DumpType,
									CONST PMINIDUMP_EXCEPTION_INFORMATION ExceptionParam,
									CONST PMINIDUMP_USER_STREAM_INFORMATION UserStreamParam,
									CONST PMINIDUMP_CALLBACK_INFORMATION CallbackParam
									);

/*
 * To perform a crash dump, we need to load dbghelp.dll and find the
 * MiniDumpWriteDump function. If possible, the copy of dbghelp.dll
 * shipped with PostgreSQL is loaded; failing that, the system copy will
 * be used.
 *
 * This function is called in crash handler context. It can't trust that much
 * of the backend is working.
 *
 * If NULL is returned, loading failed and the crash dump handler should
 * not try to continue.
 */
static HMODULE
loadDbgHelp(void)
{
	HMODULE hDll = NULL;
	char dbgHelpPath[_MAX_PATH];

	if (GetModuleFileName(NULL, dbgHelpPath, _MAX_PATH))
	{
		char *slash = strrchr(dbgHelpPath, '\\');
		if (slash)
		{
			strcpy(slash+1, "DBGHELP.DLL");
			hDll = LoadLibrary(dbgHelpPath);
		}
	}

	if (hDll==NULL)
	{
		/*
		 * Load whichever version of dbghelp.dll we can find.
		 *
		 * It is safe to call LoadLibrary with an unqualified name (thus
		 * searching the PATH) only because the postmaster ensures that
		 * backends run in a sensible environment with the datadir as
		 * the working directory. It's usually unsafe to
		 * LoadLibrary("unqualifiedname.dll")
		 * because the user can run your program with a CWD containing
		 * a malicious copy of "unqualifiedname.dll" thanks to the way
		 * windows (rather insecurely) includes the CWD in the PATH by default.
		 */
		hDll = LoadLibrary("DBGHELP.DLL");
	}

	return hDll;
}


/*
 * This function is the exception handler passed to SetUnhandledExceptionFilter.
 * It's invoked only if there's an unhandled exception. The handler will use
 * dbghelp.dll to generate a crash dump, then resume the normal unhandled
 * exception process, which will generally exit with a an error message from
 * the runtime.
 *
 * This function is run under the unhandled exception handler, effectively
 * in a crash context, so it should be careful with memory and avoid using
 * any PostgreSQL functions.
 */
static LONG WINAPI
crashDumpHandler(struct _EXCEPTION_POINTERS *pExceptionInfo)
{
	/*
	 * We only write crash dumps if the "crashdumps" directory within
	 * the postgres data directory exists.
	 */
	DWORD attribs = GetFileAttributesA("crashdumps");
	if (attribs != INVALID_FILE_ATTRIBUTES && (attribs & FILE_ATTRIBUTE_DIRECTORY) )
	{
		/* 'crashdumps' exists and is a directory. Try to write a dump' */
		HMODULE hDll = NULL;
		IMAGEHLPAPIVERSION pApiVersion = NULL;
		MINIDUMPWRITEDUMP pDump = NULL;
		LPAPI_VERSION version;
		char dumpPath[_MAX_PATH];

		/*
		 * Dump pretty much everything except shared memory, code segments,
		 * and memory mapped files.
		 */
		MINIDUMP_TYPE dumpType = MiniDumpNormal |
					 MiniDumpWithHandleData |
					 MiniDumpWithDataSegs;

		HANDLE selfProcHandle = GetCurrentProcess();
		DWORD selfPid = GetProcessId(selfProcHandle);
		HANDLE dumpFile;
		DWORD systemTicks;
		struct _MINIDUMP_EXCEPTION_INFORMATION ExInfo;

		ExInfo.ThreadId = GetCurrentThreadId();
		ExInfo.ExceptionPointers = pExceptionInfo;
		ExInfo.ClientPointers = FALSE;

		/* Load the dbghelp.dll library and functions */
		hDll = loadDbgHelp();
		pApiVersion = (IMAGEHLPAPIVERSION)GetProcAddress(hDll, "ImagehlpApiVersion");
		pDump = (MINIDUMPWRITEDUMP)GetProcAddress(hDll, "MiniDumpWriteDump");

		if (pApiVersion==NULL || pDump==NULL)
		{
			write_stderr("could not load dbghelp.dll, cannot write crashdump\n");
			return EXCEPTION_CONTINUE_SEARCH;
		}

		/*
		 * Add additional flags for dumping if supported by the
		 * loaded version of dbghelp.dll.
		 */
		version = (*pApiVersion)();
		if (version->MajorVersion >= 6)
		{
			/* Supported in versions higher than 5.1 */
			dumpType |= MiniDumpWithIndirectlyReferencedMemory |
				MiniDumpWithPrivateReadWriteMemory;
		}
		if (version->MajorVersion > 6 ||
			(version->MajorVersion == 6 && version->MinorVersion > 1))
		{
			/* Supported in versions higher than 6.1 */
			dumpType |= MiniDumpWithThreadInfo;
		}

		systemTicks = GetTickCount();
		snprintf(dumpPath, _MAX_PATH,
				 "crashdumps\\postgres-pid%0i-%0i.mdmp", selfPid, systemTicks);
		dumpPath[_MAX_PATH-1] = '\0';

		dumpFile = CreateFile(dumpPath, GENERIC_WRITE, FILE_SHARE_WRITE,
							  NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL,
							  NULL);
		if (dumpFile==INVALID_HANDLE_VALUE)
		{
			write_stderr("could not open crash dump file %s for writing: error code %d\n",
					dumpPath, GetLastError());
			return EXCEPTION_CONTINUE_SEARCH;
		}

		if ((*pDump)(selfProcHandle, selfPid, dumpFile, dumpType, &ExInfo,
					 NULL, NULL))
			write_stderr("wrote crash dump to %s\n", dumpPath);
		else
			write_stderr("could not write crash dump to %s: error code %08x\n",
					dumpPath, GetLastError());

		CloseHandle(dumpFile);
	}

	return EXCEPTION_CONTINUE_SEARCH;
}


void
pgwin32_install_crashdump_handler(void)
{
	SetUnhandledExceptionFilter(crashDumpHandler);
}
