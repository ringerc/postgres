/*-------------------------------------------------------------------------
 *
 * unix_crashdump.c
 *         No-op crash dump handler for unsupported platforms
 *
 * No support for generating crash dumps has been implemented for unix
 * platforms, but an implementation of "void installCrashDumpHandler(void)"
 * is still required.
 *
 * Copyright (c) 1996-2010, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/port/unix_crashdump.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

void
installCrashDumpHandler(void)
{
}
