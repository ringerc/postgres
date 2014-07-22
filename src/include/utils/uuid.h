/*-------------------------------------------------------------------------
 *
 * uuid.h
 *	  Header file for the "uuid" ADT. In C, we use the name pg_uuid_t,
 *	  to avoid conflicts with any uuid_t type that might be defined by
 *	  the system headers.
 *
 * Copyright (c) 2007-2014, PostgreSQL Global Development Group
 *
 * src/include/utils/uuid.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UUID_H
#define UUID_H

/* guid size in bytes */
#define UUID_LEN 16

/* opaque struct; defined in uuid.c */
typedef struct pg_uuid_t pg_uuid_t;

/* fmgr interface macros */
#define UUIDPGetDatum(X)		PointerGetDatum(X)
#define PG_RETURN_UUID_P(X)		return UUIDPGetDatum(X)
#define DatumGetUUIDP(X)		((pg_uuid_t *) DatumGetPointer(X))
#define PG_GETARG_UUID_P(X)		DatumGetUUIDP(PG_GETARG_DATUM(X))

/*
 * Write a uuid to a UUID_LEN sized buffer. For use with code that
 * handles UUIDs in raw binary form.
 */
void pg_copy_uuid_to_bytebuf(char *buf, pg_uuid_t *uuid, size_t bufsize);

/*
 * Create a palloc'd pg_uuid_t from a UUID_LEN sized buffer. For use with
 * code that handles UUIDs in raw binary form.
 */
pg_uuid_t *pg_uuid_from_bytebuf(char *buf, size_t bufsize);

/* Note that all Datum handling functions are in builtins.h */

#endif   /* UUID_H */
