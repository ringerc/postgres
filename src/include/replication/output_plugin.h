/*-------------------------------------------------------------------------
 * output_plugin.h
 *	   PostgreSQL Logical Decode Plugin Interface
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef OUTPUT_PLUGIN_H
#define OUTPUT_PLUGIN_H

#include "replication/reorderbuffer.h"

struct LogicalDecodingContext;

/*
 * Callback that gets called in a user-defined plugin. ctx->private_data can
 * be set to some private data.
 *
 * "is_init" will be set to "true" if the decoding slot just got defined. When
 * the same slot is used from there one, it will be "false".
 *
 * Gets looked up via the library symbol pg_decode_init.
 */
typedef void (*LogicalDecodeInitCB) (
										  struct LogicalDecodingContext *ctx,
												 bool is_init
);

/*
 * Callback called for every BEGIN of a successful transaction.
 *
 * Gets looked up via the library symbol pg_decode_begin_txn.
 */
typedef void (*LogicalDecodeBeginCB) (
											 struct LogicalDecodingContext *,
												  ReorderBufferTXN *txn);

/*
 * Callback for every individual change in a successful transaction.
 *
 * Gets looked up via the library symbol pg_decode_change.
 */
typedef void (*LogicalDecodeChangeCB) (
											 struct LogicalDecodingContext *,
												   ReorderBufferTXN *txn,
												   Relation relation,
												   ReorderBufferChange *change
);

/*
 * Called for every COMMIT of a successful transaction.
 *
 * Gets looked up via the library symbol pg_decode_commit_txn.
 */
typedef void (*LogicalDecodeCommitCB) (
											 struct LogicalDecodingContext *,
												   ReorderBufferTXN *txn,
												   XLogRecPtr commit_lsn);

/*
 * Called to cleanup the state of an output plugin.
 *
 * Gets looked up via the library symbol pg_decode_cleanup.
 */
typedef void (*LogicalDecodeCleanupCB) (
											  struct LogicalDecodingContext *
);

#endif   /* OUTPUT_PLUGIN_H */
