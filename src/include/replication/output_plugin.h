/*-------------------------------------------------------------------------
 * output_plugin.h
 *	   PostgreSQL Logical Decode Plugin Interface
 *
 * Copyright (c) 2012-2016, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef OUTPUT_PLUGIN_H
#define OUTPUT_PLUGIN_H

#include "replication/reorderbuffer.h"

struct LogicalDecodingContext;
struct OutputPluginCallbacks;

typedef enum OutputPluginOutputType
{
	OUTPUT_PLUGIN_BINARY_OUTPUT,
	OUTPUT_PLUGIN_TEXTUAL_OUTPUT
} OutputPluginOutputType;

/*
 * Options set by the output plugin, in the startup callback.
 */
typedef struct OutputPluginOptions
{
	OutputPluginOutputType output_type;
} OutputPluginOptions;

/*
 * Type of the shared library symbol _PG_output_plugin_init that is looked up
 * when loading an output plugin shared library.
 */
typedef void (*LogicalOutputPluginInit) (struct OutputPluginCallbacks *cb);

/*
 * Callback that gets called in a user-defined plugin. ctx->private_data can
 * be set to some private data.
 *
 * "is_init" will be set to "true" if the decoding slot just got defined. When
 * the same slot is used from there one, it will be "false".
 */
typedef void (*LogicalDecodeStartupCB) (
										  struct LogicalDecodingContext *ctx,
												OutputPluginOptions *options,
													bool is_init
);

/*
 * Callback called for every (explicit or implicit) BEGIN of a successful
 * transaction.
 */
typedef void (*LogicalDecodeBeginCB) (
											 struct LogicalDecodingContext *,
												  ReorderBufferTXN *txn);

/*
 * Callback for every individual change in a successful transaction.
 */
typedef void (*LogicalDecodeChangeCB) (
											 struct LogicalDecodingContext *,
												   ReorderBufferTXN *txn,
												   Relation relation,
												   ReorderBufferChange *change
);

/*
 * Called for every (explicit or implicit) COMMIT of a successful transaction.
 */
typedef void (*LogicalDecodeCommitCB) (
											 struct LogicalDecodingContext *,
												   ReorderBufferTXN *txn,
												   XLogRecPtr commit_lsn);

/*
 * Filter changes by origin.
 */
typedef bool (*LogicalDecodeFilterByOriginCB) (
											 struct LogicalDecodingContext *,
													  RepOriginId origin_id);

/*
 * Called to shutdown an output plugin.
 */
typedef void (*LogicalDecodeShutdownCB) (
											  struct LogicalDecodingContext *
);

/*
 * A sequence has been advanced, with a new chunk allocated and written
 * to WAL.
 *
 * This doesn't happen every time nextval() is called, there's caching.  It's
 * guaranteed that the new sequence position will be at or ahead of the most
 * recent value any committed xact has obtained when this callback is invoked.
 */
typedef void (*LogicalDecodeSeqAdvanceCB) (
											struct LogicalDecodingContext *,
											const char * seq_name,
											uint64 last_value
		);


/*
 * Output plugin callbacks
 */
typedef struct OutputPluginCallbacks
{
	LogicalDecodeStartupCB startup_cb;
	LogicalDecodeBeginCB begin_cb;
	LogicalDecodeChangeCB change_cb;
	LogicalDecodeCommitCB commit_cb;
	LogicalDecodeFilterByOriginCB filter_by_origin_cb;
	LogicalDecodeShutdownCB shutdown_cb;
	LogicalDecodeSeqAdvanceCB seq_advance_cb;
} OutputPluginCallbacks;

void		OutputPluginPrepareWrite(struct LogicalDecodingContext *ctx, bool last_write);
void		OutputPluginWrite(struct LogicalDecodingContext *ctx, bool last_write);

#endif   /* OUTPUT_PLUGIN_H */
