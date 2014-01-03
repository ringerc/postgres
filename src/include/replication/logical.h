/*-------------------------------------------------------------------------
 * logical.h
 *	   PostgreSQL WAL to logical transformation
 *
 * Copyright (c) 2012-2013, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICAL_H
#define LOGICAL_H

#include "replication/slot.h"

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "replication/output_plugin.h"
#include "storage/shmem.h"
#include "storage/spin.h"


struct LogicalDecodingContext;



typedef void (*LogicalOutputPluginWriterWrite) (
										   struct LogicalDecodingContext *lr,
															XLogRecPtr Ptr,
															TransactionId xid,
															bool last_write
);

typedef LogicalOutputPluginWriterWrite LogicalOutputPluginWriterPrepareWrite;

typedef struct LogicalDecodingContext
{
	/* infrastructure pieces */
	struct XLogReaderState *reader;
	struct ReplicationSlot *slot;
	struct ReorderBuffer *reorder;
	struct SnapBuild *snapshot_builder;

	struct OutputPluginCallbacks callbacks;

	/*
	 * User specified options
	 */
	List	   *output_plugin_options;

	/*
	 * User-Provided callback for writing/streaming out data.
	 */
	LogicalOutputPluginWriterPrepareWrite prepare_write;
	LogicalOutputPluginWriterWrite write;

	/*
	 * Output buffer.
	 */
	StringInfo	out;

	/*
	 * Private data pointer of the output plugin.
	 */
	void	   *output_plugin_private;

	/*
	 * Private data pointer for the data writer.
	 */
	void	   *output_writer_private;

	/*
	 * State for writing output.
	 */
	bool accept_writes;
	bool prepared_write;
	XLogRecPtr write_location;
	TransactionId write_xid;
} LogicalDecodingContext;

extern bool LogicalDecodingCountDBSlots(Oid dboid, int *nslots, int *nactive);

extern XLogRecPtr ComputeLogicalRestartLSN(void);

/* change logical xmin */
extern void IncreaseLogicalXminForSlot(XLogRecPtr lsn, TransactionId xmin);

/* change recovery restart location */
extern void IncreaseRestartDecodingForSlot(XLogRecPtr current_lsn, XLogRecPtr restart_lsn);

extern void LogicalConfirmReceivedLocation(XLogRecPtr lsn);

extern void CheckLogicalDecodingRequirements(void);

extern LogicalDecodingContext *CreateDecodingContext(
							 bool is_init, char *plugin,
							 XLogRecPtr	start_lsn,
							 List *output_plugin_options,
							 XLogPageReadCB read_page,
						 LogicalOutputPluginWriterPrepareWrite prepare_write,
							 LogicalOutputPluginWriterWrite do_write);
extern void DecodingContextFindStartpoint(LogicalDecodingContext *ctx);
extern bool DecodingContextReady(LogicalDecodingContext *ctx);
extern void FreeDecodingContext(LogicalDecodingContext *ctx);

#endif
