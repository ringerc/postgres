/*-------------------------------------------------------------------------
 *
 * otel_pg_attrs.h
 *	  Attribute-key names owned by otel_postgres_tracing itself: the
 *	  pg.plan.*, pg.exec.*, pg.lock.*, pg.fdw.* and related keys this
 *	  module stamps onto spans.
 *
 * OTel semantic-convention names (db.*, server.*, client.*, exception.*,
 * ...) and the project-wide pg.* names (pg.session.user, pg.query_id,
 * pg.error.*, pg.parallel.*, pg.otel.span_source, ...) live in
 * <otel_api/otel_semconv.h> instead; use those directly.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/otel_postgres_tracing/otel_pg_attrs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONTRIB_OTEL_POSTGRES_TRACING_PG_ATTRS_H
#define CONTRIB_OTEL_POSTGRES_TRACING_PG_ATTRS_H

/* Plan-shape capture (otel_planshape.c) */
#define OTEL_ATTR_PG_PLAN_SHAPE_HASH		"pg.plan.shape_hash"
#define OTEL_ATTR_PG_PLAN_NODE_COUNT		"pg.plan.node_count"
#define OTEL_ATTR_PG_PLAN_RISKS				"pg.plan.risks"

/* Per-node "rich" events (otel_planpath.c) */
#define OTEL_ATTR_PG_PLAN_NODE_TYPE				"pg.plan.node.type"
#define OTEL_ATTR_PG_PLAN_NODE_ACTUAL_STARTUP_MS	"pg.plan.node.actual_startup_ms"
#define OTEL_ATTR_PG_PLAN_NODE_ACTUAL_TOTAL_MS		"pg.plan.node.actual_total_ms"
#define OTEL_ATTR_PG_PLAN_NODE_ROWS					"pg.plan.node.rows"
#define OTEL_ATTR_PG_PLAN_NODE_LOOPS					"pg.plan.node.loops"
#define OTEL_ATTR_PG_PLAN_NODE_BUFFERS_READ			"pg.plan.node.buffers_read"
#define OTEL_ATTR_PG_PLAN_NODE_BUFFERS_HIT			"pg.plan.node.buffers_hit"
#define OTEL_ATTR_PG_PLAN_NODE_BUFFERS_DIRTIED		"pg.plan.node.buffers_dirtied"
#define OTEL_ATTR_PG_PLAN_NODE_EVENT					"pg.plan.node"	/* event name */

/* Compact-actuals + pathology-flags collector (otel_planpath.c) */
#define OTEL_ATTR_PG_EXEC_SPILLED				"pg.exec.spilled"
#define OTEL_ATTR_PG_EXEC_SPILL_KB				"pg.exec.spill_kb"
#define OTEL_ATTR_PG_EXEC_MISESTIMATE			"pg.exec.misestimate"
#define OTEL_ATTR_PG_EXEC_MISESTIMATE_RATIO		"pg.exec.misestimate_ratio"
#define OTEL_ATTR_PG_EXEC_MISESTIMATE_NODE		"pg.exec.misestimate_node"
#define OTEL_ATTR_PG_EXEC_BITMAP_LOSSY_PAGES		"pg.exec.bitmap_lossy_pages"
#define OTEL_ATTR_PG_EXEC_BUFFERS_READ			"pg.exec.buffers_read"
#define OTEL_ATTR_PG_EXEC_BUFFERS_HIT			"pg.exec.buffers_hit"
#define OTEL_ATTR_PG_EXEC_BUFFERS_DIRTIED		"pg.exec.buffers_dirtied"
#define OTEL_ATTR_PG_EXEC_NODE_COUNT				"pg.exec.node_count"
#define OTEL_ATTR_PG_EXEC_SLOWEST_NODES			"pg.exec.slowest_nodes"

/* Curated child spans (otel_planspans.c) */
#define OTEL_ATTR_PG_CUSTOMSCAN_METHOD		"pg.customscan.method"

/* FDW scan spans (otel_fdw.c) */
/* db.system.name / db.collection.name (semconv) cover this span's attrs. */

/* Lock-wait bridge span (otel_sdt_bridge.c) */
#define OTEL_ATTR_PG_LOCK_TYPE			"pg.lock.type"
#define OTEL_ATTR_PG_LOCK_MODE			"pg.lock.mode"
#define OTEL_ATTR_PG_LOCK_DBOID			"pg.lock.dboid"
#define OTEL_ATTR_PG_LOCK_RELID			"pg.lock.relid"
#define OTEL_ATTR_PG_LOCK_BLOCK			"pg.lock.block"
#define OTEL_ATTR_PG_LOCK_OFFSET			"pg.lock.offset"
#define OTEL_ATTR_PG_LOCK_XID			"pg.lock.xid"
#define OTEL_ATTR_PG_LOCK_PROCNO			"pg.lock.procno"
#define OTEL_ATTR_PG_LOCK_LOCALXID		"pg.lock.localxid"
#define OTEL_ATTR_PG_LOCK_FIELD1			"pg.lock.field1"
#define OTEL_ATTR_PG_LOCK_FIELD2			"pg.lock.field2"
#define OTEL_ATTR_PG_LOCK_FIELD3			"pg.lock.field3"
#define OTEL_ATTR_PG_LOCK_FIELD4			"pg.lock.field4"

/* Replica-apply / commit-LSN bridge spans (otel_sdt_bridge.c) */
#define OTEL_ATTR_PG_COMMIT_LSN			"pg.commit_lsn"

#endif							/* CONTRIB_OTEL_POSTGRES_TRACING_PG_ATTRS_H */
