/*-------------------------------------------------------------------------
 *
 * otel_producer_stub.h
 *	  No-op stand-in for otel_producer.h.
 *
 * For producers that make otel_api an optional build dependency.  Copy
 * this file and otel_types.h into the consumer's tree, then:
 *
 *	   #ifdef HAVE_OTEL_API
 *	   #include "otel_api/otel_producer.h"
 *	   #else
 *	   #include "otel_producer_stub.h"
 *	   #endif
 *
 * Every name in otel_producer.h that a producer uses is declared here,
 * with the same signature, doing nothing.  No #ifdef is needed at call
 * sites or declarations.  The conformance tests compile one source file
 * against both headers to keep them in step.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * otel_api/otel_producer_stub.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OTEL_PRODUCER_H
#define OTEL_PRODUCER_H
#define OTEL_PRODUCER_STUB 1

#include "utils/elog.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"

#include "otel_types.h"

typedef struct OtelSpanRef
{
	int64		v;
} OtelSpanRef;

#define OTEL_SPAN_NONE	((OtelSpanRef) {0})

typedef struct OtelActivation
{
	int64		v;
} OtelActivation;

#define OTEL_ACTIVATION_NONE	((OtelActivation) {0})

typedef struct OtelTracer
{
	const char *name;
	const char *version;
	const char *schema_url;
	const OtelInstrumentationScope *scope;
} OtelTracer;

typedef enum OtelSpanParent
{
	OTEL_PARENT_ACTIVE = 0,
	OTEL_PARENT_CONTEXT,
	OTEL_PARENT_SPAN,
	OTEL_PARENT_ROOT,
} OtelSpanParent;

#define OTEL_OWNER_SESSION	((ResourceOwner) (uintptr_t) 1)

typedef struct OtelSpanStartArgs
{
	uint32		struct_size;
	OtelTracer *tracer;
	const char *name;
	OtelSpanKind kind;
	OtelSpanParent parent;
	const OtelSpanContext *parent_ctx;
	OtelSpanRef parent_span;
	ResourceOwner owner;
	bool		detached;
	bool		scoped;
	TimestampTz start_time;
	bool		force_sample;
} OtelSpanStartArgs;

static inline bool otel_span_recording(OtelSpanRef s) { (void) s; return false; }
static inline OtelSpanRef otel_span_start_args(const OtelSpanStartArgs *args) { (void) args; return OTEL_SPAN_NONE; }
#define otel_span_start(...) \
	otel_span_start_args(&(OtelSpanStartArgs) { \
		.struct_size = sizeof(OtelSpanStartArgs), __VA_ARGS__ })
static inline void otel_span_end(OtelSpanRef s) { (void) s; }
static inline void otel_span_discard(OtelSpanRef s) { (void) s; }
static inline OtelActivation otel_span_activate(OtelSpanRef s) { (void) s; return OTEL_ACTIVATION_NONE; }
static inline void otel_span_deactivate(OtelActivation tok) { (void) tok; }
static inline void otel_span_end_at(OtelSpanRef s, TimestampTz t) { (void) s; (void) t; }
static inline void otel_span_set_str(OtelSpanRef s, const char *k, const char *v) { (void) s; (void) k; (void) v; }
static inline void otel_span_set_int(OtelSpanRef s, const char *k, int64 v) { (void) s; (void) k; (void) v; }
static inline void otel_span_set_double(OtelSpanRef s, const char *k, double v) { (void) s; (void) k; (void) v; }
static inline void otel_span_set_bool(OtelSpanRef s, const char *k, bool v) { (void) s; (void) k; (void) v; }
static inline void otel_span_set_printf(OtelSpanRef s, const char *k, const char *fmt,...) pg_attribute_printf(3, 4);
static inline void otel_span_set_printf(OtelSpanRef s, const char *k, const char *fmt,...) { (void) s; (void) k; (void) fmt; }
#define OTEL_SPAN_SET_STR_IF_RECORDING(s, key, expr) do { (void) (s); } while (0)
static inline void otel_span_set_name(OtelSpanRef s, const char *n) { (void) s; (void) n; }
static inline void otel_span_set_status(OtelSpanRef s, OtelSpanStatus c, const char *d) { (void) s; (void) c; (void) d; }
static inline void otel_span_add_event(OtelSpanRef s, const char *n, TimestampTz t, const OtelAttribute *a, int na) { (void) s; (void) n; (void) t; (void) a; (void) na; }
static inline void otel_span_add_link(OtelSpanRef s, const OtelSpanContext *t) { (void) s; (void) t; }
static inline void otel_span_capture_error(OtelSpanRef s) { (void) s; }
static inline void otel_span_record_error(OtelSpanRef s, const ErrorData *e) { (void) s; (void) e; }
static inline OtelSpanRef otel_span_current(void) { return OTEL_SPAN_NONE; }
static inline bool otel_span_context_of(OtelSpanRef s, OtelSpanContext *out) { (void) s; memset(out, 0, sizeof(*out)); return false; }
static inline void otel_resource_add(const char *k, const char *v) { (void) k; (void) v; }

#endif							/* OTEL_PRODUCER_H */
