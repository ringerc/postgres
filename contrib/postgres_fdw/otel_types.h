/*-------------------------------------------------------------------------
 *
 * otel_types.h
 *	  Value types shared by every otel_api audience: trace and span IDs,
 *	  the span context, span kinds and status codes, and the text and
 *	  binary codecs for trace context.
 *
 * Header-only and self-contained: it depends only on core postgres
 * headers, so a consumer that wants an optional dependency on otel_api
 * can vendor it together with otel_api.h, otel_producer.h and
 * otel_producer_stub.h.
 *
 * IDs are binary.  Hex is only produced and parsed at the edges: the
 * W3C traceparent string, log output, SQL-visible functions.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * otel_api/otel_types.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OTEL_TYPES_H
#define OTEL_TYPES_H

#include "lib/stringinfo.h"
#include "libpq/pqformat.h"

/* Binary ID sizes. */
#define OTEL_TRACE_ID_BYTES		16
#define OTEL_SPAN_ID_BYTES		8

/* Hex lengths, excluding the trailing NUL. */
#define OTEL_TRACE_ID_HEX_LEN	(OTEL_TRACE_ID_BYTES * 2)
#define OTEL_SPAN_ID_HEX_LEN	(OTEL_SPAN_ID_BYTES * 2)

/* "00-{32 hex}-{16 hex}-{2 hex}", excluding the trailing NUL. */
#define OTEL_TRACEPARENT_LEN	55

/* W3C trace-flags bits. */
#define OTEL_TRACE_FLAG_SAMPLED	0x01
#define OTEL_TRACE_FLAG_RANDOM	0x02	/* W3C Trace Context level 2 */

typedef struct OtelTraceId
{
	uint8		b[OTEL_TRACE_ID_BYTES];
} OtelTraceId;

typedef struct OtelSpanId
{
	uint8		b[OTEL_SPAN_ID_BYTES];
} OtelSpanId;

/*
 * The only trace-context type.  An all-zero trace_id means "no context";
 * an all-zero span_id means "no parent span".  Both are invalid IDs per
 * W3C, so they never collide with a real one.
 *
 * tracestate is the W3C vendor list, or NULL.  Its lifetime is whatever
 * the function that filled the struct documents; copy it to keep it.
 */
typedef struct OtelSpanContext
{
	OtelTraceId trace_id;
	OtelSpanId	span_id;
	uint8		trace_flags;
	const char *tracestate;
} OtelSpanContext;

typedef enum OtelSpanKind
{
	OTEL_SPAN_KIND_INTERNAL = 0,
	OTEL_SPAN_KIND_SERVER = 1,
	OTEL_SPAN_KIND_CLIENT = 2,
	OTEL_SPAN_KIND_PRODUCER = 3,
	OTEL_SPAN_KIND_CONSUMER = 4,
} OtelSpanKind;

typedef enum OtelSpanStatus
{
	OTEL_STATUS_UNSET = 0,
	OTEL_STATUS_OK = 1,
	OTEL_STATUS_ERROR = 2,
} OtelSpanStatus;

/*
 * InstrumentationScope: which producer created a span.  Obtained once per
 * producer from the tracer_register entry point and cached.  Owned by
 * otel_api; valid for the backend's lifetime.
 */
typedef struct OtelInstrumentationScope
{
	const char *name;
	const char *version;		/* may be NULL */
	const char *schema_url;		/* may be NULL */
} OtelInstrumentationScope;


/*
 * A typed attribute.  Used for span attributes and event attributes, in
 * both directions: producers pass arrays of these to add_event, and
 * exporters read them from OtelSpan.  In an exported span every string
 * is owned by otel_api and valid for the duration of the emit hook.
 */
typedef enum OtelAttrType
{
	OTEL_ATTR_STRING = 0,
	OTEL_ATTR_INT = 1,
	OTEL_ATTR_DOUBLE = 2,
	OTEL_ATTR_BOOL = 3,
} OtelAttrType;

typedef struct OtelAttribute
{
	const char *key;
	OtelAttrType type;
	union
	{
		const char *s;
		int64		i;
		double		d;
		bool		b;
	}			v;
} OtelAttribute;

#define OTEL_ATTR_STR(k, val)	((OtelAttribute) {.key = (k), .type = OTEL_ATTR_STRING, .v.s = (val)})
#define OTEL_ATTR_I64(k, val)	((OtelAttribute) {.key = (k), .type = OTEL_ATTR_INT, .v.i = (val)})
#define OTEL_ATTR_F64(k, val)	((OtelAttribute) {.key = (k), .type = OTEL_ATTR_DOUBLE, .v.d = (val)})
#define OTEL_ATTR_BOOLV(k, val)	((OtelAttribute) {.key = (k), .type = OTEL_ATTR_BOOL, .v.b = (val)})


/* ----------------------------------------------------------------
 * ID helpers
 * ---------------------------------------------------------------- */

static inline bool
otel_trace_id_is_valid(const OtelTraceId *id)
{
	for (int i = 0; i < OTEL_TRACE_ID_BYTES; i++)
		if (id->b[i] != 0)
			return true;
	return false;
}

static inline bool
otel_span_id_is_valid(const OtelSpanId *id)
{
	uint64		v;

	memcpy(&v, id->b, sizeof(v));
	return v != 0;
}

static inline bool
otel_span_id_equal(const OtelSpanId *a, const OtelSpanId *b)
{
	return memcmp(a->b, b->b, OTEL_SPAN_ID_BYTES) == 0;
}

static inline bool
otel_span_context_is_valid(const OtelSpanContext *ctx)
{
	return otel_trace_id_is_valid(&ctx->trace_id) &&
		otel_span_id_is_valid(&ctx->span_id);
}

static inline bool
otel_span_context_sampled(const OtelSpanContext *ctx)
{
	return (ctx->trace_flags & OTEL_TRACE_FLAG_SAMPLED) != 0;
}

/* Write 2 * n lowercase hex chars plus a NUL to out. */
static inline void
otel_bytes_to_hex(const uint8 *in, int n, char *out)
{
	static const char hexdigits[] = "0123456789abcdef";

	for (int i = 0; i < n; i++)
	{
		out[2 * i] = hexdigits[in[i] >> 4];
		out[2 * i + 1] = hexdigits[in[i] & 0x0f];
	}
	out[2 * n] = '\0';
}

/*
 * Parse exactly 2 * n lowercase hex chars.  W3C requires lowercase, so
 * uppercase is rejected.  Returns false, leaving out unspecified, on any
 * other character.
 */
static inline bool
otel_hex_to_bytes(const char *in, int n, uint8 *out)
{
	for (int i = 0; i < 2 * n; i++)
	{
		char		c = in[i];
		int			v;

		if (c >= '0' && c <= '9')
			v = c - '0';
		else if (c >= 'a' && c <= 'f')
			v = c - 'a' + 10;
		else
			return false;
		if ((i & 1) == 0)
			out[i / 2] = (uint8) (v << 4);
		else
			out[i / 2] |= (uint8) v;
	}
	return true;
}

static inline void
otel_trace_id_to_hex(const OtelTraceId *id, char out[OTEL_TRACE_ID_HEX_LEN + 1])
{
	otel_bytes_to_hex(id->b, OTEL_TRACE_ID_BYTES, out);
}

static inline void
otel_span_id_to_hex(const OtelSpanId *id, char out[OTEL_SPAN_ID_HEX_LEN + 1])
{
	otel_bytes_to_hex(id->b, OTEL_SPAN_ID_BYTES, out);
}

static inline bool
otel_trace_id_from_hex(const char *hex, OtelTraceId *out)
{
	return otel_hex_to_bytes(hex, OTEL_TRACE_ID_BYTES, out->b) &&
		hex[OTEL_TRACE_ID_HEX_LEN] == '\0';
}

static inline bool
otel_span_id_from_hex(const char *hex, OtelSpanId *out)
{
	return otel_hex_to_bytes(hex, OTEL_SPAN_ID_BYTES, out->b) &&
		hex[OTEL_SPAN_ID_HEX_LEN] == '\0';
}


/* ----------------------------------------------------------------
 * Text codec: the W3C traceparent header value
 * ---------------------------------------------------------------- */

/*
 * Format ctx as a version-00 traceparent.  out must hold
 * OTEL_TRACEPARENT_LEN + 1 bytes.  tracestate is not part of
 * traceparent; callers propagate it separately.
 */
static inline void
otel_traceparent_format(const OtelSpanContext *ctx,
						char out[OTEL_TRACEPARENT_LEN + 1])
{
	out[0] = '0';
	out[1] = '0';
	out[2] = '-';
	otel_bytes_to_hex(ctx->trace_id.b, OTEL_TRACE_ID_BYTES, out + 3);
	out[35] = '-';
	otel_bytes_to_hex(ctx->span_id.b, OTEL_SPAN_ID_BYTES, out + 36);
	out[52] = '-';
	otel_bytes_to_hex(&ctx->trace_flags, 1, out + 53);
}

/*
 * Parse a W3C traceparent.  Returns false on any malformed input.
 *
 * Version 00 must be exactly 55 chars.  Versions 01..fe are parsed for
 * the known prefix; if longer than 55 chars, char 55 must be '-'.
 * Version ff is invalid.  All-zero trace-id or parent-id is invalid.
 *
 * Sets out->tracestate to NULL.
 */
static inline bool
otel_traceparent_parse(const char *s, OtelSpanContext *out)
{
	size_t		len = strlen(s);
	uint8		version;

	if (len < OTEL_TRACEPARENT_LEN)
		return false;
	if (s[2] != '-' || s[35] != '-' || s[52] != '-')
		return false;
	if (!otel_hex_to_bytes(s, 1, &version) || version == 0xff)
		return false;
	if (version == 0x00 ? len != OTEL_TRACEPARENT_LEN
		: (len > OTEL_TRACEPARENT_LEN && s[OTEL_TRACEPARENT_LEN] != '-'))
		return false;
	if (!otel_hex_to_bytes(s + 3, OTEL_TRACE_ID_BYTES, out->trace_id.b) ||
		!otel_hex_to_bytes(s + 36, OTEL_SPAN_ID_BYTES, out->span_id.b) ||
		!otel_hex_to_bytes(s + 53, 1, &out->trace_flags))
		return false;
	if (!otel_span_context_is_valid(out))
		return false;
	out->tracestate = NULL;
	return true;
}


/* ----------------------------------------------------------------
 * Binary codec: span context in a StringInfo message
 *
 * Wire format (network byte order):
 *	 uint8	 format version (OTEL_SPAN_CONTEXT_WIRE_V1)
 *	 uint16	 length of the rest of the record, in bytes
 *	 [16]	 trace_id
 *	 [8]	 span_id
 *	 uint8	 trace_flags
 *	 uint16	 tracestate length, 0 if none
 *	 [n]	 tracestate bytes, not NUL-terminated
 *
 * The length prefix lets a reader skip records from a newer format
 * version: fields a newer writer appends come after the ones above.
 * ---------------------------------------------------------------- */

#define OTEL_SPAN_CONTEXT_WIRE_V1	1
#define OTEL_TRACESTATE_MAX_LEN		512	/* W3C: 32 members, 512 chars total */

static inline void
otel_span_context_send(StringInfo buf, const OtelSpanContext *ctx)
{
	size_t		tslen = ctx->tracestate ? strlen(ctx->tracestate) : 0;

	if (tslen > OTEL_TRACESTATE_MAX_LEN)
		tslen = 0;				/* invalid per W3C; drop, don't truncate */
	pq_sendint8(buf, OTEL_SPAN_CONTEXT_WIRE_V1);
	pq_sendint16(buf, (uint16) (OTEL_TRACE_ID_BYTES + OTEL_SPAN_ID_BYTES + 1 +
								2 + tslen));
	pq_sendbytes(buf, ctx->trace_id.b, OTEL_TRACE_ID_BYTES);
	pq_sendbytes(buf, ctx->span_id.b, OTEL_SPAN_ID_BYTES);
	pq_sendint8(buf, ctx->trace_flags);
	pq_sendint16(buf, (uint16) tslen);
	if (tslen > 0)
		pq_sendbytes(buf, ctx->tracestate, tslen);
}

/*
 * Read a record written by otel_span_context_send.  tracestate, if
 * present, is palloc'd in CurrentMemoryContext.  Raises ERROR (via
 * pq_copymsgbytes) on a truncated message, like the other pq_get*
 * functions; returns false for an unknown format version, after
 * skipping the record, and for an invalid context.
 */
static inline bool
otel_span_context_recv(StringInfo buf, OtelSpanContext *ctx)
{
	uint8		version = pq_getmsgbyte(buf);
	uint16		reclen = pq_getmsgint(buf, 2);
	int			start = buf->cursor;
	uint16		tslen;

	memset(ctx, 0, sizeof(*ctx));
	if (version != OTEL_SPAN_CONTEXT_WIRE_V1 ||
		reclen < OTEL_TRACE_ID_BYTES + OTEL_SPAN_ID_BYTES + 1 + 2)
	{
		(void) pq_getmsgbytes(buf, reclen);
		return false;
	}
	pq_copymsgbytes(buf, ctx->trace_id.b, OTEL_TRACE_ID_BYTES);
	pq_copymsgbytes(buf, ctx->span_id.b, OTEL_SPAN_ID_BYTES);
	ctx->trace_flags = pq_getmsgbyte(buf);
	tslen = pq_getmsgint(buf, 2);
	if (tslen > reclen - (buf->cursor - start))
	{
		/* tracestate claims to run past the record: corrupt. */
		(void) pq_getmsgbytes(buf, reclen - (buf->cursor - start));
		memset(ctx, 0, sizeof(*ctx));
		return false;
	}
	if (tslen > 0)
	{
		char	   *ts = palloc(tslen + 1);

		pq_copymsgbytes(buf, ts, tslen);
		ts[tslen] = '\0';
		ctx->tracestate = ts;
	}
	/* Skip fields appended by a newer writer. */
	if (buf->cursor - start < reclen)
		(void) pq_getmsgbytes(buf, reclen - (buf->cursor - start));
	return otel_span_context_is_valid(ctx);
}

#endif							/* OTEL_TYPES_H */
