/*-------------------------------------------------------------------------
 *
 * otel.h
 *	  Umbrella header: every public otel_api header.
 *
 * Prefer the header for your audience:
 *	 otel_producer.h		creating spans
 *	 otel_exporter.h		receiving finished spans
 *	 otel_internal_api.h	otel_postgres_tracing internals
 *	 otel_semconv.h			attribute names
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * otel_api/otel.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OTEL_H
#define OTEL_H

#include "otel_types.h"
#include "otel_api.h"
#include "otel_producer.h"
#include "otel_exporter.h"
#include "otel_internal_api.h"
#include "otel_semconv.h"

#endif							/* OTEL_H */
