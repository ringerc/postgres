# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Resource-attribute API end-to-end test (OTEL_TRACING_API v2.1).
#
# Verifies that contrib/otel populates an OTel Resource for the
# postmaster process, exposes it via OtelTracingApi.
# get_resource_attributes(), and that operators can override the
# defaults via the otel.service_name and otel.service_instance_id
# GUCs.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# ----------------------------------------------------------------------
# Test 1: defaults --- no service_name / service_instance_id overrides.
# ----------------------------------------------------------------------
{
	my $node = PostgreSQL::Test::Cluster->new('default');
	$node->init;
	$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel,otel_postgres_tracing,test_otel_exporter'
log_min_messages = warning
EOCONF
	$node->start;
	$node->safe_psql('postgres',
		'CREATE EXTENSION otel; CREATE EXTENSION test_otel_exporter');

	my $out = $node->safe_psql('postgres',
		'SELECT test_otel_resource_attributes()');

	# Expected shape: service.name=postgres;service.instance.id=<digits>;host.name=<something>
	like($out, qr/(?:^|;)service\.name=postgres(?:;|$)/,
		'default service.name is "postgres"');
	like($out,
		qr/(?:^|;)service\.instance\.id=\d+(?:;|$)/,
		'default service.instance.id is the cluster system identifier (digits)');
	like($out, qr/(?:^|;)host\.name=[^;]+(?:;|$)/,
		'host.name is populated');

	$node->stop;
}

# ----------------------------------------------------------------------
# Test 2: GUC overrides --- service_name and service_instance_id set
# via postgresql.conf are reflected in the Resource.
# ----------------------------------------------------------------------
{
	my $node = PostgreSQL::Test::Cluster->new('overridden');
	$node->init;
	$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel,otel_postgres_tracing,test_otel_exporter'
otel.service_name = 'orders-db-primary'
otel.service_instance_id = 'instance-abc-42'
log_min_messages = warning
EOCONF
	$node->start;
	$node->safe_psql('postgres',
		'CREATE EXTENSION otel; CREATE EXTENSION test_otel_exporter');

	my $out = $node->safe_psql('postgres',
		'SELECT test_otel_resource_attributes()');

	like($out, qr/(?:^|;)service\.name=orders-db-primary(?:;|$)/,
		'GUC override: service.name = orders-db-primary');
	like($out, qr/(?:^|;)service\.instance\.id=instance-abc-42(?:;|$)/,
		'GUC override: service.instance.id = instance-abc-42');

	$node->stop;
}

done_testing();
