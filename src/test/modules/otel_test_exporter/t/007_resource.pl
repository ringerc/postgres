# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Resource-attribute API end-to-end test (OTEL_TRACING_API v2.1).
#
# Verifies that otel_api populates an OTel Resource for the
# postmaster process, exposes it via OtelTracingApi.
# get_resource_attributes(), and that operators can override the
# defaults via the otel_api.service_name and otel_api.service_instance_id
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
shared_preload_libraries = 'otel_api,otel_postgres_tracing,test_otel_exporter'
log_min_messages = warning
EOCONF
	$node->start;
	$node->safe_psql('postgres',
		'CREATE EXTENSION otel_api; CREATE EXTENSION test_otel_exporter');

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
shared_preload_libraries = 'otel_api,otel_postgres_tracing,test_otel_exporter'
otel_api.service_name = 'orders-db-primary'
otel_api.service_instance_id = 'instance-abc-42'
log_min_messages = warning
EOCONF
	$node->start;
	$node->safe_psql('postgres',
		'CREATE EXTENSION otel_api; CREATE EXTENSION test_otel_exporter');

	my $out = $node->safe_psql('postgres',
		'SELECT test_otel_resource_attributes()');

	like($out, qr/(?:^|;)service\.name=orders-db-primary(?:;|$)/,
		'GUC override: service.name = orders-db-primary');
	like($out, qr/(?:^|;)service\.instance\.id=instance-abc-42(?:;|$)/,
		'GUC override: service.instance.id = instance-abc-42');

	$node->stop;
}

# ----------------------------------------------------------------------
# Test 3: OTEL_SERVICE_NAME env fallback --- no GUC set, env var wins.
# ----------------------------------------------------------------------
{
	local $ENV{OTEL_SERVICE_NAME} = 'env-service';

	my $node = PostgreSQL::Test::Cluster->new('env_fallback');
	$node->init;
	$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel_api,otel_postgres_tracing,test_otel_exporter'
log_min_messages = warning
EOCONF
	$node->start;
	$node->safe_psql('postgres',
		'CREATE EXTENSION otel_api; CREATE EXTENSION test_otel_exporter');

	my $out = $node->safe_psql('postgres',
		'SELECT test_otel_resource_attributes()');

	like($out, qr/(?:^|;)service\.name=env-service(?:;|$)/,
		'OTEL_SERVICE_NAME env fallback: service.name = env-service');

	$node->stop;
}

# ----------------------------------------------------------------------
# Test 4: GUC wins over OTEL_SERVICE_NAME env.
# ----------------------------------------------------------------------
{
	local $ENV{OTEL_SERVICE_NAME} = 'env-loses';

	my $node = PostgreSQL::Test::Cluster->new('guc_wins');
	$node->init;
	$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel_api,otel_postgres_tracing,test_otel_exporter'
otel_api.service_name = 'guc-wins'
log_min_messages = warning
EOCONF
	$node->start;
	$node->safe_psql('postgres',
		'CREATE EXTENSION otel_api; CREATE EXTENSION test_otel_exporter');

	my $out = $node->safe_psql('postgres',
		'SELECT test_otel_resource_attributes()');

	like($out, qr/(?:^|;)service\.name=guc-wins(?:;|$)/,
		'GUC wins over env: service.name = guc-wins');
	unlike($out, qr/(?:^|;)service\.name=env-loses(?:;|$)/,
		'GUC wins over env: env-loses not used');

	$node->stop;
}

# ----------------------------------------------------------------------
# Test 5: OTEL_RESOURCE_ATTRIBUTES service.name + extra attr.
# ----------------------------------------------------------------------
{
	local $ENV{OTEL_RESOURCE_ATTRIBUTES} =
		'service.name=ora-service,deployment.environment=dev';

	my $node = PostgreSQL::Test::Cluster->new('ora_service_name');
	$node->init;
	$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel_api,otel_postgres_tracing,test_otel_exporter'
log_min_messages = warning
EOCONF
	$node->start;
	$node->safe_psql('postgres',
		'CREATE EXTENSION otel_api; CREATE EXTENSION test_otel_exporter');

	my $out = $node->safe_psql('postgres',
		'SELECT test_otel_resource_attributes()');

	like($out, qr/(?:^|;)service\.name=ora-service(?:;|$)/,
		'OTEL_RESOURCE_ATTRIBUTES: service.name = ora-service');
	like($out, qr/(?:^|;)deployment\.environment=dev(?:;|$)/,
		'OTEL_RESOURCE_ATTRIBUTES: deployment.environment = dev');

	$node->stop;
}

# ----------------------------------------------------------------------
# Test 6: OTEL_SERVICE_NAME wins over OTEL_RESOURCE_ATTRIBUTES service.name.
# ----------------------------------------------------------------------
{
	local $ENV{OTEL_SERVICE_NAME}         = 'env-wins';
	local $ENV{OTEL_RESOURCE_ATTRIBUTES}  = 'service.name=ra-loses';

	my $node = PostgreSQL::Test::Cluster->new('env_over_ora');
	$node->init;
	$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel_api,otel_postgres_tracing,test_otel_exporter'
log_min_messages = warning
EOCONF
	$node->start;
	$node->safe_psql('postgres',
		'CREATE EXTENSION otel_api; CREATE EXTENSION test_otel_exporter');

	my $out = $node->safe_psql('postgres',
		'SELECT test_otel_resource_attributes()');

	like($out, qr/(?:^|;)service\.name=env-wins(?:;|$)/,
		'OTEL_SERVICE_NAME wins over OTEL_RESOURCE_ATTRIBUTES service.name');
	unlike($out, qr/(?:^|;)service\.name=ra-loses(?:;|$)/,
		'OTEL_RESOURCE_ATTRIBUTES service.name not used when OTEL_SERVICE_NAME set');

	$node->stop;
}

# ----------------------------------------------------------------------
# Test 7: OTEL_SERVICE_INSTANCE_ID env fallback.
# ----------------------------------------------------------------------
{
	local $ENV{OTEL_SERVICE_INSTANCE_ID} = 'env-instance-42';

	my $node = PostgreSQL::Test::Cluster->new('env_instance_id');
	$node->init;
	$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel_api,otel_postgres_tracing,test_otel_exporter'
log_min_messages = warning
EOCONF
	$node->start;
	$node->safe_psql('postgres',
		'CREATE EXTENSION otel_api; CREATE EXTENSION test_otel_exporter');

	my $out = $node->safe_psql('postgres',
		'SELECT test_otel_resource_attributes()');

	like($out, qr/(?:^|;)service\.instance\.id=env-instance-42(?:;|$)/,
		'OTEL_SERVICE_INSTANCE_ID env fallback: service.instance.id = env-instance-42');

	$node->stop;
}

# ----------------------------------------------------------------------
# Test 8: URL-decoding of values in OTEL_RESOURCE_ATTRIBUTES.
# ----------------------------------------------------------------------
{
	local $ENV{OTEL_RESOURCE_ATTRIBUTES} =
		'deployment.environment=hello%20world';

	my $node = PostgreSQL::Test::Cluster->new('url_decode');
	$node->init;
	$node->append_conf('postgresql.conf', <<EOCONF);
shared_preload_libraries = 'otel_api,otel_postgres_tracing,test_otel_exporter'
log_min_messages = warning
EOCONF
	$node->start;
	$node->safe_psql('postgres',
		'CREATE EXTENSION otel_api; CREATE EXTENSION test_otel_exporter');

	my $out = $node->safe_psql('postgres',
		'SELECT test_otel_resource_attributes()');

	like($out, qr/(?:^|;)deployment\.environment=hello world(?:;|$)/,
		'URL-decoding: %20 decoded to space in OTEL_RESOURCE_ATTRIBUTES value');

	$node->stop;
}

done_testing();
