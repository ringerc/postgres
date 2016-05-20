use strict;
use warnings;

use Config;
use PostgresNode;
use TestLib;
use Test::More;

my $node = get_new_node('main');
$node->init;
$node->start;

my $port = $node->port;

$node->stop('fast');
