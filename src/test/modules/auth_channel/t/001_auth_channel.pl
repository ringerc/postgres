
# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Protocol-level authorization channel TAP test (irrevocable-privilege-drop
# Phase 4 / design §16).  Speaks the wire protocol via a raw socket so the
# new V / e / U / b frontend tags and Y backend tag can be exercised without
# libpq client-side support.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use IO::Socket::UNIX;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

$node->safe_psql(
	'postgres', q[
		CREATE ROLE regress_authch_low NOSUPERUSER NOINHERIT;
		GRANT regress_authch_low TO CURRENT_USER;
	]);

# Resolve the Unix socket directory from the cluster.  PostgreSQL::Test::Cluster
# uses Unix sockets by default on non-Windows; host() returns the socket dir.
my $sock_dir = $node->host;
my $port = $node->port;
my $sock_path = "$sock_dir/.s.PGSQL.$port";

note "sock_dir=$sock_dir port=$port sock_path=$sock_path";

sub raw_connect
{
	my $sock = IO::Socket::UNIX->new(
		Type => SOCK_STREAM(),
		Peer => $sock_path,
	);
	defined $sock or die "connect failed for $sock_path: $!";
	$sock->autoflush(1);
	binmode $sock;
	return $sock;
}

sub raw_send
{
	my ($sock, $bytes) = @_;
	my $sent = syswrite($sock, $bytes);
	unless (defined $sent && $sent == length($bytes))
	{
		my $err = $!;
		my $got = defined $sent ? $sent : 'undef';
		die "syswrite returned $got/" . length($bytes) . " err=$err";
	}
}

sub raw_read_exact
{
	my ($sock, $n) = @_;
	my $buf = '';
	while (length($buf) < $n)
	{
		my $r = sysread($sock, my $chunk, $n - length($buf));
		die "sysread failed: $!" unless defined $r;
		last if $r == 0;
		$buf .= $chunk;
	}
	die "short read: " . length($buf) . "/$n" if length($buf) != $n;
	return $buf;
}

sub recv_msg
{
	my ($sock) = @_;
	my $hdr = '';
	my $r = sysread($sock, $hdr, 5);
	return (undef, undef) unless defined $r && $r == 5;
	my ($tag, $len) = unpack('a N', $hdr);
	my $payload = '';
	my $rem = $len - 4;
	$payload = raw_read_exact($sock, $rem) if $rem > 0;
	return ($tag, $payload);
}

sub send_startup
{
	my ($sock, $user, $db, %opts) = @_;
	my $params = '';
	$params .= "user\0$user\0";
	$params .= "database\0$db\0";
	$params .= "_pq_.auth_channel\0" . "1\0" if $opts{auth_channel};
	$params .= "\0";
	my $body = pack('N', 196608) . $params;
	my $pkt = pack('N', 4 + length($body)) . $body;
	raw_send($sock, $pkt);
}

sub drain_until_ready
{
	my ($sock) = @_;
	my @msgs;
	while (1)
	{
		my ($tag, $payload) = recv_msg($sock);
		last unless defined $tag;
		push @msgs, [ $tag, $payload ];
		last if $tag eq 'Z';
	}
	return \@msgs;
}

sub build_auth_set_role
{
	my ($role_name, $kind, $sas) = @_;
	my $payload = pack('C', $kind) . $role_name . "\0" . pack('N', 0);
	my $tag = $sas ? 'e' : 'V';
	return $tag . pack('N', 4 + length($payload)) . $payload;
}

sub build_auth_reset_role
{
	my ($cookie, $sas) = @_;
	my $payload;
	if (defined $cookie)
	{
		$payload = pack('C', 1) . pack('N', length($cookie)) . $cookie;
	}
	else
	{
		$payload = pack('C', 0);
	}
	my $tag = $sas ? 'b' : 'U';
	return $tag . pack('N', 4 + length($payload)) . $payload;
}

# Parse the AuthLockResponse 'Y' payload:
#   Int8 status, Int32 cookie_len, [ByteN cookie], Cstring message
sub parse_auth_lock_response
{
	my ($payload) = @_;
	my ($status, $cookie_len, $rest) = unpack('C N a*', $payload);
	my $cookie;
	if ($cookie_len > 0)
	{
		$cookie = substr($rest, 0, $cookie_len);
		$rest = substr($rest, $cookie_len);
	}
	my ($message) = $rest =~ /^(.*?)\0/s;
	$message //= '';
	return ($status, $cookie, $message);
}

sub auth_response_from_msgs
{
	my ($msgs) = @_;
	for my $m (@$msgs)
	{
		return parse_auth_lock_response($m->[1]) if $m->[0] eq 'Y';
	}
	return (undef, undef, undef);
}

my $user = $node->safe_psql('postgres', 'SELECT current_user');

# ============================================================
# Test 1: AuthSetRole IRREVOCABLE, then verify via Q.
# ============================================================
{
	my $sock = raw_connect();
	send_startup($sock, $user, 'postgres', auth_channel => 1);
	drain_until_ready($sock);

	raw_send($sock, build_auth_set_role('regress_authch_low', 1, 0));
	my $msgs = drain_until_ready($sock);
	my ($status, $cookie, $message) = auth_response_from_msgs($msgs);
	is($status, 0, 'Test 1: AuthSetRole IRREVOCABLE returns OK');

	# Verify session is at the locked role.
	my $q = "Q" . pack('N', 4 + length("SELECT current_user;\0")) . "SELECT current_user;\0";
	raw_send($sock, $q);
	my $q_msgs = drain_until_ready($sock);
	my $datarow_payload;
	for my $m (@$q_msgs)
	{
		$datarow_payload = $m->[1] if $m->[0] eq 'D';
	}
	# DataRow: Int16 nfields, then per field: Int32 len + ByteN value
	my ($nfields, $rest) = unpack('n a*', $datarow_payload // '');
	my ($val_len, $val) = unpack('N a*', $rest // '');
	my $current_user_value = substr($val, 0, $val_len);
	is($current_user_value, 'regress_authch_low',
		'Test 1: session role is the locked role after AuthSetRole');
	close $sock;
}

# ============================================================
# Test 2: AuthSetRole WITH COOKIE, then AuthResetRole with that cookie.
# ============================================================
{
	my $sock = raw_connect();
	send_startup($sock, $user, 'postgres', auth_channel => 1);
	drain_until_ready($sock);

	raw_send($sock, build_auth_set_role('regress_authch_low', 2, 0));
	my $set_msgs = drain_until_ready($sock);
	my ($set_status, $cookie, undef) = auth_response_from_msgs($set_msgs);
	is($set_status, 1, 'Test 2: AuthSetRole WITH COOKIE returns OK_COOKIE');
	is(length($cookie // ''), 32, 'Test 2: cookie is 32 bytes');

	raw_send($sock, build_auth_reset_role($cookie, 0));
	my $reset_msgs = drain_until_ready($sock);
	my ($reset_status, undef, undef) = auth_response_from_msgs($reset_msgs);
	is($reset_status, 0, 'Test 2: AuthResetRole with valid cookie returns OK');
	close $sock;
}

# ============================================================
# Test 3: AuthSetRole without _pq_.auth_channel negotiation -> UNAVAILABLE.
# ============================================================
{
	my $sock = raw_connect();
	send_startup($sock, $user, 'postgres', auth_channel => 0);
	drain_until_ready($sock);

	raw_send($sock, build_auth_set_role('regress_authch_low', 1, 0));
	my $msgs = drain_until_ready($sock);
	my ($status, undef, $message) = auth_response_from_msgs($msgs);
	is($status, 6, 'Test 3: AuthSetRole without negotiation returns UNAVAILABLE');
	like($message, qr/auth_channel not negotiated/,
		'Test 3: UNAVAILABLE message points at startup parameter');
	close $sock;
}

# ============================================================
# Test 4: AuthResetRole with no cookie present -> LOCK_PROTECTED.
# ============================================================
{
	my $sock = raw_connect();
	send_startup($sock, $user, 'postgres', auth_channel => 1);
	drain_until_ready($sock);

	# Install a lock first.
	raw_send($sock, build_auth_set_role('regress_authch_low', 1, 0));
	drain_until_ready($sock);

	# Now try AuthResetRole with no cookie.
	raw_send($sock, build_auth_reset_role(undef, 0));
	my $msgs = drain_until_ready($sock);
	my ($status, undef, $message) = auth_response_from_msgs($msgs);
	is($status, 2, 'Test 4: AuthResetRole without cookie returns LOCK_PROTECTED');
	close $sock;
}

# ============================================================
# Test 5: AuthSetSessionAuthorization (e tag, sas variant).
# ============================================================
{
	my $sock = raw_connect();
	send_startup($sock, $user, 'postgres', auth_channel => 1);
	drain_until_ready($sock);

	raw_send($sock, build_auth_set_role('regress_authch_low', 1, 1));
	my $msgs = drain_until_ready($sock);
	my ($status, undef, undef) = auth_response_from_msgs($msgs);
	is($status, 0, 'Test 5: AuthSetSessionAuthorization IRREVOCABLE returns OK');
	close $sock;
}

# ============================================================
# Test 6: Bad cookie returns BAD_COOKIE / error.
# ============================================================
{
	my $sock = raw_connect();
	send_startup($sock, $user, 'postgres', auth_channel => 1);
	drain_until_ready($sock);

	# Install a cookie lock.
	raw_send($sock, build_auth_set_role('regress_authch_low', 2, 0));
	my $set_msgs = drain_until_ready($sock);
	my ($set_status, $valid_cookie, undef) = auth_response_from_msgs($set_msgs);
	is($set_status, 1, 'Test 6: cookie lock installed');

	# Present a 32-byte cookie that's all zeros (won't match the real hash).
	my $wrong = "\0" x 32;
	raw_send($sock, build_auth_reset_role($wrong, 0));
	my $reset_msgs = drain_until_ready($sock);
	# The reset SQL function ereports on mismatch; we'll receive an ErrorResponse
	# 'E' rather than a 'Y' AuthLockResponse.
	my $got_error;
	for my $m (@$reset_msgs)
	{
		$got_error = 1 if $m->[0] eq 'E';
	}
	ok($got_error, 'Test 6: wrong cookie produces ErrorResponse');

	# Lock should still be in effect.
	raw_send($sock, build_auth_reset_role($valid_cookie, 0));
	my $cleanup_msgs = drain_until_ready($sock);
	my ($cleanup_status, undef, undef) = auth_response_from_msgs($cleanup_msgs);
	is($cleanup_status, 0,
		'Test 6: valid cookie still works after a wrong-cookie attempt');
	close $sock;
}

$node->stop;
done_testing();
