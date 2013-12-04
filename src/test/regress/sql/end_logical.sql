\copy (SELECT data FROM start_logical_replication('regression_slot', 'now', 'include-xids', '0')) TO '/tmp/testresult';
SELECT stop_logical_replication('regression_slot');
