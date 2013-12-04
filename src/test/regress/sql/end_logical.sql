\copy (SELECT data FROM decoding_slot_get_changes('regression_slot', 'now', 'include-xids', '0')) TO '/tmp/testresult';
SELECT drop_replication_slot('regression_slot');
