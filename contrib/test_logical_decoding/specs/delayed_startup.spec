setup
{
    CREATE EXTENSION test_logical_decoding;
    CREATE TABLE do_write(id serial primary key);
}

teardown
{
    DROP TABLE do_write;
    DROP EXTENSION test_logical_decoding;
    SELECT 'stop' FROM stop_logical_replication('isolation_slot');
}

session "s1"
setup { SET synchronous_commit=on; }
step "s1b" { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step "s1w" { INSERT INTO do_write DEFAULT VALUES; }
step "s1c" { COMMIT; }
session "s2"
setup { SET synchronous_commit=on; }
step "s2init" {SELECT 'init' FROM init_logical_replication('isolation_slot', 'test_decoding');}
step "s2start" {SELECT data FROM start_logical_replication('isolation_slot', 'now', 'include-xids', 'false');}


permutation "s1b" "s1w" "s2init" "s1c" "s2start" "s1b" "s1w" "s1c" "s2start" "s1b" "s1w" "s2start" "s1c" "s2start"
