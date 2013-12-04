setup
{
    DROP TABLE IF EXISTS do_write;
    DROP EXTENSION IF EXISTS test_logical_decoding;
    CREATE EXTENSION test_logical_decoding;
    CREATE TABLE do_write(id serial primary key);
}

teardown
{
    DROP TABLE IF EXISTS do_write;
    DROP EXTENSION test_logical_decoding;
    SELECT 'stop' FROM stop_logical_replication('isolation_slot');
}

session "s0"
setup { SET synchronous_commit=on; }
step "s0init" {SELECT 'init' FROM init_logical_replication('isolation_slot', 'test_decoding');}
step "s0start" {SELECT data FROM start_logical_replication('isolation_slot', 'now', 'include-xids', 'false');}
step "s0alter" {ALTER TABLE do_write ADD column ts timestamptz; }
step "s0w" { INSERT INTO do_write DEFAULT VALUES; }

session "s1"
setup { SET synchronous_commit=on; }
step "s1begin" {BEGIN;}
step "s1sharepgclass" { SELECT count(*) > 1 FROM (SELECT * FROM pg_class FOR SHARE) s; }
step "s1keysharepgclass" { SELECT count(*) > 1 FROM (SELECT * FROM pg_class FOR KEY SHARE) s; }
step "s1commit" {COMMIT;}

session "s2"
setup { SET synchronous_commit=on; }
step "s2begin" {BEGIN;}
step "s2sharepgclass" { SELECT count(*) > 1 FROM (SELECT * FROM pg_class FOR SHARE) s; }
step "s2keysharepgclass" { SELECT count(*) > 1 FROM (SELECT * FROM pg_class FOR KEY SHARE) s; }
step "s2commit" {COMMIT;}

# test that we're handling an update-only mxact xmax correctly
permutation "s0init" "s0start" "s1begin" "s1sharepgclass" "s2begin" "s2sharepgclass" "s0w" "s0start" "s2commit" "s1commit"

# test that we're handling an update-only mxact xmax correctly
permutation "s0init" "s0start" "s1begin" "s1keysharepgclass" "s2begin" "s2keysharepgclass" "s0alter" "s0w" "s0start" "s2commit" "s1commit"
