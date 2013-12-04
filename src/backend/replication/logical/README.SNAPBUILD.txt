= Snapshot Building =
:author: Andres Freund, 2nQuadrant Ltd

== Why do we need timetravel catalog access ==

When doing WAL decoding (see DESIGN.txt for reasons to do so), we need to know
how the catalog looked at the point a record was inserted into the WAL, because
without that information we don't know much more about the record other than
its length.  It's just an arbitrary bunch of bytes without further information.
Unfortunately, due the possibility that the table definition might change we
cannot just access a newer version of the catalog and assume the table
definition continues to be the same.

If only the type information were required, it might be enough to annotate the
WAL records with a bit more information (table oid, table name, column name,
column type) --- but as we want to be able to convert the output to more useful
formats such as text, we additionally need to be able to call output functions.
Those need a normal environment including the usual caches and normal catalog
access to lookup operators, functions and other types.

Our solution to this is to add the capability to access the catalog such as it
was at the time the record was inserted into the WAL. The locking used during
WAL generation guarantees the catalog is/was in a consistent state at that
point.  We call this 'time-travel catalog access'.

Interesting cases include:

- enums
- composite types
- extension types
- non-C functions
- relfilenode to table OID mapping

Due to postgres' non-overwriting storage manager, regular modifications of a
table's content are theoretically non-destructive. The problem is that there is
no way to access an arbitrary point in time even if the data for it is there.

This module adds the capability to do so in the very limited set of
circumstances we need it in for WAL decoding. It does *not* provide a general
time-travelling facility.

A 'Snapshot' is the data structure used in postgres to describe which tuples
are visible and which are not. We need to build a Snapshot which can be used to
access the catalog the way it looked when the WAL record was inserted.

Restrictions:

- Only works for catalog tables or tables explicitly marked as such.
- Snapshot modifications are somewhat expensive
- it cannot build initial visibility information for every point in time, it
  needs a specific circumstances to start.

== How are time-travel snapshots built ==

'Hot Standby' added infrastructure to build snapshots from WAL during recovery in
the 9.0 release. Most of that can be reused for our purposes.

We cannot reuse all of the hot standby infrastructure because:

- we are not in recovery
- we need to look at interim states *inside* a transaction
- we need the capability to have multiple different snapshots around at the same time

Normally the catalog is accessed using SnapshotNow which can legally be
replaced by SnapshotMVCC that has been taken at the start of a scan. So catalog
timetravel contains infrastructure to make SnapshotNow catalog access use
appropriate MVCC snapshots. They aren't generated with GetSnapshotData()
though, but reassembled from WAL contents.

We collect our data in a normal struct SnapshotData, repurposing some fields
creatively:

- +Snapshot->xip+ contains all transaction we consider committed
- +Snapshot->subxip+ contains all transactions belonging to our transaction,
  including the toplevel one
- +Snapshot->active_count+ is used as a refcount

The meaning of +xip+ is inverted in comparison with non-timetravel snapshots in
the sense that members of the array are the committed transactions, not the in
progress ones. Because usually only a tiny percentage of committed transactions
will have modified the catalog between xmin and xmax this allows us to keep the
array small in the usual cases. It also makes subtransaction handling easier
since we neither need to query pg_subtrans (which we couldn't anyway since it's
truncated at restart) nor have problems with suboverflowed snapshots.

== Building of initial snapshot ==

We can start building an initial snapshot as soon as we find either an
+XLOG_RUNNING_XACTS+ or an +XLOG_CHECKPOINT_SHUTDOWN+ record because they allow us
to know how many transactions are running.

We need to know which transactions were running when we start to build a
snapshot/start decoding as we don't have enough information about them (they
could have done catalog modifications before we started watching). Also, we
wouldn't have the complete contents of those transactions, because we started
reading after they began.  (The latter is also important when building
snapshots that can be used to build a consistent initial clone.)

There also is the problem that +XLOG_RUNNING_XACT+ records can be
'suboverflowed' which means there were more running subtransactions than
fitting into shared memory. In that case we use the same incremental building
trick hot standby uses which is either

1. wait till further +XLOG_RUNNING_XACT+ records have a running->oldestRunningXid
after the initial xl_runnign_xacts->nextXid
2. wait for a further +XLOG_RUNNING_XACT+ that is not overflowed or
a +XLOG_CHECKPOINT_SHUTDOWN+

When we start building a snapshot we are in the +SNAPBUILD_START+ state. As
soon as we find any visibility information, even if incomplete, we change to
+SNAPBUILD_INITIAL_POINT+.

When we have collected enough information to decode any transaction starting
after that point in time we fall over to +SNAPBUILD_FULL_SNAPSHOT+. If those
transactions commit before the next state is reached, we throw their complete
contents away.

As soon as all transactions that were running when we switched over to
+SNAPBUILD_FULL_SNAPSHOT+ commit, we change state to +SNAPBUILD_CONSISTENT+.
Every transaction that commits from now on gets handed to the output plugin.
When doing the switch to +SNAPBUILD_CONSISTENT+ we optionally export a snapshot
which makes all transactions that committed up to this point visible.  This
exported snapshot can be used to run pg_dump; replaying all changes emitted
by the output plugin on a database restored from such a dump will result in
a consistent clone.

["ditaa",scaling="0.8"]
---------------

        +-------------------------+
   +----|SNAPBUILD_START          |-------------+
   |    +-------------------------+             |
   |                 |                          |
   |                 |                          |
   |     running_xacts with running xacts       |
   |                 |                          |
   |                 |                          |
   |                 v                          |
   |    +-------------------------+             v
   |    |SNAPBUILD_FULL_SNAPSHOT  |------------>|
   |    +-------------------------+             |
XLOG_RUNNING_XACTS   |                      saved snapshot
  with zero xacts    |                 at running_xacts's lsn
   |                 |                          |
   |     all running toplevel TXNs finished     |
   |                 |                          |
   |                 v                          |
   |    +-------------------------+             |
   +--->|SNAPBUILD_CONSISTENT     |<------------+
        +-------------------------+

---------------

== Snapshot Management ==

Whenever a transaction is detected as having started during decoding in
+SNAPBUILD_FULL_SNAPSHOT+ state, we distribute the currently maintained
snapshot to it (i.e. call ReorderBufferSetBaseSnapshot). This serves as its
initial snapshot. Unless there are concurrent catalog changes that snapshot
will be used for the decoding the entire transaction's changes.

Whenever a transaction-with-catalog-changes commits, we iterate over all
concurrently active transactions and add a new SnapshotNow to it
(ReorderBufferAddSnapshot(current_lsn)). This is required because any row
written from now that point on will have used the changed catalog contents.

When decoding a transaction that made catalog changes itself we tell that
transaction that (ReorderBufferAddNewCommandId(current_lsn)) which will cause
the decoding to use the appropriate command id from that point on.

SnapshotNow's need to be setup globally so the syscache and other pieces access
it transparently. This is done using two new tqual.h functions:
SetupDecodingSnapshots() and RevertFromDecodingSnapshots().

== Catalog/User Table Detection ==

Since we only want to store committed transactions that actually modified the
catalog we need a way to detect that from WAL:

Right now, we assume that every transaction that commits before we reach
+SNAPBUILD_CONSISTENT+ state has made catalog modifications since we can't rely
on having seen the entire transaction before that. That's not harmful beside
incurring some price in memory usage and runtime.

After having reached consistency we recognize catalog modifying transactions
via HEAP2_NEW_CID and HEAP_INPLACE that are logged by catalog modifying
actions.

== mixed DDL/DML transaction handling  ==

When a transactions uses DDL and DML in the same transaction things get a bit
more complicated because we need to handle CommandIds and ComboCids as we need
to use the correct version of the catalog when decoding the individual tuples.

For that we emit the new HEAP2_NEW_CID records which contain the physical tuple
location, cmin and cmax when the catalog is modified. If we need to detect
visibility of a catalog tuple that has been modified in our own transaction -
which we can detect via xmin/xmax - we look in a hash table using the location
as key to get correct cmin/cmax values.
From those values we can also extract the commandid that generated the record.

All this only needs to happen in the transaction performing the DDL.

== Cache Handling ==

As we allow usage of the normal {sys,cat,rel,..}cache we also need to integrate
cache invalidation. For transactions that only do DDL thats easy as everything
is already provided by HS. Whenever we read a commit record we apply the sinval
messages contained therein.

For transactions that contain DDL and DML cache invalidation needs to happen
more frequently because we need to all tore down all caches that just got
modified. To do that we simply apply all invalidation messages that got
collected at the end of transaction and apply them whenever we've decoded
single change. At some point this can get optimized by generating new local
invalidation messages, but that seems too complicated for now.

XXX: talk about syscache handling of relmapped relation.

== xmin Horizon Handling ==

Reusing MVCC for timetravel access has one obvious major problem: VACUUM. Rows
we still need for decoding cannot be removed but at the same time we cannot
keep data in the catalog indefinitely.

For that we peg the xmin horizon that's used to decide which rows can be
removed. We only need to prevent removal of those rows for catalog like
relations, not for all user tables. For that reason a separate xmin horizon
RecentGlobalDataXmin got introduced.

Since we need to persist that knowledge across restarts we keep the xmin for a
in the logical slots which are safed in a crashsafe manner. They are restored
from disk into memory at server startup.

== Restartable Decoding ==

As we want to generate a consistent stream of changes we need to have the
ability to start from a previously decoded location without waiting possibly
very long to reach consistency. For that reason we dump the current visibility
information to disk whenever we read an xl_running_xacts record.
