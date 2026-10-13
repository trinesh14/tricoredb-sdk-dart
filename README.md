# tricoredb-sdk-dart

Official Dart client for [TriCoreDB](https://hub.docker.com/r/trinesh14/tricoredb):
SQL, documents, vectors, graphs and cache over one native connection.

[![pub package](https://img.shields.io/pub/v/tricoredb.svg?cacheSeconds=86400)](https://pub.dev/packages/tricoredb)
[![Dart SDK](https://img.shields.io/badge/dart-%3E%3D3.3-blue?logo=dart&cacheSeconds=86400)](https://dart.dev)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue?cacheSeconds=86400)](LICENSE)

- **No dependencies.** `dart:io` and `dart:convert` are all it needs, TLS included.
- **Everything is `async`.** Nothing blocks the isolate waiting for the server.
- **Server-side parameters.** Values never become part of the SQL text.
- **Transactions, a connection pool, TLS and mutual TLS.**

## Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Running a server](#running-a-server)
- [Quick start](#quick-start)
- [Connecting](#connecting)
- [SQL](#sql)
- [Parameters](#parameters)
- [Transactions](#transactions)
- [Connection pool](#connection-pool)
- [Cache](#cache)
- [Documents](#documents)
- [Vectors](#vectors)
- [Graphs](#graphs)
- [Operations added for server 0.1.0-rc.3](#operations-added-for-server-010-rc3)
- [LLM context](#llm-context)
- [Admin](#admin)
- [Errors](#errors)
- [TLS](#tls)
- [Testing](#testing)

## Requirements

- Dart **3.3** or later (Flutter 3.19+), on any platform with `dart:io` —
  server, desktop, CLI and mobile. Not the web: a database connection is a
  socket, and a browser has none.
- A TriCoreDB server speaking protocol 1.0 (`tricore-server` 0.1.0-rc.1 or
  later; 0.1.0-rc.3 or later for the
  [operations added for it](#operations-added-for-server-010-rc3)). See [Running a server](#running-a-server).

## Installation

```bash
dart pub add tricoredb
```

or, in `pubspec.yaml`:

```yaml
dependencies:
  tricoredb: ^0.3.0
```

## Running a server

```bash
docker run --rm -p 8427:8427 trinesh14/tricoredb:0.1.0-rc.1-r2
```

The image listens on 8427 and speaks the `tricore` protocol this package uses.
For credentials and configuration, see the image's own documentation.

## Quick start

```dart
import 'package:tricoredb/tricoredb.dart';

Future<void> main() async {
  final db = await TriCore.connect(
    host: '127.0.0.1',
    port: 8427,
    user: 'admin',
    secret: 'secret',
  );

  await db.execute(
    'CREATE TABLE people (id INT PRIMARY KEY, name TEXT, score DOUBLE)',
  );
  await db.execute('INSERT INTO people VALUES (?, ?, ?)', [1, 'ada', 9.5]);

  final rows = await db.query(
    'SELECT name, score FROM people WHERE id = ?',
    [1],
  );
  print(rows.value(0, 'name')); // ada

  await db.close();
}
```

## Connecting

```dart
final db = await TriCore.connect(
  host: '127.0.0.1',
  port: 8427,
  user: 'admin',
  secret: Platform.environment['TRICORE_SECRET']!,
  database: 'main',
  connectTimeout: const Duration(seconds: 10),
  readTimeout: const Duration(seconds: 30),
  requestTimeout: const Duration(seconds: 5),
);
```

`connectTimeout` covers the TCP connect, the TLS handshake and the login.
`readTimeout` bounds each reply afterwards: when it fires the connection is
closed, because a late reply would otherwise be read as the answer to the next
request. `requestTimeout` is a deadline the **server** applies.

### Following the leader

In a cluster only the leader accepts writes, and a follower refuses with
`not_leader`. The client handles that itself (`followLeader: true`, the
default):

- when the refusal names the leader, it opens a connection there with the same
  user, secret and TLS settings (the leader's host is the certificate name),
  sends the request again, and keeps that connection (`db.address` reports it);
- while an election is in progress (no leader named), it waits and asks the same
  node again, pausing 250 ms, then 500 ms, up to 2 s (`leaderRetryDelay`);
- after `leaderRetries` (5) it throws the last refusal.

This is safe because a `not_leader` refusal was applied nowhere. Nothing is sent
again inside a session transaction, and never a refusal with
`outcomeUnknownCode`: a follower handed that request to the leader and lost the
answer, or the leader could not confirm it, so it may have been applied. The
leader's address must be reachable from your application; when it is not, the
client keeps its connection and waits. `TriCorePool.to` takes the same three
options. Pass `followLeader: false` to handle redirects yourself.

A connection is a single request/response stream. Calls made from several places
at once are queued, so they are safe but not concurrent — for concurrency, use a
[pool](#connection-pool).

The handshake negotiates capabilities:

```dart
db.grantedFeatures.serverParams; // ? placeholders bound by the server
db.grantedFeatures.sessionTxn;   // begin/commit as separate requests
```

A call that needs a capability the server did not grant fails **before anything
is sent**, rather than falling back to something weaker.

## SQL

```dart
final rows = await db.query('SELECT id, name FROM people WHERE score > ?', [8]);

rows.length;           // how many rows
rows[0][1];            // by position
rows.value(0, 'name'); // by column name
rows.toMaps();         // [{id: '1', name: 'ada'}]
for (final row in rows) { /* … */ }
```

Cells are the server's own text rendering of each value, so a number arrives as
its digits and a SQL `NULL` arrives as the text `NULL`.

Writes go through `execute`, which reports how many rows changed:

```dart
final result = await db.execute(
  'UPDATE people SET score = ? WHERE id = ?',
  [9.9, 1],
);
result.rowsAffected; // 1
```

## Parameters

Values are bound by the server. This driver never renders them into the
statement text, because escaping and binding are not the same guarantee:

```dart
await db.query(
  'SELECT id FROM people WHERE name = ?',
  ["ada'; DROP TABLE people; --"],
);
// finds nothing, drops nothing
```

| Dart | On the wire |
| --- | --- |
| `null`, `bool`, `int` | themselves |
| `double` | a JSON number; NaN and the infinities are refused |
| `String` | UTF-8 text, always — never bytes |
| `Blob`, `Uint8List` | `0x…` hex, for a BLOB column |
| `Decimal` | plain digits, so nothing is rounded |
| `BigInt` | text, for integers beyond a double |
| `DateTime` | ISO-8601 |

Anything else is refused by name, with the parameter's position, rather than
pushed through `toString()`.

## Transactions

```dart
await db.transaction((tx) async {
  await tx.execute(
    'UPDATE accounts SET balance = balance - ? WHERE id = ?',
    [100, 1],
  );
  await tx.execute(
    'UPDATE accounts SET balance = balance + ? WHERE id = ?',
    [100, 2],
  );
});
```

If the block throws, the transaction is rolled back and the original error is
rethrown. `begin`, `commit` and `rollback` are available separately, and
`db.inTransaction` says where you are.

Session transactions need the `SESSION_TXN` capability. Without it, send a whole
script in one request instead:

```dart
await db.execute('BEGIN; UPDATE …; COMMIT;');
```

## Connection pool

```dart
final pool = TriCorePool.to(
  host: '127.0.0.1',
  user: 'admin',
  secret: 'secret',
  size: 8,
  checkoutTimeout: const Duration(seconds: 5),
);

final rows = await pool.withConnection((db) => db.query('SELECT 1'));
await pool.close();
```

A connection goes back to the pool when the callback ends, whether it returned
or threw. A transaction left open is rolled back before the connection is lent
to anyone else.

## Cache

Cache values are bytes; pass a `String` and its UTF-8 bytes are sent.

```dart
await db.cacheSet('sessions', 'abc', 'ada', ttl: const Duration(minutes: 30));
await db.cacheGetString('sessions', 'abc'); // 'ada'
await db.cacheGet('sessions', 'abc');       // Uint8List, or null on a miss
```

A miss is `null`, which is how it is told apart from a stored empty value.
Lists, sets, hashes and streams are there too — `cacheRightPush`, `cacheSetAdd`,
`cacheHashSet`, `cacheStreamAdd` and the rest.

## Documents

```dart
await db.documentCreateCollection('people');
await db.documentInsert('people', {'name': 'ada', 'city': 'Pune', 'visits': 5});

final found = await db.documentFind(
  'people',
  filter: Filter.allOf([Filter.eq('city', 'Pune'), Filter.gt('visits', 4)]),
);

final totals = await db.documentAggregate('people', [
  Stage.group(Stage.byField('city'), [Acc.sum('total', 'visits')]),
  Stage.sort(['total'], descending: ['total']),
]);
```

## Vectors

```dart
await db.vectorCreateCollection('embeddings', dimension: 3);
await db.vectorUpsert('embeddings', 'a', [1, 0, 0], metadata: {'kind': 'doc'});

final nearest = await db.vectorSearch('embeddings', [1, 0, 0], topK: 5);
nearest.first.id;    // 'a'
nearest.first.score; // higher is closer, for every metric
```

## Graphs

```dart
await db.graphCreate('social');
await db.graphAddNode(
  'social',
  'n1',
  labels: ['Person'],
  properties: {'name': 'ada'},
);
await db.graphAddEdge('social', 'e1', from: 'n1', to: 'n2', label: 'KNOWS');

await db.graphNeighbors('social', 'n1');
await db.graphShortestPath('social', from: 'n1', to: 'n3');
```

"No path" comes back as `found: false` — an answer, not a failure.

## Operations added for server 0.1.0-rc.3

These need a server at **0.1.0-rc.3 or later**. An older server does not know the
operation and answers it with an error, which this client raises like any other
refusal.

### Cache

| Method | Server operation | What it does |
| --- | --- | --- |
| `cacheSetFresh` | `Cache.SetFresh` | Store a value that answers as fresh for the given time and as stale after it, until its TTL (if any) ends |
| `cacheGetFresh` | `Cache.GetFresh` | Read with stale-while-revalidate and stampede protection |
| `cacheSetMissing` | `Cache.SetMissing` | Negative caching |
| `cacheWarm` | `Cache.Warm` | Cache warming |
| `cacheGetMany` | `Cache.MGet` | Read many values in one request |
| `cacheSetMany` | `Cache.MSet` | Write many values as one batch, all with the same optional TTL |
| `cacheSetIf` | `Cache.SetIf` | Write a value under a condition, with explicit control over the TTL |
| `cacheSortedAdd` | `Cache.ZAdd` | Add scored members to a sorted set, or re-score ones already there |
| `cacheSortedRemove` | `Cache.ZRem` | Remove members from a sorted set, returning how many were present |
| `cacheSortedScore` | `Cache.ZScore` | The score of one member, or null when it is not in the set |
| `cacheSortedCount` | `Cache.ZCard` | How many members a sorted set holds |
| `cacheSortedRank` | `Cache.ZRank` | A member's position in the ordering, or null when it is absent |
| `cacheSortedRange` | `Cache.ZRange` | Members by rank, `start..=stop` inclusive |
| `cacheSortedRangeByScore` | `Cache.ZRangeByScore` | Members by score, between two bounds |
| `cacheSortedIncrement` | `Cache.ZIncrBy` | Add to a member's score, creating it at that score when absent |
| `cacheLockAcquire` | `Cache.LockAcquire` | Take a lease on a lock, or refresh one already held by `owner` |
| `cacheLockRenew` | `Cache.LockRenew` | Extend a lease the caller still holds |
| `cacheLockRelease` | `Cache.LockRelease` | Give up a lock |
| `cacheSetBit` | `Cache.SetBit` | Set one bit of a string value, growing it with zero bytes if needed |
| `cacheGetBit` | `Cache.GetBit` | Read one bit |
| `cacheBitCount` | `Cache.BitCount` | Count the set bits, optionally within a byte range `(start, end)` with signed indices |
| `cacheBitPosition` | `Cache.BitPos` | The position of the first bit equal to `bit`, optionally within a byte range, or -1 when there is none |
| `cacheBitOperation` | `Cache.BitOp` | Combine several values bitwise and store the result at `dest` |
| `cacheHyperLogLogAdd` | `Cache.PfAdd` | Add elements to a HyperLogLog, creating it when absent |
| `cacheHyperLogLogCount` | `Cache.PfCount` | Estimated number of distinct elements |
| `cacheHyperLogLogMerge` | `Cache.PfMerge` | Merge several HyperLogLogs into `dest`, which is included as a source when it already exists |
| `cacheListMove` | `Cache.LMove` | Atomically pop an element from one end of `source` and push it onto one end of `destination` |
| `cacheExpireAt` | `Cache.ExpireAt` | Set the absolute instant an existing live key dies |
| `cacheStats` | `Cache.Stats` | What this node's cache is doing |
| `cacheFlushDatabase` | `Cache.FlushDatabase` | Delete every cache key in this database, across every namespace |
| `cacheAppend` | `Cache.Append` | Concatenate bytes onto what a key already holds, and return the new length |
| `cacheCompareAndSwap` | `Cache.CompareAndSwap` | Write only if the key still holds `expected` (null |
| `cacheGetAndDelete` | `Cache.GetDel` | Read a key and delete it in one atomic step |
| `cacheGetAndSet` | `Cache.GetSet` | Write a value and return the one it replaced, atomically |
| `cacheGetAndExpire` | `Cache.GetEx` | Read a key and change its expiry in the same atomic step |
| `cacheListTrim` | `Cache.LTrim` | Keep only the inclusive index range of a list, dropping everything outside it |
| `cacheSetUnion` | `Cache.SUnion` | Every member of every named set, without duplicates |
| `cacheSetIntersection` | `Cache.SInter` | The members present in every named set |
| `cacheSetDifference` | `Cache.SDiff` | The members of the first set that are in none of the rest |
| `cacheSetRandomMembers` | `Cache.SRandMember` | Random members of a set, without removing them |
| `cacheHashIncrement` | `Cache.HIncrBy` | Add to one hash field, atomically, returning its new value |
| `cachePipeline` | `Cache.Pipeline` | Run cache operations in order in one round trip, each exactly as if it had been sent alone |
| `cacheTransaction` | `Cache.Transaction` | Run cache operations in order holding the cache write lock, so no other writer interleaves with them |
| `cacheScriptLoad` | `Cache.ScriptLoad` | Store a script |
| `cacheScriptRun` | `Cache.ScriptRun` | Run a stored script with these arguments as one transaction |
| `cacheHotKeys` | `Cache.HotKeys` | The most frequently addressed keys on this node since it started, with estimated counts |
| `cachePurgeExpired` | `Cache.PurgeExpired` | Delete every expired cache key in this database now, rather than leaving it to compaction |
| `cacheBlockingLeftPop` | `Cache.BLPop` | Pop the head of the first non-empty list among `keys`, waiting up to the timeout for one to receive an element |
| `cacheBlockingRightPop` | `Cache.BRPop` | Pop the tail of the first non-empty list among `keys`, waiting up to the timeout for one to receive an element |
| `cacheSetNamespaceLimit` | `Cache.SetNamespaceLimit` | Cap a namespace at `max_keys` live keys and `max_bytes` of values |
| `cacheGetNamespaceLimit` | `Cache.GetNamespaceLimit` | A namespace's limit and what it holds now |
| `cachePublish` | `Cache.Publish` | Append a message to a channel, returning the id it was given |
| `cachePoll` | `Cache.Poll` | The messages after id `after` on these channels (a trailing `*` is a pattern) |

### Documents

| Method | Server operation | What it does |
| --- | --- | --- |
| `documentReplace` | `Document.Replace` | Swap a document's whole body for a new one |
| `documentInsertMany` | `Document.InsertMany` | Insert several documents as one atomic batch |
| `documentDeleteMany` | `Document.DeleteMany` | Delete every document a filter matches, returning how many went |
| `documentRenameCollection` | `Document.RenameCollection` | Move a collection, with its documents and indexes, to a new name |
| `documentPurgeExpired` | `Document.PurgeExpired` | Delete the documents a TTL index says have expired, now |
| `documentExplain` | `Document.Explain` | Run a find and describe how it ran instead of returning its documents |
| `documentBeginTransaction` | `Document.BeginTransaction` | Open a document transaction spanning requests, returning its id |
| `documentCommitTransaction` | `Document.CommitTransaction` | Commit a document transaction |
| `documentRollbackTransaction` | `Document.RollbackTransaction` | Discard a document transaction |
| `documentRebuildIndex` | `Document.RebuildIndex` | Rebuild one index from the stored documents |
| `documentSetCollectionOptions` | `Document.SetCollectionOptions` | Set a collection's options (`schema`, `defaults`, `versioned`), replacing any it had |
| `documentGetCollectionOptions` | `Document.GetCollectionOptions` | A collection's options |
| `documentBulkWrite` | `Document.BulkWrite` | Many inserts, updates, replaces and deletes on one collection, applied in order as one atomic batch |

### Vectors

| Method | Server operation | What it does |
| --- | --- | --- |
| `vectorEmbed` | `Vector.Embed` | Embed texts with the server's built-in embedder (deterministic feature hashing |
| `vectorUpsertText` | `Vector.UpsertText` | Embed each text at the collection's dimension and upsert the vectors as one atomic batch |
| `vectorSetAlias` | `Vector.SetAlias` | Point an alias at a collection; a null collection removes it |
| `vectorListAliases` | `Vector.ListAliases` | Every alias and the collection it names, as `{alias, collection}` |
| `vectorAddNamedVector` | `Vector.AddNamedVector` | Declare a named vector on a collection |
| `vectorUpsertNamed` | `Vector.UpsertNamed` | Write an item's vectors by name (`default` is the collection's own), each with the same metadata |
| `vectorUpsertMany` | `Vector.UpsertMany` | Write many vectors as one atomic batch |
| `vectorDeleteMany` | `Vector.DeleteMany` | Delete many vectors by id as one atomic batch, returning how many of the ids were present |
| `vectorSearchBatch` | `Vector.SearchBatch` | Run several query vectors against one collection in one request |
| `vectorDeleteByFilter` | `Vector.DeleteByFilter` | Delete every vector whose metadata matches a filter, as one atomic batch |
| `vectorUpdateByFilter` | `Vector.UpdateByFilter` | Merge `set` into the metadata of every vector matching a filter, as one atomic batch |
| `vectorCount` | `Vector.Count` | How many vectors a collection holds, or, with a filter, how many match it |
| `vectorListPartitions` | `Vector.ListPartitions` | The partitions of a collection |
| `vectorRenameCollection` | `Vector.RenameCollection` | Rename a collection, moving its catalog record and every vector with it |
| `vectorRebuildIndex` | `Vector.RebuildIndex` | Rebuild the collection's ANN index from its stored vectors and persist it, clearing every tombstone |
| `vectorIndexStats` | `Vector.IndexStats` | The shape of the collection's ANN index |
| `vectorMeasureRecall` | `Vector.MeasureRecall` | Measure the ANN index's recall |
| `vectorSearchMulti` | `Vector.SearchMulti` | Search with several query vectors and return one fused ranking |
| `vectorSetCollectionConfig` | `Vector.SetCollectionConfig` | Change a collection's configuration, returning it |
| `vectorCopyCollection` | `Vector.CopyCollection` | Copy a collection (configuration and every vector) to a new name |
| `vectorSearchByExamples` | `Vector.SearchByExamples` | Search with a query built from examples |

### Graphs

| Method | Server operation | What it does |
| --- | --- | --- |
| `graphAddMany` | `Graph.AddMany` | Write many nodes and edges as one unit |
| `graphRemoveMany` | `Graph.RemoveMany` | Delete many nodes and edges as one unit |
| `graphUpdateNode` | `Graph.UpdateNode` | Change an existing node in place, returning it |
| `graphUpdateEdge` | `Graph.UpdateEdge` | Change an existing edge in place, returning it |
| `graphCopy` | `Graph.CopyGraph` | Copy a graph (nodes, edges and schema) to a new name |
| `graphDfs` | `Graph.Dfs` | A bounded depth-first walk from a node |
| `graphConnectedComponents` | `Graph.ConnectedComponents` | Partition the graph's nodes into connected components, largest first |
| `graphDetectCycle` | `Graph.DetectCycle` | Whether the graph contains a cycle (`hasCycle`), and one `cycle` as evidence |
| `graphStats` | `Graph.GraphStats` | Counts describing a graph |
| `graphSubgraph` | `Graph.Subgraph` | The part of the graph within `max_depth` hops of the seeds |
| `graphSchema` | `Graph.GraphSchema` | What the graph's data looks like |
| `graphKShortestPaths` | `Graph.KShortestPaths` | The `k` cheapest loopless paths between two nodes, cheapest first (default 3, at most 20) |
| `graphAllPaths` | `Graph.AllPaths` | Every simple path between two nodes of at most `max_depth` edges (default 5, at most 10) |
| `graphCentrality` | `Graph.Centrality` | Betweenness, closeness or eigenvector centrality |
| `graphCommunities` | `Graph.Communities` | Communities by Louvain modularity optimisation, direction ignored, largest first, with the partition's `modularity` |
| `graphTriangleCount` | `Graph.TriangleCount` | Triangles and clustering coefficients, direction ignored |
| `graphNodeSimilarity` | `Graph.NodeSimilarity` | The nodes whose neighbour sets most resemble a node's, under `results` |
| `graphCheckConsistency` | `Graph.CheckConsistency` | Check a graph's stored records against each other and against the resident mirror |
| `graphCreateNodeIndex` | `Graph.CreateNodeIndex` | Index node properties for finding nodes |
| `graphCreateConstraint` | `Graph.CreateConstraint` | Enforce a rule on a property of every node (or, with target `edge`, every relationship) carrying a label |
| `graphDropSchemaObject` | `Graph.DropSchemaObject` | Drop an index or a constraint by name |
| `graphListSchemaObjects` | `Graph.ListSchemaObjects` | A graph's `indexes` and `constraints` |
| `graphFindNodes` | `Graph.FindNodes` | Nodes with a label (when given) whose properties equal the given values, in id order |
| `graphImportCsv` | `Graph.ImportCsv` | Import nodes and edges from CSV text as one atomic batch, so constraints apply |
| `graphPageRank` | `Graph.PageRank` | PageRank over edge direction |
| `graphExecute` | `Graph.Execute` | Run a Cypher write |
| `graphBeginTransaction` | `Graph.BeginTransaction` | Open a transaction on a graph, returning its id |
| `graphCommitTransaction` | `Graph.CommitTransaction` | Apply every statement queued in a graph transaction as one |
| `graphRollbackTransaction` | `Graph.RollbackTransaction` | Discard a graph transaction and its queued statements |
`documentBulkWrite` takes its operations from `BulkOp` (`insert`, `updateOne`,
`replace`, `delete`), and a cache pipeline, transaction or script takes its from
`CacheOp`:

```dart
await db.documentBulkWrite('people', [
  BulkOp.insert({'name': 'ada'}, id: 'a'),
  BulkOp.updateOne('a', set: {'city': 'Pune'}),
  BulkOp.delete('b'),
]); // every operation lands, or none does

final results = await db.cachePipeline([
  CacheOp.set('sessions', 'abc', 'ada'),
  CacheOp.get('sessions', 'abc'),
]);
results[1].value; // the bytes the second operation read
```

A document transaction is named in each call that runs inside it, and a graph
transaction queues Cypher writes until it commits:

```dart
final txn = await db.documentBeginTransaction();
await db.documentInsert('people', {'name': 'ada'}, id: 'a', txn: txn);
await db.documentCommitTransaction(txn);

final graphTxn = await db.graphBeginTransaction('social');
await db.graphExecute('social', "CREATE (n:Person {name: 'ada'})", txn: graphTxn);
await db.graphCommitTransaction('social', graphTxn);
```

A result with one meaningful field comes back as that value; a larger one comes
back as the server's own JSON object, with the server's field names.

## LLM context

```dart
final schema = await db.llmSchema(); // TOON text
final bundle = await db.llmContext([
  LlmSource.sql('SELECT id, name FROM people'),
  LlmSource.documents('notes', limit: 50),
]);
```

## Admin

```dart
await db.adminPing();   // a round trip through the whole pipeline
await db.adminStatus(); // what the server says about itself
await db.ping();        // the socket alone
```

## Errors

Every failure is a `TriCoreException`. Branch on `code`, never on the message:

```dart
try {
  await db.execute('INSERT INTO t VALUES (1)');
} on ServerException catch (e) {
  if (e.isRedirect) {
    // No node would serve it, even after following the leader (or
    // followLeader is off). e.leaderHint names the leader when the cluster
    // knows one.
    print(e.leaderHint);
  }
} on ConnectionException {
  // the socket failed or timed out; this connection is closed
} on FeatureNotGrantedException catch (e) {
  // a capability the handshake did not grant — nothing was sent
  print(e.feature);
}
```

| Exception | What happened |
| --- | --- |
| `ServerException` | the server processed the request and refused it |
| `AuthException` | the credentials were refused |
| `ProtocolException` | the byte stream can no longer be trusted |
| `ConnectionException` | the socket failed or was closed |
| `ReadTimeoutException` | no reply within the read timeout |
| `FeatureNotGrantedException` | a capability the server did not grant |
| `ParameterException` | a value with no SQL form |
| `PoolTimeoutException` | no connection became free in time |

## TLS

```dart
final db = await TriCore.connect(
  host: 'db.example.com',
  user: 'admin',
  secret: 'secret',
  tls: const TlsOptions(caFile: '/etc/tricore/ca.pem'),
);
```

Mutual TLS needs both halves of the identity:

```dart
tls: const TlsOptions(
  caFile: '/etc/tricore/ca.pem',
  clientCertificateFile: '/etc/tricore/client.pem',
  clientKeyFile: '/etc/tricore/client.key',
),
```

`dangerAcceptInvalidCertificates: true` turns off the guarantee TLS exists to
provide. It is for a development server with a self-signed certificate, and
errors name a file's path, never its contents.

## Testing

```bash
dart test
```

The unit tests and the scripted-peer tests need no server: a peer built on
`dart:io` plays the answers a real cluster would send, including a `not_leader`
refusal and a frame that declares more bytes than it sends. The live tests start
their own `tricore-server` — point `TRICORE_SERVER_BIN` at the binary — or use
one that is already running, named by `TRICORE_HOST`, `TRICORE_PORT`,
`TRICORE_USER` and `TRICORE_SECRET`. Without either they are **skipped** rather
than failed.

## License

[Apache License 2.0](LICENSE)
