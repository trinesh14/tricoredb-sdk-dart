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
  later). See [Running a server](#running-a-server).

## Installation

```bash
dart pub add tricoredb
```

or, in `pubspec.yaml`:

```yaml
dependencies:
  tricoredb: ^0.1.0
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
    // This node is not the leader. e.leaderHint names the one that is, when
    // the cluster knows. This driver never follows the hint by itself: the
    // address may not be reachable from here, a new connection has to
    // authenticate again, and an open transaction cannot move nodes.
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
their own `tricore-server` — point `TRICORE_SERVER_BIN` at the binary, and
without one they are **skipped** rather than failed.

## License

[Apache License 2.0](LICENSE)
