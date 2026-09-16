import 'dart:convert';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:tricoredb/tricoredb.dart';

import 'support/live_server.dart';

/// What this client does against a real `tricore-server`.
///
/// Without a server binary these skip rather than fail: a dependent's
/// `dart test` has to stay green.
void main() {
  final skip = LiveServer.skipReason;
  late LiveServer server;
  late TriCore db;

  setUpAll(() async {
    if (skip != null) return;
    server = await LiveServer.start();
    db = await server.connect();
  });

  tearDownAll(() async {
    if (skip != null) return;
    await db.close();
    await server.stop();
  });

  test('the handshake grants the capabilities this client asks for', () async {
    expect(db.sessionId, isNotEmpty);
    expect(db.grantedFeatures.serverParams, isTrue);
    expect(db.grantedFeatures.sessionTxn, isTrue);
    await db.ping();
    await db.adminPing();
  }, skip: skip);

  test('values round trip through bound parameters', () async {
    final table = uniqueName('t');
    await db.execute(
      'CREATE TABLE $table (id INT PRIMARY KEY, name TEXT, score DOUBLE, '
      'payload BLOB, note TEXT)',
    );
    await db.execute(
      'INSERT INTO $table (id, name, score, payload, note) '
      'VALUES (?, ?, ?, ?, ?)',
      [
        1,
        'ada',
        9.5,
        Blob([0, 159, 146, 150]),
        null
      ],
    );

    final rows = await db.query(
        'SELECT name, score, note FROM $table '
        'WHERE id = ?',
        [1]);
    expect(rows.length, 1);
    expect(rows.value(0, 'name'), 'ada');
    expect(double.parse(rows.value(0, 'score')!), 9.5);
    // Cells are the server's own text rendering, and a SQL NULL renders as
    // the text `NULL` — not as an absent value.
    expect(rows.value(0, 'note'), 'NULL');
  }, skip: skip);

  test('a parameter is a value, never part of the statement', () async {
    final table = uniqueName('t');
    await db.execute('CREATE TABLE $table (id INT PRIMARY KEY, name TEXT)');
    await db.execute('INSERT INTO $table VALUES (?, ?)', [1, 'ada']);

    final found = await db.query(
      'SELECT id FROM $table WHERE name = ?',
      ["ada'; DROP TABLE $table; --"],
    );
    expect(found, isEmpty);

    final survived = await db.query('SELECT id FROM $table');
    expect(survived.length, 1, reason: 'the table is still there');
  }, skip: skip);

  test('a write reports how many rows it changed', () async {
    final table = uniqueName('t');
    await db.execute('CREATE TABLE $table (id INT PRIMARY KEY, n INT)');
    for (var i = 1; i <= 3; i++) {
      await db.execute('INSERT INTO $table VALUES (?, ?)', [i, i]);
    }

    final updated =
        await db.execute('UPDATE $table SET n = ? WHERE id > ?', [0, 1]);
    expect(updated.rowsAffected, 2);
  }, skip: skip);

  test('transactions commit together and roll back together', () async {
    final table = uniqueName('t');
    await db.execute('CREATE TABLE $table (id INT PRIMARY KEY, n INT)');

    await db.begin();
    expect(db.inTransaction, isTrue);
    await db.execute('INSERT INTO $table VALUES (?, ?)', [1, 1]);
    await db.rollback();
    expect(db.inTransaction, isFalse);
    expect(await db.query('SELECT id FROM $table'), isEmpty);

    await db.transaction((tx) async {
      await tx.execute('INSERT INTO $table VALUES (?, ?)', [2, 2]);
      await tx.execute('INSERT INTO $table VALUES (?, ?)', [3, 3]);
    });
    expect((await db.query('SELECT id FROM $table')).length, 2);

    // A failure inside the block leaves nothing behind.
    await expectLater(
      db.transaction((tx) async {
        await tx.execute('INSERT INTO $table VALUES (?, ?)', [4, 4]);
        throw StateError('the caller changed their mind');
      }),
      throwsStateError,
    );
    expect(db.inTransaction, isFalse);
    expect((await db.query('SELECT id FROM $table')).length, 2);
  }, skip: skip);

  test('a refused statement leaves the connection usable', () async {
    await expectLater(
      db.query('SELECT * FROM ${uniqueName('missing')}'),
      throwsA(isA<ServerException>()),
    );
    expect(db.isClosed, isFalse);
    expect((await db.query('SELECT 1')).length, 1);
  }, skip: skip);

  test('cache values are bytes, and a miss is null', () async {
    final namespace = uniqueName('ns');
    expect(await db.cacheGet(namespace, 'k'), isNull);

    await db.cacheSet(namespace, 'k', 'value');
    expect(await db.cacheGetString(namespace, 'k'), 'value');

    final binary = Uint8List.fromList([0, 159, 146, 150]);
    await db.cacheSet(namespace, 'bytes', binary);
    expect(await db.cacheGet(namespace, 'bytes'), binary,
        reason: 'bytes that are not UTF-8 survive the round trip');

    expect(await db.cacheSetIfAbsent(namespace, 'k', 'other'), isFalse);
    expect(await db.cacheDelete(namespace, 'k'), isTrue);
    expect(await db.cacheExists(namespace, 'k'), isFalse);

    expect(await db.cacheIncrement(namespace, 'n'), 1);
    expect(await db.cacheIncrement(namespace, 'n', by: 4), 5);

    await db.cacheRightPush(namespace, 'list', ['a', 'b']);
    expect(await db.cacheLength(namespace, 'list'), 2);
    expect(utf8.decode((await db.cacheLeftPop(namespace, 'list'))!), 'a');

    await db.cacheHashSet(namespace, 'h', {'field': 'value'});
    expect(utf8.decode((await db.cacheHashGet(namespace, 'h', 'field'))!),
        'value');

    final id = await db.cacheStreamAdd(namespace, 's', {'event': 'created'});
    expect(id, isNotEmpty);
    final entries = await db.cacheStreamRange(namespace, 's');
    expect(entries.single.text(), {'event': 'created'});
  }, skip: skip);

  test('documents are inserted, filtered and aggregated', () async {
    final collection = uniqueName('c');
    await db.documentCreateCollection(collection);
    await db.documentInsert(
        collection, {'name': 'ada', 'city': 'Pune', 'visits': 5});
    await db.documentInsert(
        collection, {'name': 'grace', 'city': 'Pune', 'visits': 2});
    await db.documentInsert(
        collection, {'name': 'alan', 'city': 'Delhi', 'visits': 9});

    final inPune = await db.documentFind(
      collection,
      filter: Filter.allOf([
        Filter.eq('city', 'Pune'),
        Filter.gt('visits', 4),
      ]),
    );
    expect(inPune.single['name'], 'ada');

    final totals = await db.documentAggregate(collection, [
      Stage.group(Stage.byField('city'), [Acc.sum('total', 'visits')]),
      Stage.sort(['total'], descending: ['total']),
    ]);
    expect(totals.first['total'], 9);
  }, skip: skip);

  test('vectors are searchable and filterable', () async {
    final collection = uniqueName('v');
    await db.vectorCreateCollection(collection, dimension: 3);
    await db.vectorUpsert(collection, 'a', [1, 0, 0], metadata: {'kind': 'x'});
    await db.vectorUpsert(collection, 'b', [0, 1, 0], metadata: {'kind': 'y'});

    final nearest = await db.vectorSearch(collection, [1, 0, 0], topK: 1);
    expect(nearest.single.id, 'a');

    final filtered = await db.vectorSearch(collection, [1, 0, 0],
        topK: 2, filter: {'kind': 'y'});
    expect(filtered.single.id, 'b');

    final described = await db.vectorDescribeCollection(collection);
    expect(described['dimension'], 3);

    await expectLater(
      db.vectorUpsert(collection, 'c', [1, 0]),
      throwsA(isA<ServerException>()),
      reason: 'a wrong dimension is refused, not padded',
    );
  }, skip: skip);

  test('graphs walk from node to node', () async {
    final graph = uniqueName('g');
    await db.graphCreate(graph);
    await db.graphAddNode(graph, 'n1',
        labels: ['Person'], properties: {'name': 'ada'});
    await db.graphAddNode(graph, 'n2',
        labels: ['Person'], properties: {'name': 'grace'});
    await db.graphAddNode(graph, 'n3',
        labels: ['Person'], properties: {'name': 'alan'});
    await db.graphAddEdge(graph, 'e1', from: 'n1', to: 'n2', label: 'KNOWS');
    await db.graphAddEdge(graph, 'e2', from: 'n2', to: 'n3', label: 'KNOWS');

    final neighbours = await db.graphNeighbors(graph, 'n1');
    expect(neighbours.single['node_id'], 'n2');
    expect(
        await db.graphDegree(graph, 'n2', direction: GraphDirection.both), 2);

    final path = await db.graphShortestPath(graph, from: 'n1', to: 'n3');
    expect(path['found'], isTrue);
    expect(path['hops'], 2);

    final none = await db.graphShortestPath(graph, from: 'n3', to: 'n1');
    expect(none['found'], isFalse,
        reason: 'no path is an answer, not a failure');
  }, skip: skip);

  test('an LLM export renders the catalogue', () async {
    final table = uniqueName('t');
    await db.execute('CREATE TABLE $table (id INT PRIMARY KEY, name TEXT)');
    await db.execute('INSERT INTO $table VALUES (?, ?)', [1, 'ada']);

    final schema = await db.llmSchema();
    expect(schema, isA<String>());
    expect(schema as String, contains(table));

    final context = await db.llmContext([
      LlmSource.sql('SELECT id, name FROM $table'),
    ]);
    expect(context as String, contains('ada'));
  }, skip: skip);

  test('the pool lends connections and takes them back', () async {
    final pool = server.pool(size: 2);
    addTearDown(pool.close);

    final counts = await Future.wait([
      pool.withConnection((db) async => (await db.query('SELECT 1')).length),
      pool.withConnection((db) async => (await db.query('SELECT 1')).length),
      pool.withConnection((db) async => (await db.query('SELECT 1')).length),
    ]);
    expect(counts, [1, 1, 1]);
    expect(pool.stats.lent, 0, reason: 'every connection came back');

    // A transaction left open would otherwise become the next caller's.
    await pool.withConnection((db) async {
      await db.begin();
      await db.execute('SELECT 1');
    });
    await pool.withConnection((db) async {
      expect(db.inTransaction, isFalse);
    });
  }, skip: skip);

  test('a second connection can cancel the first one\'s request', () async {
    final other = await server.connect();
    addTearDown(other.close);

    // Nothing is running under this id, so the server reports zero rather than
    // failing: cancellation is advisory.
    expect(await other.cancel('no-such-request'), 0);
  }, skip: skip);

  test('what the server says about itself is readable', () async {
    final status = await db.adminStatus();
    expect(status, isNotEmpty);
  }, skip: skip);
}
