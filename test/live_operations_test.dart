import 'dart:convert';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:tricoredb/tricoredb.dart';

import 'support/live_server.dart';

/// The operations added for server 0.1.0-rc.3, against a real server.
///
/// Without a server these skip rather than fail.
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

  String text(Uint8List? bytes) => utf8.decode(bytes!);

  test('sorted sets rank, range and re-score their members', () async {
    final ns = uniqueName('zset');
    final added = await db.cacheSortedAdd(ns, 'z', {'a': 1.5, 'b': 2, 'c': 3});
    expect(added['added'], 3);
    expect(await db.cacheSortedCount(ns, 'z'), 3);
    expect(await db.cacheSortedScore(ns, 'z', 'a'), 1.5);
    expect(await db.cacheSortedScore(ns, 'z', 'nobody'), isNull);
    expect(await db.cacheSortedRank(ns, 'z', 'b'), 1);
    expect(await db.cacheSortedRank(ns, 'z', 'c', rev: true), 0);
    expect(await db.cacheSortedRank(ns, 'z', 'nobody'), isNull);

    final all = await db.cacheSortedRange(ns, 'z', 0, -1, withScores: true);
    expect([for (final m in all) m.text], ['a', 'b', 'c']);
    expect([for (final m in all) m.score], [1.5, 2.0, 3.0]);
    final reversed = await db.cacheSortedRange(ns, 'z', 0, 0, rev: true);
    expect(reversed.single.text, 'c');
    expect(reversed.single.score, isNull, reason: 'scores were not asked for');

    final middle = await db.cacheSortedRangeByScore(
      ns,
      'z',
      const ScoreBound.exclusive(1.5),
      ScoreBound.highest,
      limit: (0, 1),
    );
    expect(middle.single.text, 'b');
    final everything = await db.cacheSortedRangeByScore(
        ns, 'z', ScoreBound.lowest, ScoreBound.highest);
    expect(everything.length, 3);

    expect(await db.cacheSortedIncrement(ns, 'z', 'a', 2.25), 3.75);
    expect(await db.cacheSortedRemove(ns, 'z', ['c', 'nobody']), 1);
  }, skip: skip);

  test('a lock has one holder and a token that only rises', () async {
    final ns = uniqueName('lock');
    final first = await db.cacheLockAcquire(
        ns, 'job', 'worker-1', const Duration(seconds: 30));
    expect(first['acquired'], isTrue);
    final token = first['token'] as int;

    final second = await db.cacheLockAcquire(
        ns, 'job', 'worker-2', const Duration(seconds: 30));
    expect(second['acquired'], isFalse);
    expect(second['holder'], 'worker-1');

    final renewed = await db.cacheLockRenew(
        ns, 'job', 'worker-1', const Duration(seconds: 30));
    expect(renewed['renewed'], isTrue);
    expect(renewed['token'], token);

    expect(await db.cacheLockRelease(ns, 'job', 'worker-2'), isFalse);
    expect(await db.cacheLockRelease(ns, 'job', 'worker-1'), isTrue);
    final next = await db.cacheLockAcquire(
        ns, 'job', 'worker-2', const Duration(seconds: 30));
    expect(next['token'] as int, greaterThan(token));
  }, skip: skip);

  test('bitmaps set, count, find and combine bits', () async {
    final ns = uniqueName('bits');
    expect(await db.cacheSetBit(ns, 'a', 7, true), 0);
    expect(await db.cacheSetBit(ns, 'a', 7, true), 1);
    expect(await db.cacheGetBit(ns, 'a', 7), 1);
    expect(await db.cacheGetBit(ns, 'a', 6), 0);
    expect(await db.cacheBitCount(ns, 'a'), 1);
    expect(await db.cacheBitCount(ns, 'a', range: (0, 0)), 1);
    expect(await db.cacheBitPosition(ns, 'a', true), 7);
    await db.cacheSetBit(ns, 'b', 0, true);
    expect(await db.cacheBitOperation(ns, BitOperation.or, 'c', ['a', 'b']), 1);
    expect(await db.cacheGet(ns, 'c'), [0x81]);
  }, skip: skip);

  test('a HyperLogLog estimates distinct elements and merges', () async {
    final ns = uniqueName('hll');
    expect(await db.cacheHyperLogLogAdd(ns, 'x', ['a', 'b', 'c']), isTrue);
    await db.cacheHyperLogLogAdd(ns, 'y', ['c', 'd']);
    expect(await db.cacheHyperLogLogCount(ns, ['x']), 3);
    expect(await db.cacheHyperLogLogCount(ns, ['x', 'y']), 4);
    await db.cacheHyperLogLogMerge(ns, 'all', ['x', 'y']);
    expect(await db.cacheHyperLogLogCount(ns, ['all']), 4);
  }, skip: skip);

  test('many keys are read and written in one request', () async {
    final ns = uniqueName('multi');
    expect(await db.cacheSetMany(ns, {'a': '1', 'b': '2'}), 2);
    final values = await db.cacheGetMany(ns, ['a', 'missing', 'b', 'a']);
    expect(
        values.map((v) => v == null ? null : text(v)), ['1', null, '2', '1']);

    final warmed = await db.cacheWarm(ns, {'a': 'other', 'c': '3'});
    expect(warmed['written'], 1);
    expect(warmed['alreadyCached'], 1);
    expect(await db.cacheGetString(ns, 'a'), '1');
  }, skip: skip);

  test('conditional writes and atomic read-and-change', () async {
    final ns = uniqueName('cond');
    expect(
        await db.cacheSetIf(ns, 'k', 'v1', condition: SetCondition.ifPresent),
        isFalse);
    expect(await db.cacheSetIf(ns, 'k', 'v1', condition: SetCondition.ifAbsent),
        isTrue);
    expect(
      await db.cacheSetIf(ns, 'k', 'v2',
          condition: SetCondition.ifPresent,
          expiry: CacheExpiry.after(const Duration(minutes: 5))),
      isTrue,
    );
    expect(await db.cacheTtl(ns, 'k'), isNotNull);
    expect(text(await db.cacheGetAndExpire(ns, 'k', CacheExpiry.clear)), 'v2');
    expect(await db.cacheTtl(ns, 'k'), isNull);

    expect(text(await db.cacheGetAndSet(ns, 'k', 'v3')), 'v2');
    expect(await db.cacheAppend(ns, 'k', '!'), 3);
    expect(text(await db.cacheGetAndDelete(ns, 'k')), 'v3!');
    expect(await db.cacheGetAndDelete(ns, 'k'), isNull);

    final created = await db.cacheCompareAndSwap(ns, 'cas', null, 'one');
    expect(created['swapped'], isTrue);
    final lost = await db.cacheCompareAndSwap(ns, 'cas', 'wrong', 'two');
    expect(lost['swapped'], isFalse);
    expect(lost['current'], 'one');
    final released = await db.cacheCompareAndSwap(ns, 'cas', 'one', null);
    expect(released['swapped'], isTrue);
    expect(await db.cacheExists(ns, 'cas'), isFalse);

    final at = DateTime.now().add(const Duration(minutes: 5));
    await db.cacheSet(ns, 'e', 'x');
    expect((await db.cacheExpireAt(ns, 'e', at))['updated'], isTrue);
    expect((await db.cacheExpireAt(ns, 'nokey', at))['updated'], isFalse);
  }, skip: skip);

  test('fresh, stale, missing and miss are told apart', () async {
    final ns = uniqueName('fresh');
    const lease = Duration(seconds: 5);
    final miss = await db.cacheGetFresh(ns, 'k', 'me', lease);
    expect(miss.state, 'miss');
    expect(miss.lease, isTrue, reason: 'the first caller computes the value');
    final waiting = await db.cacheGetFresh(ns, 'k', 'you', lease);
    expect(waiting.lease, isFalse);
    expect(waiting.retryAfter, isNotNull);

    final until =
        await db.cacheSetFresh(ns, 'k', 'v', const Duration(minutes: 1));
    expect(until, greaterThan(DateTime.now().millisecondsSinceEpoch));
    final fresh = await db.cacheGetFresh(ns, 'k', 'me', lease);
    expect(fresh.state, 'fresh');
    expect(fresh.text, 'v');

    final binary = Uint8List.fromList([0, 159, 146, 150]);
    await db.cacheSetFresh(ns, 'bin', binary, const Duration(minutes: 1));
    expect((await db.cacheGetFresh(ns, 'bin', 'me', lease)).value, binary,
        reason: 'bytes that are not UTF-8 survive');

    await db.cacheSetFresh(ns, 'old', 'v', const Duration(milliseconds: 1),
        ttl: const Duration(minutes: 1));
    await Future<void>.delayed(const Duration(milliseconds: 50));
    final stale = await db.cacheGetFresh(ns, 'old', 'me', lease);
    expect(stale.state, 'stale');
    expect(stale.refresh, isTrue);

    await db.cacheSetMissing(ns, 'gone', const Duration(minutes: 1));
    expect((await db.cacheGetFresh(ns, 'gone', 'me', lease)).state, 'missing');
  }, skip: skip);

  test('lists move, trim and pop with a wait; sets combine', () async {
    final ns = uniqueName('coll');
    await db.cacheRightPush(ns, 'src', ['a', 'b', 'c']);
    expect(
        text(await db.cacheListMove(
            ns, 'src', 'dst', ListEnd.left, ListEnd.right)),
        'a');
    expect(
        await db.cacheListMove(ns, 'none', 'dst', ListEnd.left, ListEnd.right),
        isNull);
    final trimmed = await db.cacheListTrim(ns, 'src', 0, 0);
    expect(trimmed['length'], 1);

    final popped = await db.cacheBlockingLeftPop(
        ns, ['empty', 'dst'], const Duration(milliseconds: 100));
    expect(popped!.key, 'dst');
    expect(text(popped.value), 'a');
    expect(
        await db.cacheBlockingRightPop(
            ns, ['empty'], const Duration(milliseconds: 100)),
        isNull);

    await db.cacheSetAdd(ns, 's1', ['a', 'b']);
    await db.cacheSetAdd(ns, 's2', ['b', 'c']);
    expect(
        (await db.cacheSetUnion(ns, ['s1', 's2'])).map(text), ['a', 'b', 'c']);
    expect((await db.cacheSetIntersection(ns, ['s1', 's2'])).map(text), ['b']);
    expect((await db.cacheSetDifference(ns, ['s1', 's2'])).map(text), ['a']);
    expect((await db.cacheSetRandomMembers(ns, 's1')).length, 1);
    expect((await db.cacheSetRandomMembers(ns, 's1', count: -5)).length, 5);

    expect(await db.cacheHashIncrement(ns, 'h', 'n', 5), 5);
    expect(await db.cacheHashIncrement(ns, 'h', 'n', -2), 3);
  }, skip: skip);

  test('a pipeline reports each operation; a transaction and a script too',
      () async {
    final ns = uniqueName('pipe');
    final results = await db.cachePipeline([
      CacheOp.set(ns, 'k', 'x'),
      CacheOp.get(ns, 'k'),
      CacheOp.increment(ns, 'k'),
      CacheOp.exists(ns, 'k'),
      CacheOp.ping,
    ]);
    expect(results.length, 5);
    expect(results[0].json!['set'], isTrue);
    expect(text(results[1].value), 'x');
    expect(results[2].isOk, isFalse, reason: 'x is not a number');
    expect(results[2].error, contains('not a valid i64'));
    expect(results[3].json!['exists'], isTrue,
        reason: 'a failing operation does not stop the rest');

    final atomic = await db.cacheTransaction([
      CacheOp.set(ns, 'n', '1'),
      CacheOp.increment(ns, 'n', by: 4),
    ]);
    expect(atomic[1].json!['value'], 5);

    final script = uniqueName('script');
    await db.cacheScriptLoad(script, [
      CacheOp.raw('Set', {
        'namespace': ns,
        'key': r'$1',
        'value': utf8.encode(r'$2'),
        'ttl_ms': null,
      }),
      CacheOp.get(ns, r'$1'),
    ]);
    final ran = await db.cacheScriptRun(script, args: ['sk', 'sv']);
    expect(text(ran[1].value), 'sv');
    expect(await db.cacheGetString(ns, 'sk'), 'sv');
  }, skip: skip);

  test('published messages are polled by id', () async {
    final channel = uniqueName('channel');
    final first = await db.cachePublish(channel, 'hello');
    final second = await db.cachePublish(channel, 'again');
    expect(second, greaterThan(first));

    final polled = await db.cachePoll([channel], after: first - 1);
    final messages = polled['messages'] as List;
    expect(
        [for (final m in messages) (m as Map)['message']], ['hello', 'again']);
    expect(polled['next'], second);

    final later = await db.cachePoll([channel], after: second, limit: 10);
    expect(later['messages'], isEmpty);
  }, skip: skip);

  test('a namespace limit refuses the write that would pass it; stats report',
      () async {
    final ns = uniqueName('limit');
    final set = await db.cacheSetNamespaceLimit(ns, maxKeys: 1);
    expect(set['max_keys'], 1);
    await db.cacheSet(ns, 'a', 'x');
    await expectLater(
        db.cacheSet(ns, 'b', 'x'), throwsA(isA<ServerException>()));
    expect((await db.cacheGetNamespaceLimit(ns))['keys'], 1);
    final lifted = await db.cacheSetNamespaceLimit(ns);
    expect(lifted['max_keys'], isNull);
    await db.cacheSet(ns, 'b', 'x');

    final stats = await db.cacheStats();
    expect(stats['hits'], isA<int>());
    expect(stats['eviction'], isA<Map<Object?, Object?>>());
  }, skip: skip);

  test('a database of its own is purged of expired keys and flushed', () async {
    // These two act on a whole database, so they get one nobody else uses.
    final database = uniqueName('db');
    await db.cacheSet('n', 'keep', 'x', database: database);
    await db.cacheSet('n', 'gone', 'x',
        ttl: const Duration(milliseconds: 1), database: database);
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(await db.cachePurgeExpired(database: database), 1);
    expect(await db.cacheFlushDatabase(database: database), 1);
    expect(await db.cacheExists('n', 'keep', database: database), isFalse);
  }, skip: skip);

  test('hot keys are refused when the server does not track them', () async {
    try {
      final hot = await db.cacheHotKeys(limit: 3);
      expect(hot, isA<Map<String, dynamic>>());
    } on ServerException catch (e) {
      expect(e.message, contains('tracking is off'));
    }
  }, skip: skip);

  test('documents are written in bulk, replaced and deleted by filter',
      () async {
    final collection = uniqueName('docs');
    await db.documentCreateCollection(collection);
    final ids = await db.documentInsertMany(collection, [
      {'_id': 'a', 'n': 1, 'city': 'Pune'},
      {'n': 2, 'city': 'Pune'},
      {'_id': 'c', 'n': 3, 'city': 'Delhi'},
    ]);
    expect(ids.length, 3);
    expect(ids.first, 'a');

    final replaced = await db.documentReplace(collection, 'a', {'n': 10},
        returnDocument: ReturnDocument.after);
    expect(replaced['replaced'], isTrue);
    expect(replaced['document'], {'_id': 'a', 'n': 10});
    expect(
        (await db.documentGet(collection, 'a'))!.containsKey('city'), isFalse,
        reason: 'a replace keeps nothing of the old body');
    await expectLater(db.documentReplace(collection, 'nobody', {'n': 1}),
        throwsA(isA<ServerException>()));
    final upserted =
        await db.documentReplace(collection, 'new', {'n': 5}, upsert: true);
    expect(upserted['inserted'], isTrue);

    final bulk = await db.documentBulkWrite(collection, [
      BulkOp.insert({'n': 1}, id: 'b1'),
      BulkOp.insert({'n': 2}),
      BulkOp.updateOne('b1', set: {'n': 5}, increment: {'m': 1}),
      BulkOp.updateOne('b9', set: {'n': 9}, upsert: true),
      BulkOp.replace('a', {'n': 7}),
      BulkOp.delete('new'),
    ]);
    expect(bulk['inserted'], 2);
    expect(bulk['updated'], 1);
    expect(bulk['upserted'], 1);
    expect(bulk['replaced'], 1);
    expect(bulk['deleted'], 1);
    expect((await db.documentGet(collection, 'b1'))!['n'], 5);
    await expectLater(
      db.documentBulkWrite(collection, [
        BulkOp.insert({'n': 1}, id: 'fresh'),
        BulkOp.insert({'n': 1}, id: 'b1'),
      ]),
      throwsA(isA<ServerException>()),
    );
    expect(await db.documentGet(collection, 'fresh'), isNull,
        reason: 'one refused operation leaves nothing of the batch behind');

    expect(
        await db.documentDeleteMany(collection, Filter.eq('city', 'Delhi')), 1);

    final plan = await db.documentExplain(collection,
        filter: Filter.eq('city', 'Pune'), limit: 5);
    expect(plan['plan'], isA<String>());
    expect(plan['rows_returned'], 1);

    await db.documentCreateIndex(collection, name: 'by_n', field: 'n');
    final rebuilt = await db.documentRebuildIndex(collection, 'by_n');
    expect(rebuilt['rebuilt'], 'by_n');
    expect(await db.documentPurgeExpired(collection), 0);

    final renamed = '${collection}_renamed';
    await db.documentRenameCollection(collection, renamed);
    expect(await db.documentGet(renamed, 'a'), isNotNull);
    await db.documentDropCollection(renamed);
  }, skip: skip);

  test('collection options enforce a schema, defaults and versions', () async {
    final collection = uniqueName('opts');
    await db.documentCreateCollection(collection);
    expect(await db.documentGetCollectionOptions(collection),
        {'versioned': false});
    await db.documentSetCollectionOptions(collection, {
      'schema': {
        'required': ['n'],
        'fields': {
          'n': {'type': 'number', 'minimum': 0},
        },
      },
      'defaults': {'status': 'new'},
      'versioned': true,
    });
    final options = await db.documentGetCollectionOptions(collection);
    expect(options['versioned'], isTrue);
    expect(options['defaults'], {'status': 'new'});

    await db.documentInsert(collection, {'n': 1}, id: 'v');
    final stored = (await db.documentGet(collection, 'v'))!;
    expect(stored['status'], 'new');
    expect(stored['_version'], 1);
    await expectLater(db.documentInsert(collection, {'x': 1}),
        throwsA(isA<ServerException>()));

    await db.documentReplace(collection, 'v', {'n': 2}, expectedVersion: 1);
    await expectLater(
      db.documentReplace(collection, 'v', {'n': 3}, expectedVersion: 1),
      throwsA(isA<ServerException>()
          .having((e) => e.message, 'message', contains('version conflict'))),
    );
    await db.documentDropCollection(collection);
  }, skip: skip);

  test('a document transaction is seen only by itself until it commits',
      () async {
    final collection = uniqueName('txn');
    await db.documentCreateCollection(collection);

    final txn = await db.documentBeginTransaction();
    await db.documentInsert(collection, {'n': 1}, id: 't1', txn: txn);
    expect(await db.documentGet(collection, 't1', txn: txn), isNotNull);
    expect(await db.documentGet(collection, 't1'), isNull);
    final committed = await db.documentCommitTransaction(txn);
    expect(committed['committed'], isTrue);
    expect(await db.documentGet(collection, 't1'), isNotNull);

    final discarded = await db.documentBeginTransaction(serializable: true);
    await db.documentInsertMany(
        collection,
        [
          {'_id': 't2'}
        ],
        txn: discarded);
    await db.documentRollbackTransaction(discarded);
    expect(await db.documentGet(collection, 't2'), isNull);
    await expectLater(db.documentCommitTransaction(discarded),
        throwsA(isA<ServerException>()));
    await db.documentDropCollection(collection);
  }, skip: skip);

  test('vectors are written and searched in batches', () async {
    final collection = uniqueName('vec');
    await db.vectorCreateCollection(collection, dimension: 3);
    expect(
      await db.vectorUpsertMany(collection, [
        {
          'id': 'a',
          'vector': [1.0, 0.0, 0.0],
          'metadata': {'kind': 'x', '_namespace': 'n1'},
        },
        {
          'id': 'b',
          'vector': [0.0, 1.0, 0.0],
          'metadata': {'kind': 'y', '_namespace': 'n1'},
        },
        {
          'id': 'c',
          'vector': [0.0, 0.0, 1.0],
        },
        {
          'id': 'd',
          'vector': [0.9, 0.1, 0.0],
          'metadata': {'kind': 'x', '_namespace': 'n2'},
        },
      ]),
      4,
    );

    final batches = await db.vectorSearchBatch(
      collection,
      [
        [1, 0, 0],
        [0, 1, 0],
      ],
      2,
    );
    expect(batches.length, 2);
    expect(batches[0].first.id, 'a');
    expect(batches[1].first.id, 'b');

    final exact = await db.vectorSearchBatch(
      collection,
      [
        [1, 0, 0],
      ],
      2,
      filter: {'kind': 'x'},
      includeVectors: true,
      options: const VectorSearchOptions(
          exact: true, namespace: 'n1', explain: true, includeMetadata: false),
    );
    expect(exact.single.single.id, 'a');
    expect(exact.single.single.vector, [1.0, 0.0, 0.0]);
    expect(exact.single.single.reason, isNotEmpty);
    expect(exact.single.single.metadata, isEmpty);

    expect(await db.vectorCount(collection), 4);
    expect(await db.vectorCount(collection, filter: {'kind': 'x'}), 2);
    expect(
        await db
            .vectorUpdateByFilter(collection, {'kind': 'x'}, {'seen': true}),
        2);
    expect(((await db.vectorGet(collection, 'a'))!['metadata'] as Map)['seen'],
        isTrue);

    final partitions = await db.vectorListPartitions(collection);
    expect(partitions['partitions'], [
      {'count': 2, 'name': 'n1'},
      {'count': 1, 'name': 'n2'},
    ]);
    expect(partitions['unpartitioned'], 1);

    final fused = await db.vectorSearchMulti(
      collection,
      [
        [1, 0, 0],
        [0, 1, 0],
      ],
      3,
      fusion: FusionMethod.max,
    );
    expect(fused.first.matched, 2);

    final alike = await db.vectorSearchByExamples(collection, 2, positive: [
      'a',
      [0.0, 1.0, 0.0],
    ], negative: [
      'c'
    ]);
    expect(alike.map((hit) => hit.id), isNot(contains('a')),
        reason: 'an example named by id is left out');
    final withExamples = await db.vectorSearchByExamples(collection, 1,
        positive: ['a'], excludeExamples: false);
    expect(withExamples.single.id, 'a');

    expect(await db.vectorDeleteMany(collection, ['c', 'nobody']), 1);
    expect(await db.vectorDeleteByFilter(collection, {'kind': 'y'}), 1);
    await expectLater(db.vectorDeleteByFilter(collection, {}),
        throwsA(isA<ServerException>()));

    expect((await db.vectorIndexStats(collection))['stored'], 2);
    expect((await db.vectorRebuildIndex(collection))['rebuilt'], isTrue);
    expect((await db.vectorMeasureRecall(collection, sample: 2))['recall'],
        isA<num>());

    final copy = '${collection}_copy';
    final renamed = '${collection}_renamed';
    await db.vectorCopyCollection(collection, copy);
    expect(await db.vectorCount(copy), 2);
    await db.vectorRenameCollection(copy, renamed);
    expect(await db.vectorCount(renamed), 2);
    await db.vectorDropCollection(renamed);
    await db.vectorDropCollection(collection);
  }, skip: skip);

  test('named vectors, aliases, configuration and text', () async {
    final collection = uniqueName('named');
    await db.vectorCreateCollection(collection, dimension: 3);
    final configured = await db.vectorSetCollectionConfig(collection,
        normalize: true, m: 8, efConstruction: 100);
    expect(configured['normalize'], isTrue);
    expect(configured['m'], 8);

    await db.vectorAddNamedVector(collection, 'title', 2,
        metric: VectorMetric.dot);
    await db.vectorUpsertNamed(collection, 'n1', {
      'default': [1.0, 0.0, 0.0],
      'title': [1.0, 0.0],
    }, metadata: {
      'kind': 'named'
    });
    expect(await db.vectorCount(collection), 1);

    final alias = uniqueName('alias');
    await db.vectorSetAlias(alias, collection: collection);
    bool listed(List<Map<String, dynamic>> aliases) => aliases
        .any((a) => a['alias'] == alias && a['collection'] == collection);
    expect(listed(await db.vectorListAliases()), isTrue);
    expect(await db.vectorCount(alias), 1,
        reason: 'an operation naming the alias acts on the collection');
    await db.vectorSetAlias(alias);
    expect(listed(await db.vectorListAliases()), isFalse);
    await db.vectorDropCollection(collection);

    final embedded = await db.vectorEmbed(['hello world', 'goodbye'], 4);
    expect(embedded['dimension'], 4);
    expect((embedded['vectors'] as List).length, 2);

    final texts = uniqueName('text');
    await db.vectorCreateCollection(texts, dimension: 8);
    expect(
      await db.vectorUpsertText(texts, [
        {
          'id': 't1',
          'text': 'the quick brown fox',
          'metadata': {'lang': 'en'},
        },
        {'id': 't2', 'text': 'lazy dog'},
      ]),
      2,
    );
    expect(((await db.vectorGet(texts, 't1'))!['metadata'] as Map)['text'],
        'the quick brown fox');
    await db.vectorDropCollection(texts);
  }, skip: skip);

  test('graphs are written in bulk and walked', () async {
    final graph = uniqueName('graph');
    await db.graphCreate(graph);
    final added = await db.graphAddMany(graph, nodes: [
      {
        'id': 'a',
        'labels': ['Person'],
        'properties': {'name': 'ada', 'age': 36},
      },
      {
        'id': 'b',
        'labels': ['Person'],
        'properties': {'name': 'bob'},
      },
      {
        'id': 'c',
        'labels': ['Person'],
        'properties': {'name': 'cy'},
      },
      {'id': 'd'},
      {
        'id': 'e',
        'labels': ['City'],
      },
    ], edges: [
      {
        'id': 'e1',
        'from': 'a',
        'to': 'b',
        'label': 'KNOWS',
        'properties': {'w': 1},
      },
      {
        'id': 'e2',
        'from': 'b',
        'to': 'c',
        'label': 'KNOWS',
        'properties': {'w': 2},
      },
      {'id': 'e3', 'from': 'c', 'to': 'a', 'label': 'KNOWS'},
      {'id': 'e4', 'from': 'a', 'to': 'c', 'label': 'LIKES'},
      {'id': 'e5', 'from': 'c', 'to': 'd', 'label': 'KNOWS'},
    ]);
    expect(added['nodes'], 5);
    expect(added['edges'], 5);

    final node = await db.graphUpdateNode(graph, 'a',
        properties: {'age': null, 'city': 'Pune'}, addLabels: ['Admin']);
    expect(node['labels'], ['Person', 'Admin']);
    expect(node['properties'], {'city': 'Pune', 'name': 'ada'});
    final edge = await db.graphUpdateEdge(graph, 'e4',
        properties: {'since': 2020}, label: 'LOVES');
    expect(edge['label'], 'LOVES');

    final walk = await db.graphDfs(graph, 'a');
    expect(walk['order'], 'depth_first');
    expect(walk['count'], 4);
    final near = await db.graphDfs(graph, 'a',
        direction: GraphDirection.both, label: 'KNOWS', maxDepth: 1, limit: 2);
    expect(near['truncated'], isTrue);

    final weak = await db.graphConnectedComponents(graph);
    expect(weak['count'], 2);
    expect(weak['largest'], 4);
    final strong = await db.graphConnectedComponents(graph,
        kind: ComponentKind.strong, label: 'KNOWS');
    expect(strong['largest'], 3);

    final cycle = await db.graphDetectCycle(graph);
    expect(cycle['hasCycle'], isTrue);
    expect(
        (await db.graphDetectCycle(graph,
            directed: false, label: 'LOVES'))['hasCycle'],
        isFalse);

    final stats = await db.graphStats(graph);
    expect(stats['nodes'], 5);
    expect(stats['edgeTypes'], {'KNOWS': 4, 'LOVES': 1});
    expect((await db.graphSchema(graph))['nodes'], 5);

    final sub = await db.graphSubgraph(graph, ['a']);
    expect(sub['node_count'], 3);
    expect(sub['edge_count'], 4);

    final shortest = await db.graphKShortestPaths(graph, 'a', 'c', k: 2);
    expect(shortest['count'], 2);
    final weighted = await db.graphKShortestPaths(graph, 'a', 'c',
        label: 'KNOWS', weightProperty: 'w');
    expect(((weighted['paths'] as List).first as Map)['cost'], 3.0);
    final paths = await db.graphAllPaths(graph, 'a', 'c');
    expect(paths['complete'], isTrue);
    expect(paths['count'], 2);

    for (final measure in CentralityMeasure.values) {
      final central = await db.graphCentrality(graph, measure, limit: 3);
      expect(central['measure'], measure.wire);
      expect((central['results'] as List).length, 3);
    }
    expect((await db.graphCommunities(graph))['algorithm'], 'louvain');
    expect((await db.graphTriangleCount(graph))['triangles'], 1);
    final similar = await db.graphNodeSimilarity(graph, 'a',
        metric: SimilarityMetric.overlap, limit: 1);
    expect((similar['results'] as List).length, 1);
    final ranks = await db.graphPageRank(graph, damping: 0.9, limit: 2);
    expect((ranks['ranks'] as List).length, 2);
    expect((await db.graphCheckConsistency(graph))['consistent'], isTrue);

    final removed = await db.graphRemoveMany(graph,
        nodes: ['d', 'nobody'], edges: ['e4'], detach: true);
    expect(removed['nodes_removed'], 1);
    expect(removed['edges_removed'], 2);

    final copy = '${graph}_copy';
    final copied = await db.graphCopy(graph, copy);
    expect(copied['nodes'], 4);
    await expectLater(
        db.graphCopy(graph, copy), throwsA(isA<ServerException>()));
    await db.graphDrop(copy);
    await db.graphDrop(graph);
  }, skip: skip);

  test('graph indexes and constraints, Cypher writes and transactions',
      () async {
    final graph = uniqueName('schema');
    await db.graphCreate(graph);
    await db.graphImportCsv(graph,
        nodes: 'id,labels,name\nx1,Person,xena\nx2,Person,yuri\n',
        edges: 'id,from,to,label\nxe1,x1,x2,KNOWS\n');

    await db.graphCreateNodeIndex(graph, 'by_name', ['name'], label: 'Person');
    await db.graphCreateConstraint(
        graph, 'unique_name', 'Person', 'name', ConstraintKind.unique);
    final objects = await db.graphListSchemaObjects(graph);
    expect(((objects['indexes'] as List).single as Map)['name'], 'by_name');
    expect(((objects['constraints'] as List).single as Map)['kind'], 'unique');

    final found = await db
        .graphFindNodes(graph, label: 'Person', properties: {'name': 'xena'});
    expect(found['count'], 1);
    expect(found['plan'], contains('by_name'));
    await expectLater(
      db.graphAddNode(graph, 'dup',
          labels: ['Person'], properties: {'name': 'xena'}),
      throwsA(isA<ServerException>()),
      reason: 'the constraint refuses a second xena',
    );
    await expectLater(
      db.graphCreateConstraint(
          graph, 'edge_since', 'KNOWS', 'since', ConstraintKind.required,
          target: ConstraintTarget.edge),
      throwsA(isA<ServerException>()),
      reason: 'a stored edge already breaks it',
    );
    await db.graphDropSchemaObject(graph, 'unique_name');

    final written =
        await db.graphExecute(graph, "CREATE (n:Person {name: 'zed'})");
    expect(written['nodesCreated'], 1);

    final txn = await db.graphBeginTransaction(graph);
    final queued = await db
        .graphExecute(graph, "CREATE (n:Person {name: 'queued'})", txn: txn);
    expect(queued['queued'], 1);
    expect(
        (await db
            .graphFindNodes(graph, properties: {'name': 'queued'}))['count'],
        0);
    final committed = await db.graphCommitTransaction(graph, txn);
    expect(committed['committed'], 1);
    expect(
        (await db
            .graphFindNodes(graph, properties: {'name': 'queued'}))['count'],
        1);

    final discarded = await db.graphBeginTransaction(graph);
    await db.graphExecute(graph, "CREATE (n:Person {name: 'never'})",
        txn: discarded);
    await db.graphRollbackTransaction(graph, discarded);
    expect(
        (await db
            .graphFindNodes(graph, properties: {'name': 'never'}))['count'],
        0);
    await db.graphDrop(graph);
  }, skip: skip);
}
