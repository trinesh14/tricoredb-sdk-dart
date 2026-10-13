import 'dart:async';
import 'dart:convert';

import 'package:test/test.dart';
import 'package:tricoredb/tricoredb.dart';

import 'support/scripted_peer.dart';

/// One call, the operation it must send, and a response to answer it with.
class _Case {
  final String name;
  final String op;
  final String reply;
  final String? txn;
  final Future<Object?> Function(TriCore db) call;

  const _Case(this.name, this.op, this.reply, this.txn, this.call);
}

/// The exact request each operation added for server 0.1.0-rc.3 sends, read
/// back from a scripted peer. These need no server.
void main() {
  for (final c in _cases) {
    test('${c.name} sends the operation the server declares', () async {
      final peer = await ScriptedPeer.start(steps: [
        response(
            {'request_id': 'r1', 'status': 'ok', 'data': json.decode(c.reply)}),
      ]);
      addTearDown(peer.shutdown);
      final db = await peer.connect();
      // Not awaited: a scripted peer never answers the goodbye, so closing
      // costs its timeout, and these tests only read what was sent.
      addTearDown(() => unawaited(db.close()));

      await c.call(db);

      final request = peer.requests.single;
      expect(request['op'], json.decode(c.op));
      expect(request['database'], 'main');
      final options = request['options'];
      expect(options is Map ? options['txn'] : null, c.txn);
    });
  }
}

final List<_Case> _cases = [
  _Case(
      'cacheSetFresh',
      r'''{"Cache":{"SetFresh":{"namespace":"x-namespace","key":"x-key","value":[97,98],"fresh_ms":1503,"ttl_ms":1504}}}''',
      r'''{"Json":{"freshUntilMs":1791870378514,"set":true}}''',
      null,
      (db) => db.cacheSetFresh(
          'x-namespace', 'x-key', 'ab', const Duration(milliseconds: 1503),
          ttl: const Duration(milliseconds: 1504))),
  _Case(
      'cacheSetFresh with only what it requires',
      r'''{"Cache":{"SetFresh":{"namespace":"x-namespace","key":"x-key","value":[97,98],"fresh_ms":1503,"ttl_ms":null}}}''',
      r'''{"Json":{"freshUntilMs":1791870378514,"set":true}}''',
      null,
      (db) => db.cacheSetFresh(
          'x-namespace', 'x-key', 'ab', const Duration(milliseconds: 1503))),
  _Case(
      'cacheGetFresh',
      r'''{"Cache":{"GetFresh":{"namespace":"x-namespace","key":"x-key","owner":"x-owner","lease_ms":1503}}}''',
      r'''{"Json":{"state":"stale","value":"v","refresh":true}}''',
      null,
      (db) => db.cacheGetFresh('x-namespace', 'x-key', 'x-owner',
          const Duration(milliseconds: 1503))),
  _Case(
      'cacheSetMissing',
      r'''{"Cache":{"SetMissing":{"namespace":"x-namespace","key":"x-key","ttl_ms":1502}}}''',
      r'''{"Json":{"set":true}}''',
      null,
      (db) => db.cacheSetMissing(
          'x-namespace', 'x-key', const Duration(milliseconds: 1502))),
  _Case(
      'cacheWarm',
      r'''{"Cache":{"Warm":{"namespace":"x-namespace","entries":[{"key":"a","value":[120]},{"key":"b","value":[121,122]}],"ttl_ms":1502}}}''',
      r'''{"Json":{"alreadyCached":1,"written":1}}''',
      null,
      (db) => db.cacheWarm(
          'x-namespace',
          {
            'a': 'x',
            'b': [121, 122]
          },
          ttl: const Duration(milliseconds: 1502))),
  _Case(
      'cacheWarm with only what it requires',
      r'''{"Cache":{"Warm":{"namespace":"x-namespace","entries":[{"key":"a","value":[120]},{"key":"b","value":[121,122]}],"ttl_ms":null}}}''',
      r'''{"Json":{"alreadyCached":1,"written":1}}''',
      null,
      (db) => db.cacheWarm('x-namespace', {
            'a': 'x',
            'b': [121, 122]
          })),
  _Case(
      'cacheGetMany',
      r'''{"Cache":{"MGet":{"namespace":"x-namespace","keys":["p","q"]}}}''',
      r'''{"CacheValues":[[49],null,[50]]}''',
      null,
      (db) => db.cacheGetMany('x-namespace', ['p', 'q'])),
  _Case(
      'cacheSetMany',
      r'''{"Cache":{"MSet":{"namespace":"x-namespace","entries":[{"key":"a","value":[120]},{"key":"b","value":[121,122]}],"ttl_ms":1502}}}''',
      r'''{"Json":{"set":2}}''',
      null,
      (db) => db.cacheSetMany(
          'x-namespace',
          {
            'a': 'x',
            'b': [121, 122]
          },
          ttl: const Duration(milliseconds: 1502))),
  _Case(
      'cacheSetMany with only what it requires',
      r'''{"Cache":{"MSet":{"namespace":"x-namespace","entries":[{"key":"a","value":[120]},{"key":"b","value":[121,122]}],"ttl_ms":null}}}''',
      r'''{"Json":{"set":2}}''',
      null,
      (db) => db.cacheSetMany('x-namespace', {
            'a': 'x',
            'b': [121, 122]
          })),
  _Case(
      'cacheSetIf',
      r'''{"Cache":{"SetIf":{"namespace":"x-namespace","key":"x-key","value":[97,98],"condition":"IfAbsent","expiry":{"Set":{"ttl_ms":1500}}}}}''',
      r'''{"Json":{"set":true}}''',
      null,
      (db) => db.cacheSetIf('x-namespace', 'x-key', 'ab',
          condition: SetCondition.ifAbsent,
          expiry: CacheExpiry.after(const Duration(milliseconds: 1500)))),
  _Case(
      'cacheSetIf with only what it requires',
      r'''{"Cache":{"SetIf":{"namespace":"x-namespace","key":"x-key","value":[97,98],"condition":"Always","expiry":"Clear"}}}''',
      r'''{"Json":{"set":true}}''',
      null,
      (db) => db.cacheSetIf('x-namespace', 'x-key', 'ab')),
  _Case(
      'cacheSortedAdd',
      r'''{"Cache":{"ZAdd":{"namespace":"x-namespace","key":"x-key","members":[{"score":1.5,"member":[97]},{"score":2.0,"member":[98]}]}}}''',
      r'''{"Json":{"added":3,"updated":0}}''',
      null,
      (db) => db.cacheSortedAdd('x-namespace', 'x-key', {'a': 1.5, 'b': 2.0})),
  _Case(
      'cacheSortedRemove',
      r'''{"Cache":{"ZRem":{"namespace":"x-namespace","key":"x-key","members":[[97],[98,99]]}}}''',
      r'''{"Json":{"removed":1}}''',
      null,
      (db) => db.cacheSortedRemove('x-namespace', 'x-key', [
            'a',
            [98, 99]
          ])),
  _Case(
      'cacheSortedScore',
      r'''{"Cache":{"ZScore":{"namespace":"x-namespace","key":"x-key","member":[97,98]}}}''',
      r'''{"Json":{"score":1.5}}''',
      null,
      (db) => db.cacheSortedScore('x-namespace', 'x-key', 'ab')),
  _Case(
      'cacheSortedCount',
      r'''{"Cache":{"ZCard":{"namespace":"x-namespace","key":"x-key"}}}''',
      r'''{"Json":{"cardinality":2}}''',
      null,
      (db) => db.cacheSortedCount('x-namespace', 'x-key')),
  _Case(
      'cacheSortedRank',
      r'''{"Cache":{"ZRank":{"namespace":"x-namespace","key":"x-key","member":[97,98],"rev":true}}}''',
      r'''{"Json":{"rank":1}}''',
      null,
      (db) => db.cacheSortedRank('x-namespace', 'x-key', 'ab', rev: true)),
  _Case(
      'cacheSortedRank with only what it requires',
      r'''{"Cache":{"ZRank":{"namespace":"x-namespace","key":"x-key","member":[97,98],"rev":false}}}''',
      r'''{"Json":{"rank":1}}''',
      null,
      (db) => db.cacheSortedRank('x-namespace', 'x-key', 'ab')),
  _Case(
      'cacheSortedRange',
      r'''{"Cache":{"ZRange":{"namespace":"x-namespace","key":"x-key","start":5,"stop":6,"rev":true,"with_scores":true}}}''',
      r'''{"Json":{"count":2,"members":[{"member":[97],"score":1.5},{"member":[98],"score":2.0}]}}''',
      null,
      (db) => db.cacheSortedRange('x-namespace', 'x-key', 5, 6,
          rev: true, withScores: true)),
  _Case(
      'cacheSortedRange with only what it requires',
      r'''{"Cache":{"ZRange":{"namespace":"x-namespace","key":"x-key","start":5,"stop":6,"rev":false,"with_scores":false}}}''',
      r'''{"Json":{"count":2,"members":[{"member":[97],"score":1.5},{"member":[98],"score":2.0}]}}''',
      null,
      (db) => db.cacheSortedRange('x-namespace', 'x-key', 5, 6)),
  _Case(
      'cacheSortedRangeByScore',
      r'''{"Cache":{"ZRangeByScore":{"namespace":"x-namespace","key":"x-key","min":{"value":1.5,"exclusive":false},"max":{"value":9.0,"exclusive":true},"rev":true,"with_scores":true,"limit":[2,9]}}}''',
      r'''{"Json":{"count":1,"members":[[97]]}}''',
      null,
      (db) => db.cacheSortedRangeByScore('x-namespace', 'x-key',
          const ScoreBound.inclusive(1.5), const ScoreBound.exclusive(9.0),
          rev: true, withScores: true, limit: (2, 9))),
  _Case(
      'cacheSortedRangeByScore with only what it requires',
      r'''{"Cache":{"ZRangeByScore":{"namespace":"x-namespace","key":"x-key","min":{"value":1.5,"exclusive":false},"max":{"value":9.0,"exclusive":true},"rev":false,"with_scores":false,"limit":null}}}''',
      r'''{"Json":{"count":1,"members":[[97]]}}''',
      null,
      (db) => db.cacheSortedRangeByScore('x-namespace', 'x-key',
          const ScoreBound.inclusive(1.5), const ScoreBound.exclusive(9.0))),
  _Case(
      'cacheSortedIncrement',
      r'''{"Cache":{"ZIncrBy":{"namespace":"x-namespace","key":"x-key","member":[97,98],"by":4.5}}}''',
      r'''{"Json":{"score":3.75}}''',
      null,
      (db) => db.cacheSortedIncrement('x-namespace', 'x-key', 'ab', 4.5)),
  _Case(
      'cacheLockAcquire',
      r'''{"Cache":{"LockAcquire":{"namespace":"x-namespace","key":"x-key","owner":"x-owner","ttl_ms":1503}}}''',
      r'''{"Json":{"acquired":true,"expiresAtMs":1791870324210,"holder":"o1","token":1}}''',
      null,
      (db) => db.cacheLockAcquire('x-namespace', 'x-key', 'x-owner',
          const Duration(milliseconds: 1503))),
  _Case(
      'cacheLockRenew',
      r'''{"Cache":{"LockRenew":{"namespace":"x-namespace","key":"x-key","owner":"x-owner","ttl_ms":1503}}}''',
      r'''{"Json":{"expiresAtMs":1791870324268,"renewed":true,"token":1}}''',
      null,
      (db) => db.cacheLockRenew('x-namespace', 'x-key', 'x-owner',
          const Duration(milliseconds: 1503))),
  _Case(
      'cacheLockRelease',
      r'''{"Cache":{"LockRelease":{"namespace":"x-namespace","key":"x-key","owner":"x-owner"}}}''',
      r'''{"Json":{"released":true}}''',
      null,
      (db) => db.cacheLockRelease('x-namespace', 'x-key', 'x-owner')),
  _Case(
      'cacheSetBit',
      r'''{"Cache":{"SetBit":{"namespace":"x-namespace","key":"x-key","offset":5,"value":true}}}''',
      r'''{"Json":{"previous":0}}''',
      null,
      (db) => db.cacheSetBit('x-namespace', 'x-key', 5, true)),
  _Case(
      'cacheGetBit',
      r'''{"Cache":{"GetBit":{"namespace":"x-namespace","key":"x-key","offset":5}}}''',
      r'''{"Json":{"bit":1}}''',
      null,
      (db) => db.cacheGetBit('x-namespace', 'x-key', 5)),
  _Case(
      'cacheBitCount',
      r'''{"Cache":{"BitCount":{"namespace":"x-namespace","key":"x-key","range":[2,9]}}}''',
      r'''{"Json":{"count":1}}''',
      null,
      (db) => db.cacheBitCount('x-namespace', 'x-key', range: (2, 9))),
  _Case(
      'cacheBitCount with only what it requires',
      r'''{"Cache":{"BitCount":{"namespace":"x-namespace","key":"x-key","range":null}}}''',
      r'''{"Json":{"count":1}}''',
      null,
      (db) => db.cacheBitCount('x-namespace', 'x-key')),
  _Case(
      'cacheBitPosition',
      r'''{"Cache":{"BitPos":{"namespace":"x-namespace","key":"x-key","bit":true,"range":[2,9]}}}''',
      r'''{"Json":{"position":7}}''',
      null,
      (db) => db.cacheBitPosition('x-namespace', 'x-key', true, range: (2, 9))),
  _Case(
      'cacheBitPosition with only what it requires',
      r'''{"Cache":{"BitPos":{"namespace":"x-namespace","key":"x-key","bit":true,"range":null}}}''',
      r'''{"Json":{"position":7}}''',
      null,
      (db) => db.cacheBitPosition('x-namespace', 'x-key', true)),
  _Case(
      'cacheBitOperation',
      r'''{"Cache":{"BitOp":{"namespace":"x-namespace","op":"Or","dest":"x-dest","keys":["p","q"]}}}''',
      r'''{"Json":{"length":1}}''',
      null,
      (db) => db.cacheBitOperation(
          'x-namespace', BitOperation.or, 'x-dest', ['p', 'q'])),
  _Case(
      'cacheHyperLogLogAdd',
      r'''{"Cache":{"PfAdd":{"namespace":"x-namespace","key":"x-key","elements":[[97],[98,99]]}}}''',
      r'''{"Json":{"changed":true}}''',
      null,
      (db) => db.cacheHyperLogLogAdd('x-namespace', 'x-key', [
            'a',
            [98, 99]
          ])),
  _Case(
      'cacheHyperLogLogCount',
      r'''{"Cache":{"PfCount":{"namespace":"x-namespace","keys":["p","q"]}}}''',
      r'''{"Json":{"count":4}}''',
      null,
      (db) => db.cacheHyperLogLogCount('x-namespace', ['p', 'q'])),
  _Case(
      'cacheHyperLogLogMerge',
      r'''{"Cache":{"PfMerge":{"namespace":"x-namespace","dest":"x-dest","sources":["p","q"]}}}''',
      r'''{"Json":{"merged":true}}''',
      null,
      (db) => db.cacheHyperLogLogMerge('x-namespace', 'x-dest', ['p', 'q'])),
  _Case(
      'cacheListMove',
      r'''{"Cache":{"LMove":{"namespace":"x-namespace","source":"x-source","destination":"x-destination","from":"Right","to":"Right"}}}''',
      r'''{"CacheValue":[97]}''',
      null,
      (db) => db.cacheListMove('x-namespace', 'x-source', 'x-destination',
          ListEnd.right, ListEnd.right)),
  _Case(
      'cacheExpireAt',
      r'''{"Cache":{"ExpireAt":{"namespace":"x-namespace","key":"x-key","at_ms":1700000000000}}}''',
      r'''{"Json":{"deleted":false,"updated":true}}''',
      null,
      (db) => db.cacheExpireAt('x-namespace', 'x-key',
          DateTime.fromMillisecondsSinceEpoch(1700000000000))),
  _Case(
      'cacheStats',
      r'''{"Cache":"Stats"}''',
      r'''{"Json":{"hits":26,"misses":9,"hitRatio":0.74,"eviction":{"evicted":0,"policy":"lru"}}}''',
      null,
      (db) => db.cacheStats()),
  _Case('cacheFlushDatabase', r'''{"Cache":"FlushDatabase"}''',
      r'''{"Json":{"cleared":1}}''', null, (db) => db.cacheFlushDatabase()),
  _Case(
      'cacheAppend',
      r'''{"Cache":{"Append":{"namespace":"x-namespace","key":"x-key","value":[97,98]}}}''',
      r'''{"Json":{"length":2}}''',
      null,
      (db) => db.cacheAppend('x-namespace', 'x-key', 'ab')),
  _Case(
      'cacheCompareAndSwap',
      r'''{"Cache":{"CompareAndSwap":{"namespace":"x-namespace","key":"x-key","expected":[97,98],"value":[97,98],"ttl_ms":1504}}}''',
      r'''{"Json":{"current":"v1","swapped":false}}''',
      null,
      (db) => db.cacheCompareAndSwap('x-namespace', 'x-key', 'ab', 'ab',
          ttl: const Duration(milliseconds: 1504))),
  _Case(
      'cacheCompareAndSwap with only what it requires',
      r'''{"Cache":{"CompareAndSwap":{"namespace":"x-namespace","key":"x-key","expected":[97,98],"value":[97,98],"ttl_ms":null}}}''',
      r'''{"Json":{"current":"v1","swapped":false}}''',
      null,
      (db) => db.cacheCompareAndSwap('x-namespace', 'x-key', 'ab', 'ab')),
  _Case(
      'cacheGetAndDelete',
      r'''{"Cache":{"GetDel":{"namespace":"x-namespace","key":"x-key"}}}''',
      r'''{"CacheValue":[97,98]}''',
      null,
      (db) => db.cacheGetAndDelete('x-namespace', 'x-key')),
  _Case(
      'cacheGetAndSet',
      r'''{"Cache":{"GetSet":{"namespace":"x-namespace","key":"x-key","value":[97,98],"ttl_ms":1503}}}''',
      r'''{"CacheValue":[49]}''',
      null,
      (db) => db.cacheGetAndSet('x-namespace', 'x-key', 'ab',
          ttl: const Duration(milliseconds: 1503))),
  _Case(
      'cacheGetAndSet with only what it requires',
      r'''{"Cache":{"GetSet":{"namespace":"x-namespace","key":"x-key","value":[97,98],"ttl_ms":null}}}''',
      r'''{"CacheValue":[49]}''',
      null,
      (db) => db.cacheGetAndSet('x-namespace', 'x-key', 'ab')),
  _Case(
      'cacheGetAndExpire',
      r'''{"Cache":{"GetEx":{"namespace":"x-namespace","key":"x-key","expiry":{"Set":{"ttl_ms":1500}}}}}''',
      r'''{"CacheValue":[50]}''',
      null,
      (db) => db.cacheGetAndExpire('x-namespace', 'x-key',
          CacheExpiry.after(const Duration(milliseconds: 1500)))),
  _Case(
      'cacheListTrim',
      r'''{"Cache":{"LTrim":{"namespace":"x-namespace","key":"x-key","start":5,"stop":6}}}''',
      r'''{"Json":{"length":1,"removed":1}}''',
      null,
      (db) => db.cacheListTrim('x-namespace', 'x-key', 5, 6)),
  _Case(
      'cacheSetUnion',
      r'''{"Cache":{"SUnion":{"namespace":"x-namespace","keys":["p","q"]}}}''',
      r'''{"Json":{"count":2,"members":[[97],[98]]}}''',
      null,
      (db) => db.cacheSetUnion('x-namespace', ['p', 'q'])),
  _Case(
      'cacheSetIntersection',
      r'''{"Cache":{"SInter":{"namespace":"x-namespace","keys":["p","q"]}}}''',
      r'''{"Json":{"count":1,"members":[[98]]}}''',
      null,
      (db) => db.cacheSetIntersection('x-namespace', ['p', 'q'])),
  _Case(
      'cacheSetDifference',
      r'''{"Cache":{"SDiff":{"namespace":"x-namespace","keys":["p","q"]}}}''',
      r'''{"Json":{"count":1,"members":[[97]]}}''',
      null,
      (db) => db.cacheSetDifference('x-namespace', ['p', 'q'])),
  _Case(
      'cacheSetRandomMembers',
      r'''{"Cache":{"SRandMember":{"namespace":"x-namespace","key":"x-key","count":5}}}''',
      r'''{"Json":{"count":1,"members":[[97]]}}''',
      null,
      (db) => db.cacheSetRandomMembers('x-namespace', 'x-key', count: 5)),
  _Case(
      'cacheSetRandomMembers with only what it requires',
      r'''{"Cache":{"SRandMember":{"namespace":"x-namespace","key":"x-key","count":null}}}''',
      r'''{"Json":{"count":1,"members":[[97]]}}''',
      null,
      (db) => db.cacheSetRandomMembers('x-namespace', 'x-key')),
  _Case(
      'cacheHashIncrement',
      r'''{"Cache":{"HIncrBy":{"namespace":"x-namespace","key":"x-key","field":[97,98],"by":6}}}''',
      r'''{"Json":{"value":5}}''',
      null,
      (db) => db.cacheHashIncrement('x-namespace', 'x-key', 'ab', 6)),
  _Case(
      'cachePipeline',
      r'''{"Cache":{"Pipeline":{"ops":[{"Set":{"namespace":"n","key":"k","value":[118],"ttl_ms":null}},{"Get":{"namespace":"n","key":"k"}},"Ping"]}}}''',
      r'''{"Json":{"results":[{"ok":{"Json":{"set":true}}},{"ok":{"CacheValue":[120]}},{"error":"invalid request: nope"}]}}''',
      null,
      (db) => db.cachePipeline(
          [CacheOp.set('n', 'k', 'v'), CacheOp.get('n', 'k'), CacheOp.ping])),
  _Case(
      'cacheTransaction',
      r'''{"Cache":{"Transaction":{"ops":[{"Set":{"namespace":"n","key":"k","value":[118],"ttl_ms":null}},{"Get":{"namespace":"n","key":"k"}},"Ping"]}}}''',
      r'''{"Json":{"results":[{"ok":{"Json":{"set":true}}},{"ok":{"CacheValue":[120]}},{"error":"invalid request: nope"}]}}''',
      null,
      (db) => db.cacheTransaction(
          [CacheOp.set('n', 'k', 'v'), CacheOp.get('n', 'k'), CacheOp.ping])),
  _Case(
      'cacheScriptLoad',
      r'''{"Cache":{"ScriptLoad":{"name":"x-name","ops":[{"Set":{"namespace":"n","key":"k","value":[118],"ttl_ms":null}},{"Get":{"namespace":"n","key":"k"}},"Ping"]}}}''',
      r'''{"Json":{"loaded":"s","ops":2}}''',
      null,
      (db) => db.cacheScriptLoad('x-name',
          [CacheOp.set('n', 'k', 'v'), CacheOp.get('n', 'k'), CacheOp.ping])),
  _Case(
      'cacheScriptRun',
      r'''{"Cache":{"ScriptRun":{"name":"x-name","args":["p","q"]}}}''',
      r'''{"Json":{"ops":[],"results":[{"ok":{"Json":{"set":true}}},{"ok":{"CacheValue":[120]}},{"error":"invalid request: nope"}]}}''',
      null,
      (db) => db.cacheScriptRun('x-name', args: ['p', 'q'])),
  _Case(
      'cacheScriptRun with only what it requires',
      r'''{"Cache":{"ScriptRun":{"name":"x-name","args":[]}}}''',
      r'''{"Json":{"ops":[],"results":[{"ok":{"Json":{"set":true}}},{"ok":{"CacheValue":[120]}},{"error":"invalid request: nope"}]}}''',
      null,
      (db) => db.cacheScriptRun('x-name')),
  _Case('cacheHotKeys', r'''{"Cache":{"HotKeys":{"limit":3}}}''',
      r'''{"Json":{}}''', null, (db) => db.cacheHotKeys(limit: 3)),
  _Case(
      'cacheHotKeys with only what it requires',
      r'''{"Cache":{"HotKeys":{"limit":null}}}''',
      r'''{"Json":{}}''',
      null,
      (db) => db.cacheHotKeys()),
  _Case('cachePurgeExpired', r'''{"Cache":"PurgeExpired"}''',
      r'''{"Json":{"purged":1}}''', null, (db) => db.cachePurgeExpired()),
  _Case(
      'cacheBlockingLeftPop',
      r'''{"Cache":{"BLPop":{"namespace":"x-namespace","keys":["p","q"],"timeout_ms":1502}}}''',
      r'''{"CacheValues":[[108,50],[97]]}''',
      null,
      (db) => db.cacheBlockingLeftPop(
          'x-namespace', ['p', 'q'], const Duration(milliseconds: 1502))),
  _Case(
      'cacheBlockingRightPop',
      r'''{"Cache":{"BRPop":{"namespace":"x-namespace","keys":["p","q"],"timeout_ms":1502}}}''',
      r'''{"CacheValues":[[108,50],[97]]}''',
      null,
      (db) => db.cacheBlockingRightPop(
          'x-namespace', ['p', 'q'], const Duration(milliseconds: 1502))),
  _Case(
      'cacheSetNamespaceLimit',
      r'''{"Cache":{"SetNamespaceLimit":{"namespace":"x-namespace","max_keys":4,"max_bytes":5}}}''',
      r'''{"Json":{"bytes":0,"keys":0,"max_bytes":1000,"max_keys":2,"namespace":"n"}}''',
      null,
      (db) =>
          db.cacheSetNamespaceLimit('x-namespace', maxKeys: 4, maxBytes: 5)),
  _Case(
      'cacheSetNamespaceLimit with only what it requires',
      r'''{"Cache":{"SetNamespaceLimit":{"namespace":"x-namespace","max_keys":null,"max_bytes":null}}}''',
      r'''{"Json":{"bytes":0,"keys":0,"max_bytes":1000,"max_keys":2,"namespace":"n"}}''',
      null,
      (db) => db.cacheSetNamespaceLimit('x-namespace')),
  _Case(
      'cacheGetNamespaceLimit',
      r'''{"Cache":{"GetNamespaceLimit":{"namespace":"x-namespace"}}}''',
      r'''{"Json":{"bytes":0,"keys":0,"max_bytes":null,"max_keys":null,"namespace":"n"}}''',
      null,
      (db) => db.cacheGetNamespaceLimit('x-namespace')),
  _Case(
      'cachePublish',
      r'''{"Cache":{"Publish":{"channel":"x-channel","message":"x-message"}}}''',
      r'''{"Json":{"channel":"c","id":7}}''',
      null,
      (db) => db.cachePublish('x-channel', 'x-message')),
  _Case(
      'cachePoll',
      r'''{"Cache":{"Poll":{"channels":["p","q"],"after":4,"limit":5}}}''',
      r'''{"Json":{"messages":[{"at_ms":1,"channel":"c","id":7,"message":"hello"}],"next":7}}''',
      null,
      (db) => db.cachePoll(['p', 'q'], after: 4, limit: 5)),
  _Case(
      'cachePoll with only what it requires',
      r'''{"Cache":{"Poll":{"channels":["p","q"],"after":0,"limit":100}}}''',
      r'''{"Json":{"messages":[{"at_ms":1,"channel":"c","id":7,"message":"hello"}],"next":7}}''',
      null,
      (db) => db.cachePoll(['p', 'q'])),
  _Case(
      'documentReplace',
      r'''{"Document":{"Replace":{"collection":"x-collection","id":"x-id","document":{"k":1,"name":"ada"},"upsert":true,"return":"Before","expected_version":8}}}''',
      r'''{"Json":{"id":"a","inserted":false,"replaced":true}}''',
      'tx-1',
      (db) => db.documentReplace(
          'x-collection', 'x-id', {'k': 1, 'name': 'ada'},
          upsert: true,
          returnDocument: ReturnDocument.before,
          expectedVersion: 8,
          txn: 'tx-1')),
  _Case(
      'documentReplace with only what it requires',
      r'''{"Document":{"Replace":{"collection":"x-collection","id":"x-id","document":{"k":1,"name":"ada"},"upsert":false,"return":"None","expected_version":null}}}''',
      r'''{"Json":{"id":"a","inserted":false,"replaced":true}}''',
      null,
      (db) =>
          db.documentReplace('x-collection', 'x-id', {'k': 1, 'name': 'ada'})),
  _Case(
      'documentInsertMany',
      r'''{"Document":{"InsertMany":{"collection":"x-collection","documents":[{"id":"a","n":1},{"id":"b"}]}}}''',
      r'''{"Json":{"ids":["a","b"],"inserted":2}}''',
      'tx-1',
      (db) => db.documentInsertMany(
          'x-collection',
          [
            {'id': 'a', 'n': 1},
            {'id': 'b'}
          ],
          txn: 'tx-1')),
  _Case(
      'documentInsertMany with only what it requires',
      r'''{"Document":{"InsertMany":{"collection":"x-collection","documents":[{"id":"a","n":1},{"id":"b"}]}}}''',
      r'''{"Json":{"ids":["a","b"],"inserted":2}}''',
      null,
      (db) => db.documentInsertMany('x-collection', [
            {'id': 'a', 'n': 1},
            {'id': 'b'}
          ])),
  _Case(
      'documentDeleteMany',
      r'''{"Document":{"DeleteMany":{"collection":"x-collection","filter":{"Eq":{"field":"city","value":"Pune"}}}}}''',
      r'''{"Json":{"deleted":1}}''',
      'tx-1',
      (db) => db.documentDeleteMany('x-collection', Filter.eq('city', 'Pune'),
          txn: 'tx-1')),
  _Case(
      'documentDeleteMany with only what it requires',
      r'''{"Document":{"DeleteMany":{"collection":"x-collection","filter":{"Eq":{"field":"city","value":"Pune"}}}}}''',
      r'''{"Json":{"deleted":1}}''',
      null,
      (db) => db.documentDeleteMany('x-collection', Filter.eq('city', 'Pune'))),
  _Case(
      'documentRenameCollection',
      r'''{"Document":{"RenameCollection":{"collection":"x-collection","to":"x-to"}}}''',
      r'''{"Json":{"renamed":true,"documents":7,"indexes":1}}''',
      null,
      (db) => db.documentRenameCollection('x-collection', 'x-to')),
  _Case(
      'documentPurgeExpired',
      r'''{"Document":{"PurgeExpired":{"collection":"x-collection"}}}''',
      r'''{"Json":{"more":false,"purged":0}}''',
      null,
      (db) => db.documentPurgeExpired('x-collection')),
  _Case(
      'documentExplain',
      r'''{"Document":{"Explain":{"collection":"x-collection","filter":{"Eq":{"field":"city","value":"Pune"}},"limit":5,"sort":[{"id":"a","n":1},{"id":"b"}],"skip":7}}}''',
      r'''{"Json":{"plan":"FindScan","index_used":false,"rows_examined":4,"rows_returned":1}}''',
      null,
      (db) => db.documentExplain('x-collection',
          filter: Filter.eq('city', 'Pune'),
          limit: 5,
          sort: [
            {'id': 'a', 'n': 1},
            {'id': 'b'}
          ],
          skip: 7)),
  _Case(
      'documentExplain with only what it requires',
      r'''{"Document":{"Explain":{"collection":"x-collection","filter":"All","limit":null,"sort":[],"skip":null}}}''',
      r'''{"Json":{"plan":"FindScan","index_used":false,"rows_examined":4,"rows_returned":1}}''',
      null,
      (db) => db.documentExplain('x-collection')),
  _Case(
      'documentBeginTransaction',
      r'''{"Document":{"BeginTransaction":{"serializable":true}}}''',
      r'''{"Json":{"serializable":false,"txn":"dtx-1-9"}}''',
      null,
      (db) => db.documentBeginTransaction(serializable: true)),
  _Case(
      'documentBeginTransaction with only what it requires',
      r'''{"Document":{"BeginTransaction":{"serializable":false}}}''',
      r'''{"Json":{"serializable":false,"txn":"dtx-1-9"}}''',
      null,
      (db) => db.documentBeginTransaction()),
  _Case(
      'documentCommitTransaction',
      r'''{"Document":"CommitTransaction"}''',
      r'''{"Json":{"committed":true,"writes":2}}''',
      'tx-1',
      (db) => db.documentCommitTransaction('tx-1')),
  _Case(
      'documentRollbackTransaction',
      r'''{"Document":"RollbackTransaction"}''',
      r'''{"Json":{"rolled_back":true}}''',
      'tx-1',
      (db) => db.documentRollbackTransaction('tx-1')),
  _Case(
      'documentRebuildIndex',
      r'''{"Document":{"RebuildIndex":{"collection":"x-collection","index_name":"x-index_name"}}}''',
      r'''{"Json":{"documents":3,"entries":3,"rebuilt":"by_n"}}''',
      null,
      (db) => db.documentRebuildIndex('x-collection', 'x-index_name')),
  _Case(
      'documentSetCollectionOptions',
      r'''{"Document":{"SetCollectionOptions":{"collection":"x-collection","options":{"k":1,"name":"ada"}}}}''',
      r'''{"Json":{"documents_checked":3,"updated":true}}''',
      null,
      (db) => db.documentSetCollectionOptions(
          'x-collection', {'k': 1, 'name': 'ada'})),
  _Case(
      'documentGetCollectionOptions',
      r'''{"Document":{"GetCollectionOptions":{"collection":"x-collection"}}}''',
      r'''{"Json":{"collection":"c","options":{"versioned":false}}}''',
      null,
      (db) => db.documentGetCollectionOptions('x-collection')),
  _Case(
      'documentBulkWrite',
      r'''{"Document":{"BulkWrite":{"collection":"x-collection","ops":[{"Insert":{"id":"a","document":{"n":1}}},{"UpdateOne":{"id":"a","update":{"set":{"n":2},"inc":{"m":1}},"upsert":true,"expected_version":3}},{"Replace":{"id":"a","document":{"n":3},"upsert":false,"expected_version":null}},{"Delete":{"id":"a","expected_version":null}}]}}}''',
      r'''{"Json":{"deleted":1,"inserted":2,"inserted_ids":["b1","x"],"replaced":1,"updated":1,"upserted":1}}''',
      'tx-1',
      (db) => db.documentBulkWrite(
          'x-collection',
          [
            BulkOp.insert({'n': 1}, id: 'a'),
            BulkOp.updateOne('a',
                set: {'n': 2},
                increment: {'m': 1},
                upsert: true,
                expectedVersion: 3),
            BulkOp.replace('a', {'n': 3}),
            BulkOp.delete('a')
          ],
          txn: 'tx-1')),
  _Case(
      'documentBulkWrite with only what it requires',
      r'''{"Document":{"BulkWrite":{"collection":"x-collection","ops":[{"Insert":{"id":"a","document":{"n":1}}},{"UpdateOne":{"id":"a","update":{"set":{"n":2},"inc":{"m":1}},"upsert":true,"expected_version":3}},{"Replace":{"id":"a","document":{"n":3},"upsert":false,"expected_version":null}},{"Delete":{"id":"a","expected_version":null}}]}}}''',
      r'''{"Json":{"deleted":1,"inserted":2,"inserted_ids":["b1","x"],"replaced":1,"updated":1,"upserted":1}}''',
      null,
      (db) => db.documentBulkWrite('x-collection', [
            BulkOp.insert({'n': 1}, id: 'a'),
            BulkOp.updateOne('a',
                set: {'n': 2},
                increment: {'m': 1},
                upsert: true,
                expectedVersion: 3),
            BulkOp.replace('a', {'n': 3}),
            BulkOp.delete('a')
          ])),
  _Case(
      'graphAddMany',
      r'''{"Graph":{"AddMany":{"graph":"x-graph","nodes":[{"id":"a","n":1},{"id":"b"}],"edges":[{"id":"a","n":1},{"id":"b"}]}}}''',
      r'''{"Json":{"atomic":true,"edges":5,"nodes":5}}''',
      null,
      (db) => db.graphAddMany('x-graph', nodes: [
            {'id': 'a', 'n': 1},
            {'id': 'b'}
          ], edges: [
            {'id': 'a', 'n': 1},
            {'id': 'b'}
          ])),
  _Case(
      'graphAddMany with only what it requires',
      r'''{"Graph":{"AddMany":{"graph":"x-graph","nodes":[],"edges":[]}}}''',
      r'''{"Json":{"atomic":true,"edges":5,"nodes":5}}''',
      null,
      (db) => db.graphAddMany('x-graph')),
  _Case(
      'graphRemoveMany',
      r'''{"Graph":{"RemoveMany":{"graph":"x-graph","nodes":["p","q"],"edges":["p","q"],"detach":true}}}''',
      r'''{"Json":{"atomic":true,"edges_removed":2,"nodes_removed":1}}''',
      null,
      (db) => db.graphRemoveMany('x-graph',
          nodes: ['p', 'q'], edges: ['p', 'q'], detach: true)),
  _Case(
      'graphRemoveMany with only what it requires',
      r'''{"Graph":{"RemoveMany":{"graph":"x-graph","nodes":[],"edges":[],"detach":false}}}''',
      r'''{"Json":{"atomic":true,"edges_removed":2,"nodes_removed":1}}''',
      null,
      (db) => db.graphRemoveMany('x-graph')),
  _Case(
      'graphUpdateNode',
      r'''{"Graph":{"UpdateNode":{"graph":"x-graph","id":"x-id","properties":{"k":1,"name":"ada"},"add_labels":["p","q"],"remove_labels":["p","q"]}}}''',
      r'''{"Json":{"id":"a","labels":["Person"],"properties":{"city":"Pune"}}}''',
      null,
      (db) => db.graphUpdateNode('x-graph', 'x-id',
          properties: {'k': 1, 'name': 'ada'},
          addLabels: ['p', 'q'],
          removeLabels: ['p', 'q'])),
  _Case(
      'graphUpdateNode with only what it requires',
      r'''{"Graph":{"UpdateNode":{"graph":"x-graph","id":"x-id","properties":{},"add_labels":[],"remove_labels":[]}}}''',
      r'''{"Json":{"id":"a","labels":["Person"],"properties":{"city":"Pune"}}}''',
      null,
      (db) => db.graphUpdateNode('x-graph', 'x-id')),
  _Case(
      'graphUpdateEdge',
      r'''{"Graph":{"UpdateEdge":{"graph":"x-graph","id":"x-id","properties":{"k":1,"name":"ada"},"label":"x-label"}}}''',
      r'''{"Json":{"id":"e4","label":"LOVES","properties":{"since":2020},"to":"c","from":"a"}}''',
      null,
      (db) => db.graphUpdateEdge('x-graph', 'x-id',
          properties: {'k': 1, 'name': 'ada'}, label: 'x-label')),
  _Case(
      'graphUpdateEdge with only what it requires',
      r'''{"Graph":{"UpdateEdge":{"graph":"x-graph","id":"x-id","properties":{},"label":null}}}''',
      r'''{"Json":{"id":"e4","label":"LOVES","properties":{"since":2020},"to":"c","from":"a"}}''',
      null,
      (db) => db.graphUpdateEdge('x-graph', 'x-id')),
  _Case(
      'graphCopy',
      r'''{"Graph":{"CopyGraph":{"graph":"x-graph","to":"x-to"}}}''',
      r'''{"Json":{"copied":true,"edges":4,"nodes":8,"to":"g2"}}''',
      null,
      (db) => db.graphCopy('x-graph', 'x-to')),
  _Case(
      'graphDfs',
      r'''{"Graph":{"Dfs":{"graph":"x-graph","start":"x-start","direction":"incoming","label":"x-label","max_depth":7,"limit":8}}}''',
      r'''{"Json":{"count":1,"nodes":[{"depth":0,"id":"a"}],"order":"depth_first","truncated":false}}''',
      null,
      (db) => db.graphDfs('x-graph', 'x-start',
          direction: GraphDirection.incoming,
          label: 'x-label',
          maxDepth: 7,
          limit: 8)),
  _Case(
      'graphDfs with only what it requires',
      r'''{"Graph":{"Dfs":{"graph":"x-graph","start":"x-start","direction":"outgoing","label":null,"max_depth":null,"limit":null}}}''',
      r'''{"Json":{"count":1,"nodes":[{"depth":0,"id":"a"}],"order":"depth_first","truncated":false}}''',
      null,
      (db) => db.graphDfs('x-graph', 'x-start')),
  _Case(
      'graphConnectedComponents',
      r'''{"Graph":{"ConnectedComponents":{"graph":"x-graph","kind":"strong","label":"x-label","limit":6}}}''',
      r'''{"Json":{"components":[{"nodes":["a","b"],"size":2,"truncated":false}],"count":1,"kind":"weak","largest":2,"truncated":false}}''',
      null,
      (db) => db.graphConnectedComponents('x-graph',
          kind: ComponentKind.strong, label: 'x-label', limit: 6)),
  _Case(
      'graphConnectedComponents with only what it requires',
      r'''{"Graph":{"ConnectedComponents":{"graph":"x-graph","kind":"weak","label":null,"limit":null}}}''',
      r'''{"Json":{"components":[{"nodes":["a","b"],"size":2,"truncated":false}],"count":1,"kind":"weak","largest":2,"truncated":false}}''',
      null,
      (db) => db.graphConnectedComponents('x-graph')),
  _Case(
      'graphDetectCycle',
      r'''{"Graph":{"DetectCycle":{"graph":"x-graph","directed":false,"label":"x-label"}}}''',
      r'''{"Json":{"cycle":{"edges":["e1"],"nodes":["a"]},"directed":true,"hasCycle":true}}''',
      null,
      (db) =>
          db.graphDetectCycle('x-graph', directed: false, label: 'x-label')),
  _Case(
      'graphDetectCycle with only what it requires',
      r'''{"Graph":{"DetectCycle":{"graph":"x-graph","directed":true,"label":null}}}''',
      r'''{"Json":{"cycle":{"edges":["e1"],"nodes":["a"]},"directed":true,"hasCycle":true}}''',
      null,
      (db) => db.graphDetectCycle('x-graph')),
  _Case(
      'graphStats',
      r'''{"Graph":{"GraphStats":{"graph":"x-graph"}}}''',
      r'''{"Json":{"nodes":5,"edges":5,"selfLoops":0,"danglingEdges":0}}''',
      null,
      (db) => db.graphStats('x-graph')),
  _Case(
      'graphSubgraph',
      r'''{"Graph":{"Subgraph":{"graph":"x-graph","seeds":["p","q"],"direction":"incoming","label":"x-label","max_depth":7,"limit":8}}}''',
      r'''{"Json":{"node_count":1,"edge_count":0,"nodes":[{"depth":0,"id":"a"}],"edges":[]}}''',
      null,
      (db) => db.graphSubgraph('x-graph', ['p', 'q'],
          direction: GraphDirection.incoming,
          label: 'x-label',
          maxDepth: 7,
          limit: 8)),
  _Case(
      'graphSubgraph with only what it requires',
      r'''{"Graph":{"Subgraph":{"graph":"x-graph","seeds":["p","q"],"direction":"both","label":null,"max_depth":null,"limit":null}}}''',
      r'''{"Json":{"node_count":1,"edge_count":0,"nodes":[{"depth":0,"id":"a"}],"edges":[]}}''',
      null,
      (db) => db.graphSubgraph('x-graph', ['p', 'q'])),
  _Case(
      'graphSchema',
      r'''{"Graph":{"GraphSchema":{"graph":"x-graph"}}}''',
      r'''{"Json":{"nodes":5,"edges":5,"node_labels":[],"edge_types":[]}}''',
      null,
      (db) => db.graphSchema('x-graph')),
  _Case(
      'graphKShortestPaths',
      r'''{"Graph":{"KShortestPaths":{"graph":"x-graph","from":"x-from","to":"x-to","k":6,"direction":"incoming","label":"x-label","weight_property":"x-weight_property"}}}''',
      r'''{"Json":{"count":1,"k":3,"paths":[{"cost":1.0,"edges":["e4"],"hops":1,"nodes":["a","c"]}]}}''',
      null,
      (db) => db.graphKShortestPaths('x-graph', 'x-from', 'x-to',
          k: 6,
          direction: GraphDirection.incoming,
          label: 'x-label',
          weightProperty: 'x-weight_property')),
  _Case(
      'graphKShortestPaths with only what it requires',
      r'''{"Graph":{"KShortestPaths":{"graph":"x-graph","from":"x-from","to":"x-to","k":null,"direction":"outgoing","label":null,"weight_property":null}}}''',
      r'''{"Json":{"count":1,"k":3,"paths":[{"cost":1.0,"edges":["e4"],"hops":1,"nodes":["a","c"]}]}}''',
      null,
      (db) => db.graphKShortestPaths('x-graph', 'x-from', 'x-to')),
  _Case(
      'graphAllPaths',
      r'''{"Graph":{"AllPaths":{"graph":"x-graph","from":"x-from","to":"x-to","direction":"incoming","label":"x-label","max_depth":8,"limit":9}}}''',
      r'''{"Json":{"complete":true,"count":1,"paths":[{"cost":1.0,"edges":["e4"],"hops":1,"nodes":["a","c"]}],"stopped_by":null}}''',
      null,
      (db) => db.graphAllPaths('x-graph', 'x-from', 'x-to',
          direction: GraphDirection.incoming,
          label: 'x-label',
          maxDepth: 8,
          limit: 9)),
  _Case(
      'graphAllPaths with only what it requires',
      r'''{"Graph":{"AllPaths":{"graph":"x-graph","from":"x-from","to":"x-to","direction":"outgoing","label":null,"max_depth":null,"limit":null}}}''',
      r'''{"Json":{"complete":true,"count":1,"paths":[{"cost":1.0,"edges":["e4"],"hops":1,"nodes":["a","c"]}],"stopped_by":null}}''',
      null,
      (db) => db.graphAllPaths('x-graph', 'x-from', 'x-to')),
  _Case(
      'graphCentrality',
      r'''{"Graph":{"Centrality":{"graph":"x-graph","measure":"closeness","directed":false,"label":"x-label","limit":7,"iterations":8,"tolerance":7.5}}}''',
      r'''{"Json":{"directed":true,"measure":"betweenness","nodes":5,"results":[{"id":"c","score":0.25}]}}''',
      null,
      (db) => db.graphCentrality('x-graph', CentralityMeasure.closeness,
          directed: false,
          label: 'x-label',
          limit: 7,
          iterations: 8,
          tolerance: 7.5)),
  _Case(
      'graphCentrality with only what it requires',
      r'''{"Graph":{"Centrality":{"graph":"x-graph","measure":"closeness","directed":true,"label":null,"limit":null,"iterations":null,"tolerance":null}}}''',
      r'''{"Json":{"directed":true,"measure":"betweenness","nodes":5,"results":[{"id":"c","score":0.25}]}}''',
      null,
      (db) => db.graphCentrality('x-graph', CentralityMeasure.closeness)),
  _Case(
      'graphCommunities',
      r'''{"Graph":{"Communities":{"graph":"x-graph","label":"x-label","iterations":5,"limit":6}}}''',
      r'''{"Json":{"algorithm":"louvain","communities":[{"nodes":["a","b"],"size":2,"truncated":false}],"count":1,"modularity":0.0}}''',
      null,
      (db) => db.graphCommunities('x-graph',
          label: 'x-label', iterations: 5, limit: 6)),
  _Case(
      'graphCommunities with only what it requires',
      r'''{"Graph":{"Communities":{"graph":"x-graph","label":null,"iterations":null,"limit":null}}}''',
      r'''{"Json":{"algorithm":"louvain","communities":[{"nodes":["a","b"],"size":2,"truncated":false}],"count":1,"modularity":0.0}}''',
      null,
      (db) => db.graphCommunities('x-graph')),
  _Case(
      'graphTriangleCount',
      r'''{"Graph":{"TriangleCount":{"graph":"x-graph","label":"x-label","limit":5}}}''',
      r'''{"Json":{"triangles":1,"transitivity":0.6,"average_clustering":0.46,"nodes":[{"clustering":1.0,"id":"a","triangles":1}]}}''',
      null,
      (db) => db.graphTriangleCount('x-graph', label: 'x-label', limit: 5)),
  _Case(
      'graphTriangleCount with only what it requires',
      r'''{"Graph":{"TriangleCount":{"graph":"x-graph","label":null,"limit":null}}}''',
      r'''{"Json":{"triangles":1,"transitivity":0.6,"average_clustering":0.46,"nodes":[{"clustering":1.0,"id":"a","triangles":1}]}}''',
      null,
      (db) => db.graphTriangleCount('x-graph')),
  _Case(
      'graphNodeSimilarity',
      r'''{"Graph":{"NodeSimilarity":{"graph":"x-graph","node":"x-node","direction":"incoming","label":"x-label","metric":"overlap","limit":8}}}''',
      r'''{"Json":{"count":1,"metric":"jaccard","node":"a","results":[{"id":"d","score":0.5}]}}''',
      null,
      (db) => db.graphNodeSimilarity('x-graph', 'x-node',
          direction: GraphDirection.incoming,
          label: 'x-label',
          metric: SimilarityMetric.overlap,
          limit: 8)),
  _Case(
      'graphNodeSimilarity with only what it requires',
      r'''{"Graph":{"NodeSimilarity":{"graph":"x-graph","node":"x-node","direction":"both","label":null,"metric":"jaccard","limit":null}}}''',
      r'''{"Json":{"count":1,"metric":"jaccard","node":"a","results":[{"id":"d","score":0.5}]}}''',
      null,
      (db) => db.graphNodeSimilarity('x-graph', 'x-node')),
  _Case(
      'graphCheckConsistency',
      r'''{"Graph":{"CheckConsistency":{"graph":"x-graph"}}}''',
      r'''{"Json":{"consistent":true,"nodes":5,"edges":5}}''',
      null,
      (db) => db.graphCheckConsistency('x-graph')),
  _Case(
      'graphCreateNodeIndex',
      r'''{"Graph":{"CreateNodeIndex":{"graph":"x-graph","name":"x-name","properties":["p","q"],"label":"x-label"}}}''',
      r'''{"Json":{"created":true,"name":"by_name"}}''',
      null,
      (db) => db.graphCreateNodeIndex('x-graph', 'x-name', ['p', 'q'],
          label: 'x-label')),
  _Case(
      'graphCreateNodeIndex with only what it requires',
      r'''{"Graph":{"CreateNodeIndex":{"graph":"x-graph","name":"x-name","properties":["p","q"],"label":null}}}''',
      r'''{"Json":{"created":true,"name":"by_name"}}''',
      null,
      (db) => db.graphCreateNodeIndex('x-graph', 'x-name', ['p', 'q'])),
  _Case(
      'graphCreateConstraint',
      r'''{"Graph":{"CreateConstraint":{"graph":"x-graph","name":"x-name","label":"x-label","property":"x-property","kind":"required","target":"edge"}}}''',
      r'''{"Json":{"created":true,"kind":"unique","name":"u","target":"node"}}''',
      null,
      (db) => db.graphCreateConstraint(
          'x-graph', 'x-name', 'x-label', 'x-property', ConstraintKind.required,
          target: ConstraintTarget.edge)),
  _Case(
      'graphCreateConstraint with only what it requires',
      r'''{"Graph":{"CreateConstraint":{"graph":"x-graph","name":"x-name","label":"x-label","property":"x-property","kind":"required","target":"node"}}}''',
      r'''{"Json":{"created":true,"kind":"unique","name":"u","target":"node"}}''',
      null,
      (db) => db.graphCreateConstraint('x-graph', 'x-name', 'x-label',
          'x-property', ConstraintKind.required)),
  _Case(
      'graphDropSchemaObject',
      r'''{"Graph":{"DropSchemaObject":{"graph":"x-graph","name":"x-name"}}}''',
      r'''{"Json":{"dropped":true,"name":"u"}}''',
      null,
      (db) => db.graphDropSchemaObject('x-graph', 'x-name')),
  _Case(
      'graphListSchemaObjects',
      r'''{"Graph":{"ListSchemaObjects":{"graph":"x-graph"}}}''',
      r'''{"Json":{"constraints":[],"indexes":[{"label":"Person","name":"by_name","properties":["name"]}]}}''',
      null,
      (db) => db.graphListSchemaObjects('x-graph')),
  _Case(
      'graphFindNodes',
      r'''{"Graph":{"FindNodes":{"graph":"x-graph","label":"x-label","properties":{"k":1,"name":"ada"},"limit":6}}}''',
      r'''{"Json":{"count":1,"nodes":[{"id":"a","labels":["Person"],"properties":{"name":"ada"}}],"plan":"scan","truncated":false}}''',
      null,
      (db) => db.graphFindNodes('x-graph',
          label: 'x-label', properties: {'k': 1, 'name': 'ada'}, limit: 6)),
  _Case(
      'graphFindNodes with only what it requires',
      r'''{"Graph":{"FindNodes":{"graph":"x-graph","label":null,"properties":{},"limit":null}}}''',
      r'''{"Json":{"count":1,"nodes":[{"id":"a","labels":["Person"],"properties":{"name":"ada"}}],"plan":"scan","truncated":false}}''',
      null,
      (db) => db.graphFindNodes('x-graph')),
  _Case(
      'graphImportCsv',
      r'''{"Graph":{"ImportCsv":{"graph":"x-graph","nodes":"x-nodes","edges":"x-edges"}}}''',
      r'''{"Json":{"atomic":true,"edges":1,"nodes":2}}''',
      null,
      (db) => db.graphImportCsv('x-graph', nodes: 'x-nodes', edges: 'x-edges')),
  _Case(
      'graphImportCsv with only what it requires',
      r'''{"Graph":{"ImportCsv":{"graph":"x-graph","nodes":null,"edges":null}}}''',
      r'''{"Json":{"atomic":true,"edges":1,"nodes":2}}''',
      null,
      (db) => db.graphImportCsv('x-graph')),
  _Case(
      'graphPageRank',
      r'''{"Graph":{"PageRank":{"graph":"x-graph","damping":2.5,"iterations":5,"tolerance":4.5,"label":"x-label","limit":8}}}''',
      r'''{"Json":{"converged":true,"count":1,"damping":0.85,"iterations":19,"ranks":[{"id":"c","score":0.25}],"truncated":false}}''',
      null,
      (db) => db.graphPageRank('x-graph',
          damping: 2.5,
          iterations: 5,
          tolerance: 4.5,
          label: 'x-label',
          limit: 8)),
  _Case(
      'graphPageRank with only what it requires',
      r'''{"Graph":{"PageRank":{"graph":"x-graph","damping":null,"iterations":null,"tolerance":null,"label":null,"limit":null}}}''',
      r'''{"Json":{"converged":true,"count":1,"damping":0.85,"iterations":19,"ranks":[{"id":"c","score":0.25}],"truncated":false}}''',
      null,
      (db) => db.graphPageRank('x-graph')),
  _Case(
      'graphExecute',
      r'''{"Graph":{"Execute":{"graph":"x-graph","cypher":"x-cypher"}}}''',
      r'''{"Json":{"graph":"g","nodesCreated":1,"statements":1}}''',
      'tx-1',
      (db) => db.graphExecute('x-graph', 'x-cypher', txn: 'tx-1')),
  _Case(
      'graphExecute with only what it requires',
      r'''{"Graph":{"Execute":{"graph":"x-graph","cypher":"x-cypher"}}}''',
      r'''{"Json":{"graph":"g","nodesCreated":1,"statements":1}}''',
      null,
      (db) => db.graphExecute('x-graph', 'x-cypher')),
  _Case(
      'graphBeginTransaction',
      r'''{"Graph":{"BeginTransaction":{"graph":"x-graph"}}}''',
      r'''{"Json":{"transaction":"gtx-1-11"}}''',
      null,
      (db) => db.graphBeginTransaction('x-graph')),
  _Case(
      'graphCommitTransaction',
      r'''{"Graph":{"CommitTransaction":{"graph":"x-graph"}}}''',
      r'''{"Json":{"committed":1,"statements":1,"nodesCreated":1}}''',
      'tx-1',
      (db) => db.graphCommitTransaction('x-graph', 'tx-1')),
  _Case(
      'graphRollbackTransaction',
      r'''{"Graph":{"RollbackTransaction":{"graph":"x-graph"}}}''',
      r'''{"Json":{"rolled_back":1}}''',
      'tx-1',
      (db) => db.graphRollbackTransaction('x-graph', 'tx-1')),
  _Case(
      'vectorEmbed',
      r'''{"Vector":{"Embed":{"texts":["p","q"],"dimension":4}}}''',
      r'''{"Json":{"dimension":4,"embedder":"hash-v1","vectors":[[0.5,0.5,0.0,-0.5]],"sparse":[]}}''',
      null,
      (db) => db.vectorEmbed(['p', 'q'], 4)),
  _Case(
      'vectorUpsertText',
      r'''{"Vector":{"UpsertText":{"collection":"x-collection","items":[{"id":"a","n":1},{"id":"b"}]}}}''',
      r'''{"Json":{"embedder":"hash-v1","upserted":2}}''',
      null,
      (db) => db.vectorUpsertText('x-collection', [
            {'id': 'a', 'n': 1},
            {'id': 'b'}
          ])),
  _Case(
      'vectorSetAlias',
      r'''{"Vector":{"SetAlias":{"alias":"x-alias","collection":"x-collection"}}}''',
      r'''{"Json":{"alias":"a","collection":"c"}}''',
      null,
      (db) => db.vectorSetAlias('x-alias', collection: 'x-collection')),
  _Case(
      'vectorSetAlias with only what it requires',
      r'''{"Vector":{"SetAlias":{"alias":"x-alias","collection":null}}}''',
      r'''{"Json":{"alias":"a","collection":"c"}}''',
      null,
      (db) => db.vectorSetAlias('x-alias')),
  _Case(
      'vectorListAliases',
      r'''{"Vector":"ListAliases"}''',
      r'''{"Json":{"aliases":[{"alias":"a","collection":"c"}]}}''',
      null,
      (db) => db.vectorListAliases()),
  _Case(
      'vectorAddNamedVector',
      r'''{"Vector":{"AddNamedVector":{"collection":"x-collection","name":"x-name","dimension":5,"metric":"dot"}}}''',
      r'''{"Json":{"collection":"c","dimension":2,"name":"title"}}''',
      null,
      (db) => db.vectorAddNamedVector('x-collection', 'x-name', 5,
          metric: VectorMetric.dot)),
  _Case(
      'vectorAddNamedVector with only what it requires',
      r'''{"Vector":{"AddNamedVector":{"collection":"x-collection","name":"x-name","dimension":5,"metric":"cosine"}}}''',
      r'''{"Json":{"collection":"c","dimension":2,"name":"title"}}''',
      null,
      (db) => db.vectorAddNamedVector('x-collection', 'x-name', 5)),
  _Case(
      'vectorUpsertNamed',
      r'''{"Vector":{"UpsertNamed":{"collection":"x-collection","id":"x-id","vectors":{"default":[1.0,0.0],"title":[0.5]},"metadata":{"k":1,"name":"ada"}}}}''',
      r'''{"Json":{"id":"n1","vectors":2}}''',
      null,
      (db) => db.vectorUpsertNamed('x-collection', 'x-id', {
            'default': [1.0, 0.0],
            'title': [0.5]
          }, metadata: {
            'k': 1,
            'name': 'ada'
          })),
  _Case(
      'vectorUpsertNamed with only what it requires',
      r'''{"Vector":{"UpsertNamed":{"collection":"x-collection","id":"x-id","vectors":{"default":[1.0,0.0],"title":[0.5]},"metadata":null}}}''',
      r'''{"Json":{"id":"n1","vectors":2}}''',
      null,
      (db) => db.vectorUpsertNamed('x-collection', 'x-id', {
            'default': [1.0, 0.0],
            'title': [0.5]
          })),
  _Case(
      'vectorUpsertMany',
      r'''{"Vector":{"UpsertMany":{"collection":"x-collection","items":[{"id":"a","n":1},{"id":"b"}]}}}''',
      r'''{"Json":{"upserted":4}}''',
      null,
      (db) => db.vectorUpsertMany('x-collection', [
            {'id': 'a', 'n': 1},
            {'id': 'b'}
          ])),
  _Case(
      'vectorDeleteMany',
      r'''{"Vector":{"DeleteMany":{"collection":"x-collection","ids":["p","q"]}}}''',
      r'''{"Json":{"deleted":1}}''',
      null,
      (db) => db.vectorDeleteMany('x-collection', ['p', 'q'])),
  _Case(
      'vectorSearchBatch',
      r'''{"Vector":{"SearchBatch":{"collection":"x-collection","vectors":[[1.0,0.0],[0.0,1.0]],"top_k":5,"filter":{"k":1,"name":"ada"},"min_score":5.5,"include_vectors":true,"offset":2,"exact":true,"using":"title"}}}''',
      r'''{"Json":{"count":1,"results":[[{"id":"a","metadata":{"kind":"x"},"score":1.0}]]}}''',
      null,
      (db) => db.vectorSearchBatch(
          'x-collection',
          [
            [1.0, 0.0],
            [0.0, 1.0]
          ],
          5,
          filter: {'k': 1, 'name': 'ada'},
          minScore: 5.5,
          includeVectors: true,
          options: const VectorSearchOptions(
              offset: 2, exact: true, using: 'title'))),
  _Case(
      'vectorSearchBatch with only what it requires',
      r'''{"Vector":{"SearchBatch":{"collection":"x-collection","vectors":[[1.0,0.0],[0.0,1.0]],"top_k":5,"filter":null,"min_score":null,"include_vectors":false}}}''',
      r'''{"Json":{"count":1,"results":[[{"id":"a","metadata":{"kind":"x"},"score":1.0}]]}}''',
      null,
      (db) => db.vectorSearchBatch(
          'x-collection',
          [
            [1.0, 0.0],
            [0.0, 1.0]
          ],
          5)),
  _Case(
      'vectorDeleteByFilter',
      r'''{"Vector":{"DeleteByFilter":{"collection":"x-collection","filter":{"k":1,"name":"ada"}}}}''',
      r'''{"Json":{"deleted":1}}''',
      null,
      (db) => db.vectorDeleteByFilter('x-collection', {'k': 1, 'name': 'ada'})),
  _Case(
      'vectorUpdateByFilter',
      r'''{"Vector":{"UpdateByFilter":{"collection":"x-collection","filter":{"k":1,"name":"ada"},"set":{"k":1,"name":"ada"}}}}''',
      r'''{"Json":{"updated":2}}''',
      null,
      (db) => db.vectorUpdateByFilter(
          'x-collection', {'k': 1, 'name': 'ada'}, {'k': 1, 'name': 'ada'})),
  _Case(
      'vectorCount',
      r'''{"Vector":{"Count":{"collection":"x-collection","filter":{"k":1,"name":"ada"}}}}''',
      r'''{"Json":{"count":4,"filtered":false}}''',
      null,
      (db) => db.vectorCount('x-collection', filter: {'k': 1, 'name': 'ada'})),
  _Case(
      'vectorCount with only what it requires',
      r'''{"Vector":{"Count":{"collection":"x-collection","filter":null}}}''',
      r'''{"Json":{"count":4,"filtered":false}}''',
      null,
      (db) => db.vectorCount('x-collection')),
  _Case(
      'vectorListPartitions',
      r'''{"Vector":{"ListPartitions":{"collection":"x-collection","field":"x-field"}}}''',
      r'''{"Json":{"field":"_namespace","partitions":[{"count":2,"name":"n1"}],"unpartitioned":1}}''',
      null,
      (db) => db.vectorListPartitions('x-collection', field: 'x-field')),
  _Case(
      'vectorListPartitions with only what it requires',
      r'''{"Vector":{"ListPartitions":{"collection":"x-collection","field":"_namespace"}}}''',
      r'''{"Json":{"field":"_namespace","partitions":[{"count":2,"name":"n1"}],"unpartitioned":1}}''',
      null,
      (db) => db.vectorListPartitions('x-collection')),
  _Case(
      'vectorRenameCollection',
      r'''{"Vector":{"RenameCollection":{"collection":"x-collection","to":"x-to"}}}''',
      r'''{"Json":{"renamed":true}}''',
      null,
      (db) => db.vectorRenameCollection('x-collection', 'x-to')),
  _Case(
      'vectorRebuildIndex',
      r'''{"Vector":{"RebuildIndex":{"collection":"x-collection"}}}''',
      r'''{"Json":{"build_ms":5,"persisted":true,"rebuilt":true,"tombstones_cleared":0,"vectors":5}}''',
      null,
      (db) => db.vectorRebuildIndex('x-collection')),
  _Case(
      'vectorIndexStats',
      r'''{"Vector":{"IndexStats":{"collection":"x-collection"}}}''',
      r'''{"Json":{"collection":"c","flat_scan_threshold":256,"index":"flat","stored":5}}''',
      null,
      (db) => db.vectorIndexStats('x-collection')),
  _Case(
      'vectorMeasureRecall',
      r'''{"Vector":{"MeasureRecall":{"collection":"x-collection","sample":4,"top_k":5,"ef_search":6}}}''',
      r'''{"Json":{"collection":"c","index":"flat","measured":false,"recall":1.0}}''',
      null,
      (db) => db.vectorMeasureRecall('x-collection',
          sample: 4, topK: 5, efSearch: 6)),
  _Case(
      'vectorMeasureRecall with only what it requires',
      r'''{"Vector":{"MeasureRecall":{"collection":"x-collection","sample":null,"top_k":null,"ef_search":null}}}''',
      r'''{"Json":{"collection":"c","index":"flat","measured":false,"recall":1.0}}''',
      null,
      (db) => db.vectorMeasureRecall('x-collection')),
  _Case(
      'vectorSearchMulti',
      r'''{"Vector":{"SearchMulti":{"collection":"x-collection","vectors":[[1.0,0.0],[0.0,1.0]],"top_k":5,"fusion":"max","filter":{"k":1,"name":"ada"}}}}''',
      r'''{"Json":{"depth":12,"fusion":"rrf","queries":2,"results":[{"id":"a","matched":2,"metadata":{"kind":"x"},"score":0.03}]}}''',
      null,
      (db) => db.vectorSearchMulti(
          'x-collection',
          [
            [1.0, 0.0],
            [0.0, 1.0]
          ],
          5,
          fusion: FusionMethod.max,
          filter: {'k': 1, 'name': 'ada'})),
  _Case(
      'vectorSearchMulti with only what it requires',
      r'''{"Vector":{"SearchMulti":{"collection":"x-collection","vectors":[[1.0,0.0],[0.0,1.0]],"top_k":5,"fusion":"rrf","filter":null}}}''',
      r'''{"Json":{"depth":12,"fusion":"rrf","queries":2,"results":[{"id":"a","matched":2,"metadata":{"kind":"x"},"score":0.03}]}}''',
      null,
      (db) => db.vectorSearchMulti(
          'x-collection',
          [
            [1.0, 0.0],
            [0.0, 1.0]
          ],
          5)),
  _Case(
      'vectorSetCollectionConfig',
      r'''{"Vector":{"SetCollectionConfig":{"collection":"x-collection","normalize":true,"m":5,"ef_construction":6,"multi_tenant":true}}}''',
      r'''{"Json":{"collection":"c","ef_construction":100,"m":8,"multi_tenant":false,"named":[],"normalize":false}}''',
      null,
      (db) => db.vectorSetCollectionConfig('x-collection',
          normalize: true, m: 5, efConstruction: 6, multiTenant: true)),
  _Case(
      'vectorSetCollectionConfig with only what it requires',
      r'''{"Vector":{"SetCollectionConfig":{"collection":"x-collection","normalize":null,"m":null,"ef_construction":null,"multi_tenant":null}}}''',
      r'''{"Json":{"collection":"c","ef_construction":100,"m":8,"multi_tenant":false,"named":[],"normalize":false}}''',
      null,
      (db) => db.vectorSetCollectionConfig('x-collection')),
  _Case(
      'vectorCopyCollection',
      r'''{"Vector":{"CopyCollection":{"collection":"x-collection","to":"x-to"}}}''',
      r'''{"Json":{"copied":true}}''',
      null,
      (db) => db.vectorCopyCollection('x-collection', 'x-to')),
  _Case(
      'vectorSearchByExamples',
      r'''{"Vector":{"SearchByExamples":{"collection":"x-collection","top_k":4,"positive":["a",[0.0,1.0]],"negative":["a",[0.0,1.0]],"filter":{"k":1,"name":"ada"},"min_score":6.5,"include_vectors":true,"exclude_examples":false,"offset":2,"exact":true,"using":"title"}}}''',
      r'''{"Json":{"examples":{"negative":1,"positive":2},"index":"flat","results":[{"id":"d","metadata":{"kind":"x"},"score":0.63}],"top_k":5}}''',
      null,
      (db) => db.vectorSearchByExamples('x-collection', 4,
          positive: [
            'a',
            [0.0, 1.0]
          ],
          negative: [
            'a',
            [0.0, 1.0]
          ],
          filter: {'k': 1, 'name': 'ada'},
          minScore: 6.5,
          includeVectors: true,
          excludeExamples: false,
          options: const VectorSearchOptions(
              offset: 2, exact: true, using: 'title'))),
  _Case(
      'vectorSearchByExamples with only what it requires',
      r'''{"Vector":{"SearchByExamples":{"collection":"x-collection","top_k":4,"positive":[],"negative":[],"filter":null,"min_score":null,"include_vectors":false,"exclude_examples":true}}}''',
      r'''{"Json":{"examples":{"negative":1,"positive":2},"index":"flat","results":[{"id":"d","metadata":{"kind":"x"},"score":0.63}],"top_k":5}}''',
      null,
      (db) => db.vectorSearchByExamples('x-collection', 4)),
];
