import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:tricoredb/tricoredb.dart';

/// A peer serving any number of connections; [reply] answers the n-th request.
class Peer {
  final ServerSocket _server;
  final Object? Function(String requestId, int n) reply;
  final List<String> users = [];
  final List<String> requestIds = [];
  int connections = 0;

  Peer._(this._server, this.reply) {
    _server.listen(_serve);
  }

  static Future<Peer> start(Object? Function(String, int) reply) async =>
      Peer._(await ServerSocket.bind(InternetAddress.loopbackIPv4, 0), reply);

  int get port => _server.port;
  String get address => '127.0.0.1:$port';

  Future<void> shutdown() => _server.close();

  void _serve(Socket socket) {
    connections += 1;
    var pending = <int>[];
    socket.listen((chunk) {
      pending = [...pending, ...chunk];
      while (pending.length >= 6) {
        final header = ByteData.sublistView(Uint8List.fromList(pending));
        final length = header.getUint32(2);
        if (pending.length < 6 + length) return;
        final tag = pending[1];
        final body = length == 0
            ? null
            : jsonDecode(utf8.decode(pending.sublist(6, 6 + length)));
        pending = pending.sublist(6 + length);
        switch (tag) {
          case Frame.hello:
            socket
                .add(Frame.encode(Frame.helloOk, {'ok': true, 'features': 7}));
          case Frame.auth:
            users.add((body as Map)['username'] as String);
            socket.add(
                Frame.encode(Frame.authOk, {'ok': true, 'session_id': 's-1'}));
          case Frame.close:
            socket.add(Frame.encode(Frame.bye, const {}));
            socket.destroy();
          case Frame.request:
            final rid = (body as Map)['request_id'] as String;
            requestIds.add(rid);
            socket.add(
                Frame.encode(Frame.response, reply(rid, requestIds.length)));
        }
      }
    }, onError: (_) {}, cancelOnError: true);
  }
}

Map<String, Object?> ok(String rid) => {
      'request_id': rid,
      'status': 'ok',
      'data': {
        'Json': {'transaction': 'began'},
      },
      'diagnostics': const {},
    };

Map<String, Object?> refusal(String rid, String code, [String? leader]) => {
      'request_id': rid,
      'status': 'error',
      'data': {'Message': 'not the raft leader'},
      'diagnostics': {
        'error_code': code,
        if (leader != null) 'leader_hint': leader,
      },
    };

Future<TriCore> connectTo(Peer peer, {String? user, int leaderRetries = 5}) =>
    TriCore.connect(
      host: '127.0.0.1',
      port: peer.port,
      user: user,
      secret: 's',
      connectTimeout: const Duration(seconds: 5),
      leaderRetries: leaderRetries,
      leaderRetryDelay: const Duration(milliseconds: 1),
    );

/// Following the leader: what the client does with `not_leader` by default.
/// A real follower's refusal was applied nowhere, which is what makes sending
/// the request again safe.
void main() {
  test('a refused request goes to the leader and stays there', () async {
    final leader = await Peer.start((rid, _) => ok(rid));
    addTearDown(leader.shutdown);
    final follower = await Peer.start(
        (rid, _) => refusal(rid, notLeaderCode, leader.address));
    addTearDown(follower.shutdown);

    final db = await connectTo(follower, user: 'u');
    await db.execute('INSERT INTO t VALUES (1)');
    expect(db.address, leader.address);
    await db.execute('INSERT INTO t VALUES (2)');
    expect(follower.requestIds, hasLength(1));
    expect(leader.requestIds, hasLength(2));
    expect(leader.connections, 1);
    expect(leader.users, ['u']);
    expect(leader.requestIds.first, follower.requestIds.first);
    await db.close();
  });

  test('with no leader named the client waits and asks again', () async {
    final peer = await Peer.start(
        (rid, n) => n < 3 ? refusal(rid, notLeaderCode) : ok(rid));
    addTearDown(peer.shutdown);
    final db = await connectTo(peer);
    await db.execute('INSERT INTO t VALUES (1)');
    expect(peer.requestIds, hasLength(3));
    expect(peer.connections, 1);
    await db.close();
  });

  test('retries are bounded and reported', () async {
    final peer = await Peer.start((rid, _) => refusal(rid, notLeaderCode));
    addTearDown(peer.shutdown);
    final db = await connectTo(peer, leaderRetries: 2);
    await expectLater(
      db.execute('INSERT INTO t VALUES (1)'),
      throwsA(isA<ServerException>()
          .having((e) => e.isRedirect, 'isRedirect', isTrue)
          .having(
              (e) => e.message, 'message', contains('sent it again 2 time'))),
    );
    expect(peer.requestIds, hasLength(3));
    await db.close();
  });

  test('an unreachable leader leaves the connection where it was', () async {
    final dead = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final nowhere = '127.0.0.1:${dead.port}';
    await dead.close();
    final peer = await Peer.start(
        (rid, n) => n == 1 ? refusal(rid, notLeaderCode, nowhere) : ok(rid));
    addTearDown(peer.shutdown);
    final db = await connectTo(peer);
    await db.execute('INSERT INTO t VALUES (1)');
    expect(db.address, peer.address);
    expect(peer.requestIds, hasLength(2));
    await db.close();
  });

  test('nothing is sent again inside a session transaction', () async {
    final peer = await Peer.start(
        (rid, n) => n == 1 ? ok(rid) : refusal(rid, notLeaderCode));
    addTearDown(peer.shutdown);
    final db = await connectTo(peer);
    await db.begin();
    await expectLater(
      db.execute('INSERT INTO t VALUES (1)'),
      throwsA(isA<ServerException>()
          .having((e) => e.isRedirect, 'isRedirect', isTrue)),
    );
    expect(peer.requestIds, hasLength(2));
    await db.close();
  });

  test('outcome_unknown is never sent again', () async {
    final peer = await Peer.start((rid, _) => refusal(rid, outcomeUnknownCode));
    addTearDown(peer.shutdown);
    final db = await connectTo(peer);
    await expectLater(
      db.execute('INSERT INTO t VALUES (1)'),
      throwsA(isA<ServerException>()
          .having((e) => e.code, 'code', outcomeUnknownCode)
          .having((e) => e.isRedirect, 'isRedirect', isFalse)),
    );
    expect(peer.requestIds, hasLength(1));
    await db.close();
  });

  test('pooled connections follow the leader', () async {
    final leader = await Peer.start((rid, _) => ok(rid));
    addTearDown(leader.shutdown);
    final follower = await Peer.start(
        (rid, _) => refusal(rid, notLeaderCode, leader.address));
    addTearDown(follower.shutdown);
    final pool = TriCorePool.to(
      port: follower.port,
      size: 2,
      leaderRetryDelay: const Duration(milliseconds: 1),
    );
    for (var i = 0; i < 2; i++) {
      await pool
          .withConnection((db) => db.execute('INSERT INTO t VALUES ($i)'));
    }
    await pool.close();
    expect(follower.requestIds, hasLength(1));
    expect(leader.requestIds, hasLength(2));
  });
}
