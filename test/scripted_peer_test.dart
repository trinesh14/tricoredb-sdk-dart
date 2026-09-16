import 'package:test/test.dart';
import 'package:tricoredb/tricoredb.dart';

import 'support/scripted_peer.dart';

/// How this client reads the protocol, proved without a server. These always
/// run.
void main() {
  test('a not_leader refusal is typed and carries the leader address',
      () async {
    final peer = await ScriptedPeer.start(steps: [
      response(const {
        'request_id': 'r1',
        'status': 'error',
        'data': {'Message': 'not the raft leader'},
        'diagnostics': {
          'error_code': 'not_leader',
          'leader_hint': '10.9.9.7:8427',
        },
      }),
    ]);
    addTearDown(peer.shutdown);

    final db = await peer.connect();
    await expectLater(
      db.execute('INSERT INTO t VALUES (1)'),
      throwsA(
        isA<ServerException>()
            .having((e) => e.code, 'code', 'not_leader')
            .having((e) => e.isRedirect, 'isRedirect', isTrue)
            .having((e) => e.leaderHint, 'leaderHint', '10.9.9.7:8427'),
      ),
    );
    expect(db.isClosed, isFalse, reason: 'a refusal leaves the socket usable');
    await db.close();
  });

  test('an AUTH_OK frame carrying ok: false is still a refusal', () async {
    final peer = await ScriptedPeer.startRaw([
      Reply(Frame.helloOk, const {'ok': true, 'message': 'ok', 'features': 7}),
      // The tag names the answer's shape; the body is the verdict.
      Reply(Frame.authOk, const {'ok': false, 'message': 'bad password'}),
    ]);
    addTearDown(peer.shutdown);

    await expectLater(
      peer.connect(),
      throwsA(
        isA<AuthException>()
            .having((e) => e.message, 'message', 'bad password'),
      ),
    );
  });

  test('a refused handshake is reported as one', () async {
    final peer = await ScriptedPeer.startRaw([
      Reply(Frame.helloOk, const {
        'ok': false,
        'message': 'unsupported protocol version',
        'code': 'protocol_version',
      }),
    ]);
    addTearDown(peer.shutdown);

    await expectLater(
      peer.connect(),
      throwsA(
        isA<ProtocolException>()
            .having((e) => e.message, 'message', contains('unsupported'))
            .having((e) => e.code, 'code', 'protocol_version'),
      ),
    );
  });

  test('a declared payload above the ceiling is refused before it is read',
      () async {
    // A control frame claiming 64 KiB + 1 bytes, with none of them sent. A
    // client that trusted the length would allocate it and wait for ever.
    final peer = await ScriptedPeer.start(steps: [
      overlongHeader(Frame.authOk, Frame.maxControlPayload + 1),
    ]);
    addTearDown(peer.shutdown);

    final db = await peer.connect();
    await expectLater(
      db.ping(),
      throwsA(
        isA<ProtocolException>()
            .having((e) => e.code, 'code', 'frame_too_large'),
      ),
    );
    expect(db.isClosed, isTrue,
        reason:
            'a stream that cannot be resynchronised is dropped, not reused');
  });

  test('a peer that hangs up mid-frame does not leave the client waiting',
      () async {
    final peer = await ScriptedPeer.start(steps: [
      Raw([Frame.version, Frame.response, 0, 0, 0, 10, 0x7b]),
      const HangUp(),
    ]);
    addTearDown(peer.shutdown);

    final db = await peer.connect();
    await expectLater(
      db.execute('SELECT 1'),
      throwsA(isA<ConnectionException>()),
    );
    await db.close();
  });

  test('a reply that never arrives ends at the read timeout', () async {
    final peer = await ScriptedPeer.start(steps: [const Silence()]);
    addTearDown(peer.shutdown);

    final db = await peer.connect(
      readTimeout: const Duration(milliseconds: 200),
    );
    final started = DateTime.now();
    await expectLater(
      db.execute('SELECT 1'),
      throwsA(isA<ReadTimeoutException>()),
    );
    expect(DateTime.now().difference(started).inSeconds, lessThan(3),
        reason: 'it did not wait');
    expect(db.isClosed, isTrue,
        reason: 'the reply may still arrive, so the socket cannot be reused');
  });

  test('a status this client does not know is treated as a failure', () async {
    final peer = await ScriptedPeer.start(steps: [
      response(const {
        'request_id': 'r1',
        'status': 'not_implemented',
        'data': {'Message': 'Cache::XGroup is refused in V1'},
      }),
    ]);
    addTearDown(peer.shutdown);

    final db = await peer.connect();
    await expectLater(
      db.request(const {
        'Cache': {'XGroup': <String, Object?>{}},
      }),
      throwsA(
        isA<ServerException>()
            .having((e) => e.status, 'status', 'not_implemented')
            .having((e) => e.message, 'message', contains('XGroup')),
      ),
    );
    await db.close();
  });

  test('a server that granted nothing makes the client refuse before sending',
      () async {
    // An older server grants no capabilities at all.
    final peer =
        await ScriptedPeer.start(features: 0, steps: [const Silence()]);
    addTearDown(peer.shutdown);

    final db = await peer.connect();
    expect(db.grantedFeatures.isEmpty, isTrue);
    expect(db.grantedFeatures.serverParams, isFalse);

    await expectLater(
      db.query('SELECT * FROM t WHERE id = ?', [1]),
      throwsA(
        isA<FeatureNotGrantedException>()
            .having((e) => e.feature, 'feature', 'SERVER_PARAMS'),
      ),
    );
    expect(db.isClosed, isFalse,
        reason: 'nothing was sent, so the connection is untouched');

    await expectLater(
      db.begin(),
      throwsA(
        isA<FeatureNotGrantedException>()
            .having((e) => e.feature, 'feature', 'SESSION_TXN'),
      ),
    );
    await db.close();
  });

  test('warnings and diagnostics reach the caller on a successful response',
      () async {
    final peer = await ScriptedPeer.start(steps: [
      response(const {
        'request_id': 'r1',
        'status': 'ok',
        'data': {'Message': 'done'},
        'diagnostics': {
          'route': 'local',
          'elapsed_ms': 4,
          'warnings': ['shard 2 was unreachable'],
        },
      }),
    ]);
    addTearDown(peer.shutdown);

    final db = await peer.connect();
    final result = await db.request(const {'Admin': 'Ping'});

    expect(result.isOk, isTrue);
    expect(result.warnings, ['shard 2 was unreachable']);
    expect(result.route, 'local');
    expect(result.elapsedMilliseconds, 4);
    await db.close();
  });

  test('the request envelope carries the database and a unique id', () async {
    final peer = await ScriptedPeer.start(steps: [
      response(const {
        'request_id': 'r1',
        'status': 'ok',
        'data': {
          'Rows': {
            'columns': ['n'],
            'rows': [
              ['1'],
            ],
          },
        },
      }),
    ]);
    addTearDown(peer.shutdown);

    final db = await peer.connect();
    db.database = 'reporting';
    final rows = await db.query('SELECT 1');

    expect(rows[0], ['1']);
    expect(db.lastRequestId, startsWith('dart-'));
    expect(db.database, 'reporting');
    await db.close();
  });

  test('requests made at once are answered in order, one at a time', () async {
    // A connection is a single stream: two callers must not interleave frames
    // on it, and neither may read the other's answer.
    final peer = await ScriptedPeer.start(steps: [
      response(const {
        'status': 'ok',
        'data': {
          'Rows': {
            'columns': ['n'],
            'rows': [
              ['first'],
            ],
          },
        },
      }),
      response(const {
        'status': 'ok',
        'data': {
          'Rows': {
            'columns': ['n'],
            'rows': [
              ['second'],
            ],
          },
        },
      }),
    ]);
    addTearDown(peer.shutdown);

    final db = await peer.connect();
    final results = await Future.wait([
      db.query('SELECT 1'),
      db.query('SELECT 2'),
    ]);

    expect(results[0][0][0], 'first');
    expect(results[1][0][0], 'second');
    await db.close();
  });
}
