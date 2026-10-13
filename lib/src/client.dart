import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

import 'builders.dart';
import 'errors.dart';
import 'frame.dart';
import 'operations.dart';
import 'options.dart';
import 'params.dart';
import 'response.dart';
import 'transport.dart';
import 'version.dart';

/// The default port a TriCoreDB server listens on.
const int defaultPort = 8427;

/// The database used when a call does not name one.
const String defaultDatabase = 'main';

/// One connection to a TriCoreDB server, speaking the native `tricore`
/// protocol.
///
/// A connection is a single request/response stream. Calls made from several
/// places at once are queued internally, so they are safe but not concurrent;
/// use a [TriCorePool] when concurrency is what you want.
///
/// ```dart
/// final db = await TriCore.connect(
///   host: '127.0.0.1',
///   user: 'admin',
///   secret: Platform.environment['TRICORE_SECRET']!,
/// );
/// await db.execute('INSERT INTO t VALUES (?, ?)', [1, 'ada']);
/// final rows = await db.query('SELECT name FROM t WHERE id = ?', [1]);
/// print(rows[0][0]); // ada
/// await db.close();
/// ```
class TriCore {
  static const Duration _closeTimeout = Duration(seconds: 2);
  static const Duration _maxLeaderRetryDelay = Duration(seconds: 2);
  static const Duration _followConnectTimeout = Duration(seconds: 3);

  Transport _transport;

  /// The database used when a call does not name one.
  String database;

  /// How long to wait for each reply. Null waits for as long as the statement
  /// runs.
  Duration? readTimeout;

  /// A server-side deadline sent with every request. Null lets the server use
  /// its own.
  Duration? requestTimeout;

  /// Send a request again when a cluster node refuses it with `not_leader`.
  ///
  /// The request goes to the leader the refusal names, over a new connection
  /// that replaces this one (same user, secret and TLS settings, with the
  /// leader's host as the certificate name), or, while no leader is known, to
  /// the same node after a pause. That is safe because a `not_leader` refusal
  /// was applied nowhere. It never happens inside a session transaction.
  bool followLeader = true;

  /// How many times a refused request is sent again.
  int leaderRetries = 5;

  /// The first pause before asking again; it doubles up to two seconds.
  Duration leaderRetryDelay = const Duration(milliseconds: 250);

  String _address = '';
  _ConnectOptions? _connectOptions;

  Future<void> _gate = Future<void>.value();
  int _requestCounter = 0;
  final String _requestPrefix;
  GrantedFeatures _granted = const GrantedFeatures(0);
  String? _sessionId;
  String? _lastRequestId;
  bool _transactionOpen = false;

  TriCore._(this._transport, this.database, this._requestPrefix);

  /// Connect, shake hands, and authenticate when a [user] is given.
  ///
  /// [connectTimeout] covers the TCP connect, the TLS handshake, HELLO and
  /// AUTH. [readTimeout] applies to every reply afterwards.
  static Future<TriCore> connect({
    String host = '127.0.0.1',
    int port = defaultPort,
    String? user,
    String secret = '',
    String database = defaultDatabase,
    Duration? connectTimeout = const Duration(seconds: 10),
    Duration? readTimeout,
    Duration? requestTimeout,
    TlsOptions? tls,
    String? clientName,
    int features = Features.all,
    bool followLeader = true,
    int leaderRetries = 5,
    Duration leaderRetryDelay = const Duration(milliseconds: 250),
  }) async {
    final transport = await Transport.connect(
      host,
      port,
      connectTimeout: connectTimeout,
      tls: tls,
    );
    final random = Random();
    final prefix = 'dart-'
        '${random.nextInt(1 << 32).toRadixString(16).padLeft(8, '0')}';
    final client = TriCore._(transport, database, prefix);
    try {
      await client._handshake(
        clientName: clientName ?? 'tricoredb-dart/$packageVersion',
        features: features,
        timeout: connectTimeout,
      );
      if (user != null) {
        await client._authenticate(user, secret, timeout: connectTimeout);
      }
    } on Object {
      transport.close();
      rethrow;
    }
    client.readTimeout = readTimeout;
    client.requestTimeout = requestTimeout;
    client.followLeader = followLeader;
    client.leaderRetries = leaderRetries;
    client.leaderRetryDelay = leaderRetryDelay;
    client._address = _joinAddress(host, port);
    client._connectOptions = _ConnectOptions(
      user: user,
      secret: secret,
      connectTimeout: connectTimeout,
      tls: tls,
      clientName: clientName,
      features: features,
    );
    return client;
  }

  /// The `host:port` this connection talks to. It changes when the client
  /// follows the leader.
  String get address => _address;

  /// Which capabilities the server granted in the handshake.
  GrantedFeatures get grantedFeatures => _granted;

  /// The session id the server issued, when this connection authenticated.
  String? get sessionId => _sessionId;

  /// The id of the most recent request. Pass it to [cancel] on another
  /// connection.
  String? get lastRequestId => _lastRequestId;

  /// Whether this connection can no longer be used.
  bool get isClosed => _transport.isClosed;

  /// Whether a session transaction is open right now.
  bool get inTransaction => _transactionOpen && !isClosed;

  // ---- handshake -----------------------------------------------------------

  Future<void> _handshake({
    required String clientName,
    required int features,
    Duration? timeout,
  }) async {
    final frame = await _exchange(
      Frame.hello,
      {
        'protocol': 'tricore',
        'version': {'major': 1, 'minor': 0},
        'client': clientName,
        'features': features,
      },
      timeout: timeout,
    );
    final body = frame.body;
    if (frame.tag == Frame.error) {
      throw _failStream(
        ProtocolException(_errorText(body), code: _errorCode(body)),
      );
    }
    if (frame.tag != Frame.helloOk) {
      throw _failStream(
        ProtocolException('expected HELLO_OK, got ${Frame.name(frame.tag)}'),
      );
    }
    if (body is! Map || body['ok'] != true) {
      throw _failStream(
        ProtocolException(
          body is Map && body['message'] is String
              ? body['message'] as String
              : 'the handshake was refused',
          code: _errorCode(body),
        ),
      );
    }
    final granted = body['features'];
    _granted = GrantedFeatures(granted is int ? granted : 0);
  }

  Future<void> _authenticate(
    String user,
    String secret, {
    Duration? timeout,
  }) async {
    final frame = await _exchange(
      Frame.auth,
      {'username': user, 'secret': utf8.encode(secret)},
      timeout: timeout,
    );
    final body = frame.body;
    if (frame.tag == Frame.error) {
      _transport.close();
      throw AuthException(_errorText(body), code: _errorCode(body));
    }
    if (frame.tag != Frame.authOk) {
      throw _failStream(
        ProtocolException('expected AUTH_OK, got ${Frame.name(frame.tag)}'),
      );
    }
    // A refused login arrives as AUTH_OK carrying `ok: false`, not as an ERROR
    // frame: the tag names the answer's shape, the body is the verdict.
    if (body is! Map || body['ok'] != true) {
      _transport.close();
      throw AuthException(
        body is Map && body['message'] is String
            ? body['message'] as String
            : 'authentication was refused',
      );
    }
    _sessionId =
        body['session_id'] is String ? body['session_id'] as String : null;
  }

  /// Exchange PING and PONG. This never reaches a module; see [adminPing] for
  /// a round trip through the whole pipeline.
  Future<void> ping() async {
    final frame = await _exchange(Frame.ping, null);
    if (frame.tag != Frame.pong) {
      throw _failStream(
        ProtocolException('expected PONG, got ${Frame.name(frame.tag)}'),
      );
    }
  }

  /// Ask the server to stop one of this principal's running statements.
  ///
  /// Send this on a **second connection**: the one running the statement is
  /// waiting for its reply and cannot carry anything else.
  ///
  /// Returns how many executions were cancelled — 0 for an id the server does
  /// not know.
  Future<int> cancel(String requestId) async {
    final frame = await _exchange(Frame.cancel, {'request_id': requestId});
    if (frame.tag == Frame.error) {
      throw ServerException(
        _errorText(frame.body),
        code: _errorCode(frame.body),
        fromErrorFrame: true,
      );
    }
    if (frame.tag != Frame.cancelOk) {
      throw _failStream(
        ProtocolException('expected CANCEL_OK, got ${Frame.name(frame.tag)}'),
      );
    }
    final body = frame.body;
    return body is Map && body['cancelled'] is int
        ? body['cancelled'] as int
        : 0;
  }

  /// Say goodbye and close the socket. Idempotent, and never throws.
  Future<void> close() async {
    if (isClosed) return;
    try {
      await _exchange(Frame.close, null, timeout: _closeTimeout);
    } on Object {
      // Going away politely is best effort; the socket closes either way.
    } finally {
      _transport.close();
      _transactionOpen = false;
    }
  }

  // ---- requests ------------------------------------------------------------

  /// Send a raw operation, such as `{'Cache': 'Ping'}`.
  ///
  /// [txn] names an open document or graph transaction the request runs
  /// inside.
  ///
  /// Returns only when the status is `ok`; any other status throws a
  /// [ServerException] carrying the server's code.
  Future<Response> request(
    Object op, {
    String? database,
    String? correlationId,
    String? txn,
  }) async {
    final payload = <String, Object?>{
      'request_id': _nextRequestId(),
      'database': database ?? this.database,
      'region_hint': null,
      'op': op,
    };
    if (correlationId != null) {
      _requireFeature(
        _granted.correlationId,
        'CORRELATION_ID',
        'a correlation id',
      );
      payload['correlation_id'] = correlationId;
    }
    if (requestTimeout case final timeout?) {
      payload['options'] = {
        'cache': {'mode': 'disabled'},
        'output': 'native',
        'consistency': 'strong_primary',
        'timeout_ms': timeout.inMilliseconds,
        'llm': null,
      };
    }
    if (txn != null) {
      payload['options'] = {
        ...?(payload['options'] as Map<String, Object?>?),
        'txn': txn,
      };
    }
    var pause = leaderRetryDelay;
    for (var retried = 0;; retried++) {
      final response = await _sendRequest(payload);
      if (response.isOk) return response;
      // A `not_leader` refusal was applied nowhere, so sending it again cannot
      // apply it twice. A session transaction belongs to this connection.
      if (response.isRedirect &&
          followLeader &&
          !_transactionOpen &&
          retried < leaderRetries) {
        final hint = response.leaderHint;
        if (hint != null && hint != _address && await _moveTo(hint)) continue;
        await Future<void>.delayed(pause);
        final doubled = pause * 2;
        pause = doubled < const Duration(milliseconds: 1)
            ? const Duration(milliseconds: 1)
            : (doubled > _maxLeaderRetryDelay ? _maxLeaderRetryDelay : doubled);
        continue;
      }
      final error = response.toException(
        transactionOpen: _transactionOpen,
        retried: followLeader ? retried : null,
      );
      if (response.isRedirect) _transactionOpen = false;
      throw error;
    }
  }

  Future<Response> _sendRequest(Map<String, Object?> payload) async {
    final frame = await _exchange(Frame.request, payload);
    if (frame.tag == Frame.error) {
      throw ServerException(
        _errorText(frame.body),
        code: _errorCode(frame.body),
        leaderHint: _leaderHint(frame.body),
        fromErrorFrame: true,
      );
    }
    if (frame.tag != Frame.response) {
      throw _failStream(
        ProtocolException('expected RESPONSE, got ${Frame.name(frame.tag)}'),
      );
    }
    return Response.fromJson(frame.body);
  }

  /// Replace this connection with one to [target], opened the way this one
  /// was. False, leaving this connection untouched, when that fails.
  Future<bool> _moveTo(String target) async {
    final options = _connectOptions;
    final parsed = _splitAddress(target);
    if (options == null || parsed == null) return false;
    final (host, port) = parsed;
    final budget = options.connectTimeout;
    final TriCore fresh;
    try {
      fresh = await TriCore.connect(
        host: host,
        port: port,
        user: options.user,
        secret: options.secret,
        database: database,
        connectTimeout: budget == null || budget > _followConnectTimeout
            ? _followConnectTimeout
            : budget,
        tls: options.tls == null
            ? null
            // The leader's certificate is checked against the name the hint
            // gives.
            : TlsOptions(
                caFile: options.tls!.caFile,
                serverName: host,
                clientCertificateFile: options.tls!.clientCertificateFile,
                clientKeyFile: options.tls!.clientKeyFile,
                clientKeyPassword: options.tls!.clientKeyPassword,
                dangerAcceptInvalidCertificates:
                    options.tls!.dangerAcceptInvalidCertificates,
              ),
        clientName: options.clientName,
        features: options.features,
        followLeader: false,
      );
    } on Object {
      return false;
    }
    final old = _transport;
    _transport = fresh._transport;
    _sessionId = fresh._sessionId;
    _granted = fresh._granted;
    _address = target;
    old.close();
    return true;
  }

  static (String, int)? _splitAddress(String address) {
    final match = RegExp(r'^\[([^\]]+)\]:(\d+)$').firstMatch(address) ??
        RegExp(r'^([^:]+):(\d+)$').firstMatch(address);
    if (match == null) return null;
    final port = int.parse(match.group(2)!);
    if (port < 1 || port > 65535) return null;
    return (match.group(1)!, port);
  }

  static String _joinAddress(String host, int port) =>
      host.contains(':') ? '[$host]:$port' : '$host:$port';

  // ---- SQL -----------------------------------------------------------------

  /// Run a statement that is not a `SELECT`: DDL, `INSERT`, `UPDATE`,
  /// `DELETE`, or a whole transaction script.
  ///
  /// Values in [params] are bound **by the server**; this driver never renders
  /// them into the statement text.
  ///
  /// A parameter this driver will not encode, or a capability the server did
  /// not grant, fails the returned future rather than throwing into the
  /// caller's stack: one call, one place to catch.
  Future<Response> execute(
    String sql, [
    List<Object?>? params,
    String? database,
  ]) async {
    return request(
      {
        'Sql': {'Exec': _sqlBody(sql, params)},
      },
      database: database,
    );
  }

  /// Run a `SELECT`. The server refuses a write sent this way.
  Future<Rows> query(
    String sql, [
    List<Object?>? params,
    String? database,
  ]) async {
    final response = await request(
      {
        'Sql': {'Query': _sqlBody(sql, params)},
      },
      database: database,
    );
    final rows = response.arm('Rows');
    if (rows is! Map) {
      throw ProtocolException('expected Rows, got ${response.kind}');
    }
    final columns = rows['columns'];
    final data = rows['rows'];
    return Rows(
      columns is List ? [for (final c in columns) '$c'] : const [],
      data is List
          ? [
              for (final row in data)
                if (row is List) [for (final cell in row) cell?.toString()],
            ]
          : const [],
    );
  }

  /// Open a session transaction on this connection.
  ///
  /// Needs the `SESSION_TXN` capability. Without it, send a whole
  /// `BEGIN; …; COMMIT` script through [execute] instead.
  Future<Map<String, dynamic>> begin({String? database}) async {
    _requireFeature(
      _granted.sessionTxn,
      'SESSION_TXN',
      'begin/commit/rollback as separate requests (send a whole '
          '`BEGIN; …; COMMIT` script with execute() instead)',
    );
    return _transactionControl('BEGIN', database);
  }

  /// Commit the open transaction.
  Future<Map<String, dynamic>> commit({String? database}) =>
      _transactionControl('COMMIT', database);

  /// Discard the open transaction.
  Future<Map<String, dynamic>> rollback({String? database}) =>
      _transactionControl('ROLLBACK', database);

  /// Begin, run [work], and commit. If [work] throws, roll back and rethrow
  /// the original error.
  Future<T> transaction<T>(
    Future<T> Function(TriCore db) work, {
    String? database,
  }) async {
    await begin(database: database);
    T result;
    try {
      result = await work(this);
    } on Object {
      if (inTransaction) {
        try {
          await rollback(database: database);
        } on Object {
          // The original failure is the one worth reporting.
        }
      }
      rethrow;
    }
    await commit(database: database);
    return result;
  }

  // ---- cache ---------------------------------------------------------------
  //
  // Cache values are bytes. Pass a String (its UTF-8 bytes are sent) or any
  // List<int>; values come back as bytes.

  /// A liveness check routed through the cache module.
  Future<void> cachePing({String? database}) =>
      request({'Cache': 'Ping'}, database: database);

  /// Store a value, expiring after [ttl] when one is given.
  Future<void> cacheSet(
    String namespace,
    String key,
    Object value, {
    Duration? ttl,
    String? database,
  }) async {
    await _cache(
      'Set',
      {
        ..._namespaceKey(namespace, key),
        'value': _bytes(value, 'value'),
        'ttl_ms': ttl?.inMilliseconds,
      },
      database,
    );
  }

  /// Read a value. Null is a miss, which is how a miss is told apart from a
  /// stored empty value.
  Future<Uint8List?> cacheGet(
    String namespace,
    String key, {
    String? database,
  }) async {
    return _cacheValue(
      await _cache('Get', _namespaceKey(namespace, key), database),
      'Get',
    );
  }

  /// Read a value as UTF-8 text. Null is a miss.
  Future<String?> cacheGetString(
    String namespace,
    String key, {
    String? database,
  }) async {
    final value = await cacheGet(namespace, key, database: database);
    return value == null ? null : utf8.decode(value, allowMalformed: true);
  }

  /// Store a value only when the key is absent, reporting whether this call
  /// stored it. The primitive behind a distributed lock.
  Future<bool> cacheSetIfAbsent(
    String namespace,
    String key,
    Object value, {
    Duration? ttl,
    String? database,
  }) async {
    final json = _json(
      await _cache(
        'SetNx',
        {
          ..._namespaceKey(namespace, key),
          'value': _bytes(value, 'value'),
          'ttl_ms': ttl?.inMilliseconds,
        },
        database,
      ),
      'SetNx',
    );
    return json['set'] == true;
  }

  /// Delete a key, reporting whether it existed.
  Future<bool> cacheDelete(
    String namespace,
    String key, {
    String? database,
  }) async {
    final json = _json(
      await _cache('Delete', _namespaceKey(namespace, key), database),
      'Delete',
    );
    return json['deleted'] == true;
  }

  /// Whether a key is present.
  Future<bool> cacheExists(
    String namespace,
    String key, {
    String? database,
  }) async {
    final json = _json(
      await _cache('Exists', _namespaceKey(namespace, key), database),
      'Exists',
    );
    return json['exists'] == true;
  }

  /// What is left of a key's life. Null when it is missing or never expires.
  Future<Duration?> cacheTtl(
    String namespace,
    String key, {
    String? database,
  }) async {
    final json = _json(
      await _cache('Ttl', _namespaceKey(namespace, key), database),
      'Ttl',
    );
    final ttl = json['ttl_ms'];
    return ttl is int ? Duration(milliseconds: ttl) : null;
  }

  /// Give a key a new expiry. False when there is no such key.
  Future<bool> cacheExpire(
    String namespace,
    String key,
    Duration ttl, {
    String? database,
  }) async {
    final json = _json(
      await _cache(
        'Expire',
        {..._namespaceKey(namespace, key), 'ttl_ms': ttl.inMilliseconds},
        database,
      ),
      'Expire',
    );
    return json['updated'] == true;
  }

  /// Remove a key's expiry. False when it had none.
  Future<bool> cachePersist(
    String namespace,
    String key, {
    String? database,
  }) async {
    final json = _json(
      await _cache('Persist', _namespaceKey(namespace, key), database),
      'Persist',
    );
    return json['persisted'] == true;
  }

  /// Add to a counter, returning its new value.
  Future<int> cacheIncrement(
    String namespace,
    String key, {
    int by = 1,
    String? database,
  }) async {
    final json = _json(
      await _cache(
        'Incr',
        {..._namespaceKey(namespace, key), 'by': by},
        database,
      ),
      'Incr',
    );
    return json['value'] as int;
  }

  /// Delete every key in a namespace, returning how many went.
  Future<int> cacheClearNamespace(
    String namespace, {
    String? database,
  }) async {
    final json = _json(
      await _cache('ClearNamespace', {'namespace': namespace}, database),
      'ClearNamespace',
    );
    return json['cleared'] as int;
  }

  /// The live keys in a namespace. [pattern] is a glob where `*` matches any
  /// run of characters.
  Future<List<Map<String, dynamic>>> cacheKeys(
    String namespace, {
    String? pattern,
    int? limit,
    String? database,
  }) async {
    final json = _json(
      await _cache(
        'Keys',
        {'namespace': namespace, 'pattern': pattern, 'limit': limit},
        database,
      ),
      'Keys',
    );
    final keys = json['keys'];
    return keys is List
        ? [for (final key in keys) Map<String, dynamic>.from(key as Map)]
        : const [];
  }

  /// Push values onto the head of a list, returning its new length.
  Future<int> cacheLeftPush(
    String namespace,
    String key,
    List<Object> values, {
    String? database,
  }) =>
      _cacheListPush('LPush', namespace, key, values, database);

  /// Push values onto the tail of a list, returning its new length.
  Future<int> cacheRightPush(
    String namespace,
    String key,
    List<Object> values, {
    String? database,
  }) =>
      _cacheListPush('RPush', namespace, key, values, database);

  /// Take a value from the head of a list. Null when it is empty.
  Future<Uint8List?> cacheLeftPop(
    String namespace,
    String key, {
    String? database,
  }) async =>
      _cacheValue(
        await _cache('LPop', _namespaceKey(namespace, key), database),
        'LPop',
      );

  /// Take a value from the tail of a list. Null when it is empty.
  Future<Uint8List?> cacheRightPop(
    String namespace,
    String key, {
    String? database,
  }) async =>
      _cacheValue(
        await _cache('RPop', _namespaceKey(namespace, key), database),
        'RPop',
      );

  /// An inclusive slice of a list. Negative indices count from the end.
  Future<List<Uint8List>> cacheRange(
    String namespace,
    String key,
    int start,
    int stop, {
    String? database,
  }) async {
    final json = _json(
      await _cache(
        'LRange',
        {..._namespaceKey(namespace, key), 'start': start, 'stop': stop},
        database,
      ),
      'LRange',
    );
    return _byteLists(json['values']);
  }

  /// How many values a list holds.
  Future<int> cacheLength(
    String namespace,
    String key, {
    String? database,
  }) async {
    final json = _json(
      await _cache('LLen', _namespaceKey(namespace, key), database),
      'LLen',
    );
    return json['length'] as int;
  }

  /// One value of a list by position. Null when the index is out of range.
  Future<Uint8List?> cacheIndex(
    String namespace,
    String key,
    int index, {
    String? database,
  }) async =>
      _cacheValue(
        await _cache(
          'LIndex',
          {..._namespaceKey(namespace, key), 'index': index},
          database,
        ),
        'LIndex',
      );

  /// Add members to a set, returning how many were new.
  Future<int> cacheSetAdd(
    String namespace,
    String key,
    List<Object> members, {
    String? database,
  }) async {
    final json = _json(
      await _cache(
        'SAdd',
        {
          ..._namespaceKey(namespace, key),
          'members': _byteList(members, 'members'),
        },
        database,
      ),
      'SAdd',
    );
    return json['added'] as int;
  }

  /// Remove members from a set, returning how many were present.
  Future<int> cacheSetRemove(
    String namespace,
    String key,
    List<Object> members, {
    String? database,
  }) async {
    final json = _json(
      await _cache(
        'SRem',
        {
          ..._namespaceKey(namespace, key),
          'members': _byteList(members, 'members'),
        },
        database,
      ),
      'SRem',
    );
    return json['removed'] as int;
  }

  /// Whether a set holds a member.
  Future<bool> cacheSetContains(
    String namespace,
    String key,
    Object member, {
    String? database,
  }) async {
    final json = _json(
      await _cache(
        'SIsMember',
        {
          ..._namespaceKey(namespace, key),
          'member': _bytes(member, 'member'),
        },
        database,
      ),
      'SIsMember',
    );
    return json['is_member'] == true;
  }

  /// How many members a set holds.
  Future<int> cacheSetCount(
    String namespace,
    String key, {
    String? database,
  }) async {
    final json = _json(
      await _cache('SCard', _namespaceKey(namespace, key), database),
      'SCard',
    );
    return json['cardinality'] as int;
  }

  /// A set's members, in ascending byte order.
  Future<List<Uint8List>> cacheSetMembers(
    String namespace,
    String key, {
    String? database,
  }) async {
    final json = _json(
      await _cache('SMembers', _namespaceKey(namespace, key), database),
      'SMembers',
    );
    return _byteLists(json['members']);
  }

  /// Set hash fields, returning how many were created rather than replaced.
  Future<int> cacheHashSet(
    String namespace,
    String key,
    Map<Object, Object> entries, {
    String? database,
  }) async {
    final json = _json(
      await _cache(
        'HSet',
        {..._namespaceKey(namespace, key), 'entries': _pairs(entries)},
        database,
      ),
      'HSet',
    );
    return json['created'] as int;
  }

  /// One hash field. Null when it is absent.
  Future<Uint8List?> cacheHashGet(
    String namespace,
    String key,
    Object field, {
    String? database,
  }) async =>
      _cacheValue(
        await _cache(
          'HGet',
          {..._namespaceKey(namespace, key), 'field': _bytes(field, 'field')},
          database,
        ),
        'HGet',
      );

  /// Delete hash fields, returning how many were present.
  Future<int> cacheHashDelete(
    String namespace,
    String key,
    List<Object> fields, {
    String? database,
  }) async {
    final json = _json(
      await _cache(
        'HDel',
        {
          ..._namespaceKey(namespace, key),
          'fields': _byteList(fields, 'fields'),
        },
        database,
      ),
      'HDel',
    );
    return json['deleted'] as int;
  }

  /// Every field of a hash, in ascending field order.
  Future<List<MapEntry<Uint8List, Uint8List>>> cacheHashAll(
    String namespace,
    String key, {
    String? database,
  }) async {
    final json = _json(
      await _cache('HGetAll', _namespaceKey(namespace, key), database),
      'HGetAll',
    );
    final entries = json['entries'];
    if (entries is! List) return const [];
    return [
      for (final entry in entries)
        if (entry is List && entry.length == 2)
          MapEntry(_toBytes(entry[0]), _toBytes(entry[1])),
    ];
  }

  /// Whether a hash holds a field.
  Future<bool> cacheHashContains(
    String namespace,
    String key,
    Object field, {
    String? database,
  }) async {
    final json = _json(
      await _cache(
        'HExists',
        {..._namespaceKey(namespace, key), 'field': _bytes(field, 'field')},
        database,
      ),
      'HExists',
    );
    return json['exists'] == true;
  }

  /// How many fields a hash holds.
  Future<int> cacheHashCount(
    String namespace,
    String key, {
    String? database,
  }) async {
    final json = _json(
      await _cache('HLen', _namespaceKey(namespace, key), database),
      'HLen',
    );
    return json['length'] as int;
  }

  /// Append an entry to a stream, returning its `<ms>-<seq>` id.
  Future<String> cacheStreamAdd(
    String namespace,
    String key,
    Map<Object, Object> fields, {
    String? id,
    String? database,
  }) async {
    final json = _json(
      await _cache(
        'XAdd',
        {
          ..._namespaceKey(namespace, key),
          'id': id,
          'fields': _pairs(fields),
        },
        database,
      ),
      'XAdd',
    );
    return json['id'] as String;
  }

  /// How many entries a stream holds.
  Future<int> cacheStreamLength(
    String namespace,
    String key, {
    String? database,
  }) async {
    final json = _json(
      await _cache('XLen', _namespaceKey(namespace, key), database),
      'XLen',
    );
    return json['length'] as int;
  }

  /// A range of stream entries, oldest first.
  Future<List<StreamEntry>> cacheStreamRange(
    String namespace,
    String key, {
    String start = '-',
    String end = '+',
    int? count,
    String? database,
  }) async {
    final json = _json(
      await _cache(
        'XRange',
        {
          ..._namespaceKey(namespace, key),
          'start': start,
          'end': end,
          'count': count,
        },
        database,
      ),
      'XRange',
    );
    return _streamEntries(json);
  }

  /// Entries strictly newer than [after]. This never blocks.
  Future<List<StreamEntry>> cacheStreamRead(
    String namespace,
    String key, {
    String after = '0-0',
    int? count,
    String? database,
  }) async {
    final json = _json(
      await _cache(
        'XRead',
        {..._namespaceKey(namespace, key), 'after': after, 'count': count},
        database,
      ),
      'XRead',
    );
    return _streamEntries(json);
  }

  /// Delete stream entries by id, returning how many went.
  Future<int> cacheStreamDelete(
    String namespace,
    String key,
    List<String> ids, {
    String? database,
  }) async {
    final json = _json(
      await _cache(
        'XDel',
        {..._namespaceKey(namespace, key), 'ids': ids},
        database,
      ),
      'XDel',
    );
    return json['deleted'] as int;
  }

  /// Trim a stream to its newest [maxLength] entries, returning how many were
  /// evicted.
  Future<int> cacheStreamTrim(
    String namespace,
    String key,
    int maxLength, {
    String? database,
  }) async {
    final json = _json(
      await _cache(
        'XTrim',
        {..._namespaceKey(namespace, key), 'max_len': maxLength},
        database,
      ),
      'XTrim',
    );
    return json['trimmed'] as int;
  }

  /// Store a value that answers as fresh for the given time and as stale
  /// after it, until its TTL (if any) ends. Returns the instant, in Unix
  /// milliseconds, it stops being fresh.
  Future<int> cacheSetFresh(
    String namespace,
    String key,
    Object value,
    Duration freshFor, {
    Duration? ttl,
    String? database,
  }) async {
    final response = await _cache(
        'SetFresh',
        {
          'namespace': namespace,
          'key': key,
          'value': _bytes(value, 'value'),
          'fresh_ms': freshFor.inMilliseconds,
          'ttl_ms': ttl?.inMilliseconds,
        },
        database);
    return _json(response, 'SetFresh')['freshUntilMs'] as int;
  }

  /// Read with stale-while-revalidate and stampede protection. The state
  /// is `fresh`, `stale` (with `refresh` true for the one caller holding
  /// the refresh lease), `missing` (a cached absence) or `miss` (with
  /// `lease` true for the one caller that should compute the value).
  Future<FreshRead> cacheGetFresh(
    String namespace,
    String key,
    String owner,
    Duration lease, {
    String? database,
  }) async {
    final response = await _cache(
        'GetFresh',
        {
          'namespace': namespace,
          'key': key,
          'owner': owner,
          'lease_ms': lease.inMilliseconds,
        },
        database);
    return FreshRead.fromJson(_json(response, 'GetFresh'));
  }

  /// Negative caching: remember for the given time that the source has no
  /// value for the key.
  Future<void> cacheSetMissing(
    String namespace,
    String key,
    Duration ttl, {
    String? database,
  }) async {
    await _cache(
        'SetMissing',
        {
          'namespace': namespace,
          'key': key,
          'ttl_ms': ttl.inMilliseconds,
        },
        database);
  }

  /// Cache warming: write the entries that are not already cached,
  /// leaving live values alone. Answers `written` and `alreadyCached`.
  Future<Map<String, dynamic>> cacheWarm(
    String namespace,
    Map<String, Object> entries, {
    Duration? ttl,
    String? database,
  }) async {
    final response = await _cache(
        'Warm',
        {
          'namespace': namespace,
          'entries': _entries(entries),
          'ttl_ms': ttl?.inMilliseconds,
        },
        database);
    return _json(response, 'Warm');
  }

  /// Read many values in one request. One result per requested key, in
  /// order, with null where the key is absent or expired. Not a snapshot.
  Future<List<Uint8List?>> cacheGetMany(
    String namespace,
    List<String> keys, {
    String? database,
  }) async {
    final response = await _cache(
        'MGet',
        {
          'namespace': namespace,
          'keys': keys,
        },
        database);
    return _cacheValues(response, 'MGet');
  }

  /// Write many values as one batch, all with the same optional TTL. A
  /// repeated key is refused. Returns how many were written.
  Future<int> cacheSetMany(
    String namespace,
    Map<String, Object> entries, {
    Duration? ttl,
    String? database,
  }) async {
    final response = await _cache(
        'MSet',
        {
          'namespace': namespace,
          'entries': _entries(entries),
          'ttl_ms': ttl?.inMilliseconds,
        },
        database);
    return _json(response, 'MSet')['set'] as int;
  }

  /// Write a value under a condition, with explicit control over the TTL.
  /// False means the condition was not met, not that anything failed.
  Future<bool> cacheSetIf(
    String namespace,
    String key,
    Object value, {
    SetCondition condition = SetCondition.always,
    CacheExpiry expiry = CacheExpiry.clear,
    String? database,
  }) async {
    final response = await _cache(
        'SetIf',
        {
          'namespace': namespace,
          'key': key,
          'value': _bytes(value, 'value'),
          'condition': condition.wire,
          'expiry': expiry.toJson(),
        },
        database);
    return _json(response, 'SetIf')['set'] == true;
  }

  /// Add scored members to a sorted set, or re-score ones already there.
  /// Answers `added` and `updated`. A NaN score is refused.
  Future<Map<String, dynamic>> cacheSortedAdd(
    String namespace,
    String key,
    Map<Object, double> members, {
    String? database,
  }) async {
    final response = await _cache(
        'ZAdd',
        {
          'namespace': namespace,
          'key': key,
          'members': _scored(members),
        },
        database);
    return _json(response, 'ZAdd');
  }

  /// Remove members from a sorted set, returning how many were present.
  Future<int> cacheSortedRemove(
    String namespace,
    String key,
    List<Object> members, {
    String? database,
  }) async {
    final response = await _cache(
        'ZRem',
        {
          'namespace': namespace,
          'key': key,
          'members': _byteList(members, 'members'),
        },
        database);
    return _json(response, 'ZRem')['removed'] as int;
  }

  /// The score of one member, or null when it is not in the set.
  Future<double?> cacheSortedScore(
    String namespace,
    String key,
    Object member, {
    String? database,
  }) async {
    final response = await _cache(
        'ZScore',
        {
          'namespace': namespace,
          'key': key,
          'member': _bytes(member, 'member'),
        },
        database);
    return _doubleOrNull(_json(response, 'ZScore')['score']);
  }

  /// How many members a sorted set holds.
  Future<int> cacheSortedCount(
    String namespace,
    String key, {
    String? database,
  }) async {
    final response = await _cache(
        'ZCard',
        {
          'namespace': namespace,
          'key': key,
        },
        database);
    return _json(response, 'ZCard')['cardinality'] as int;
  }

  /// A member's position in the ordering, or null when it is absent.
  /// `rev` counts from the high-scoring end.
  Future<int?> cacheSortedRank(
    String namespace,
    String key,
    Object member, {
    bool rev = false,
    String? database,
  }) async {
    final response = await _cache(
        'ZRank',
        {
          'namespace': namespace,
          'key': key,
          'member': _bytes(member, 'member'),
          'rev': rev,
        },
        database);
    return _intOrNull(_json(response, 'ZRank')['rank']);
  }

  /// Members by rank, `start..=stop` inclusive. Negative indices count
  /// from the end, so `0, -1` is the whole set.
  Future<List<ScoredMember>> cacheSortedRange(
    String namespace,
    String key,
    int start,
    int stop, {
    bool rev = false,
    bool withScores = false,
    String? database,
  }) async {
    final response = await _cache(
        'ZRange',
        {
          'namespace': namespace,
          'key': key,
          'start': start,
          'stop': stop,
          'rev': rev,
          'with_scores': withScores,
        },
        database);
    return _scoredMembers(_json(response, 'ZRange'));
  }

  /// Members by score, between two bounds. `limit` is `(offset, count)`,
  /// applied after the range is selected.
  Future<List<ScoredMember>> cacheSortedRangeByScore(
    String namespace,
    String key,
    ScoreBound min,
    ScoreBound max, {
    bool rev = false,
    bool withScores = false,
    (int, int)? limit,
    String? database,
  }) async {
    final response = await _cache(
        'ZRangeByScore',
        {
          'namespace': namespace,
          'key': key,
          'min': min.toJson(),
          'max': max.toJson(),
          'rev': rev,
          'with_scores': withScores,
          'limit': limit == null ? null : [limit.$1, limit.$2],
        },
        database);
    return _scoredMembers(_json(response, 'ZRangeByScore'));
  }

  /// Add to a member's score, creating it at that score when absent.
  /// Returns the resulting score.
  Future<double> cacheSortedIncrement(
    String namespace,
    String key,
    Object member,
    double by, {
    String? database,
  }) async {
    final response = await _cache(
        'ZIncrBy',
        {
          'namespace': namespace,
          'key': key,
          'member': _bytes(member, 'member'),
          'by': by,
        },
        database);
    return (_json(response, 'ZIncrBy')['score'] as num).toDouble();
  }

  /// Take a lease on a lock, or refresh one already held by `owner`.
  /// Answers `acquired`, `token` (a fencing token that strictly increases
  /// with every acquisition and is never reused), `holder` and
  /// `expiresAtMs`.
  Future<Map<String, dynamic>> cacheLockAcquire(
    String namespace,
    String key,
    String owner,
    Duration ttl, {
    String? database,
  }) async {
    final response = await _cache(
        'LockAcquire',
        {
          'namespace': namespace,
          'key': key,
          'owner': owner,
          'ttl_ms': ttl.inMilliseconds,
        },
        database);
    return _json(response, 'LockAcquire');
  }

  /// Extend a lease the caller still holds. Answers `renewed`, `token`
  /// and `expiresAtMs`. Refused once the lease has lapsed, even if nobody
  /// else has taken the lock since.
  Future<Map<String, dynamic>> cacheLockRenew(
    String namespace,
    String key,
    String owner,
    Duration ttl, {
    String? database,
  }) async {
    final response = await _cache(
        'LockRenew',
        {
          'namespace': namespace,
          'key': key,
          'owner': owner,
          'ttl_ms': ttl.inMilliseconds,
        },
        database);
    return _json(response, 'LockRenew');
  }

  /// Give up a lock. Only the current, live holder can release it.
  Future<bool> cacheLockRelease(
    String namespace,
    String key,
    String owner, {
    String? database,
  }) async {
    final response = await _cache(
        'LockRelease',
        {
          'namespace': namespace,
          'key': key,
          'owner': owner,
        },
        database);
    return _json(response, 'LockRelease')['released'] == true;
  }

  /// Set one bit of a string value, growing it with zero bytes if needed.
  /// Returns the bit's old value. Bits are numbered from the most
  /// significant bit of byte 0.
  Future<int> cacheSetBit(
    String namespace,
    String key,
    int offset,
    bool value, {
    String? database,
  }) async {
    final response = await _cache(
        'SetBit',
        {
          'namespace': namespace,
          'key': key,
          'offset': offset,
          'value': value,
        },
        database);
    return _json(response, 'SetBit')['previous'] as int;
  }

  /// Read one bit. Past the end of the value, and on a missing key, every
  /// bit is 0.
  Future<int> cacheGetBit(
    String namespace,
    String key,
    int offset, {
    String? database,
  }) async {
    final response = await _cache(
        'GetBit',
        {
          'namespace': namespace,
          'key': key,
          'offset': offset,
        },
        database);
    return _json(response, 'GetBit')['bit'] as int;
  }

  /// Count the set bits, optionally within a byte range `(start, end)`
  /// with signed indices.
  Future<int> cacheBitCount(
    String namespace,
    String key, {
    (int, int)? range,
    String? database,
  }) async {
    final response = await _cache(
        'BitCount',
        {
          'namespace': namespace,
          'key': key,
          'range': range == null ? null : [range.$1, range.$2],
        },
        database);
    return _json(response, 'BitCount')['count'] as int;
  }

  /// The position of the first bit equal to `bit`, optionally within a
  /// byte range, or -1 when there is none.
  Future<int> cacheBitPosition(
    String namespace,
    String key,
    bool bit, {
    (int, int)? range,
    String? database,
  }) async {
    final response = await _cache(
        'BitPos',
        {
          'namespace': namespace,
          'key': key,
          'bit': bit,
          'range': range == null ? null : [range.$1, range.$2],
        },
        database);
    return _json(response, 'BitPos')['position'] as int;
  }

  /// Combine several values bitwise and store the result at `dest`.
  /// Returns the result's size in bytes.
  Future<int> cacheBitOperation(
    String namespace,
    BitOperation op,
    String dest,
    List<String> keys, {
    String? database,
  }) async {
    final response = await _cache(
        'BitOp',
        {
          'namespace': namespace,
          'op': op.wire,
          'dest': dest,
          'keys': keys,
        },
        database);
    return _json(response, 'BitOp')['length'] as int;
  }

  /// Add elements to a HyperLogLog, creating it when absent. Returns
  /// whether the estimate may have moved.
  Future<bool> cacheHyperLogLogAdd(
    String namespace,
    String key,
    List<Object> elements, {
    String? database,
  }) async {
    final response = await _cache(
        'PfAdd',
        {
          'namespace': namespace,
          'key': key,
          'elements': _byteList(elements, 'elements'),
        },
        database);
    return _json(response, 'PfAdd')['changed'] == true;
  }

  /// Estimated number of distinct elements. With several keys, the
  /// estimate is of their union.
  Future<int> cacheHyperLogLogCount(
    String namespace,
    List<String> keys, {
    String? database,
  }) async {
    final response = await _cache(
        'PfCount',
        {
          'namespace': namespace,
          'keys': keys,
        },
        database);
    return _json(response, 'PfCount')['count'] as int;
  }

  /// Merge several HyperLogLogs into `dest`, which is included as a
  /// source when it already exists.
  Future<void> cacheHyperLogLogMerge(
    String namespace,
    String dest,
    List<String> sources, {
    String? database,
  }) async {
    await _cache(
        'PfMerge',
        {
          'namespace': namespace,
          'dest': dest,
          'sources': sources,
        },
        database);
  }

  /// Atomically pop an element from one end of `source` and push it onto
  /// one end of `destination`. Returns the element, or null when `source`
  /// is empty.
  Future<Uint8List?> cacheListMove(
    String namespace,
    String source,
    String destination,
    ListEnd from,
    ListEnd to, {
    String? database,
  }) async {
    final response = await _cache(
        'LMove',
        {
          'namespace': namespace,
          'source': source,
          'destination': destination,
          'from': from.wire,
          'to': to.wire,
        },
        database);
    return _cacheValue(response, 'LMove');
  }

  /// Set the absolute instant an existing live key dies. A deadline
  /// already passed deletes the key. Answers `updated` and `deleted`.
  Future<Map<String, dynamic>> cacheExpireAt(
    String namespace,
    String key,
    DateTime at, {
    String? database,
  }) async {
    final response = await _cache(
        'ExpireAt',
        {
          'namespace': namespace,
          'key': key,
          'at_ms': at.millisecondsSinceEpoch,
        },
        database);
    return _json(response, 'ExpireAt');
  }

  /// What this node's cache is doing: lookup hits and misses, the memory
  /// budget and the eviction policy. Node-wide, not per database.
  Future<Map<String, dynamic>> cacheStats({
    String? database,
  }) async {
    final response = await request({'Cache': 'Stats'}, database: database);
    return _json(response, 'Stats');
  }

  /// Delete every cache key in this database, across every namespace.
  /// Returns how many keys were removed.
  Future<int> cacheFlushDatabase({
    String? database,
  }) async {
    final response =
        await request({'Cache': 'FlushDatabase'}, database: database);
    return _json(response, 'FlushDatabase')['cleared'] as int;
  }

  /// Concatenate bytes onto what a key already holds, and return the new
  /// length. A missing key is created; an existing key keeps its expiry.
  Future<int> cacheAppend(
    String namespace,
    String key,
    Object value, {
    String? database,
  }) async {
    final response = await _cache(
        'Append',
        {
          'namespace': namespace,
          'key': key,
          'value': _bytes(value, 'value'),
        },
        database);
    return _json(response, 'Append')['length'] as int;
  }

  /// Write only if the key still holds `expected` (null: the key must be
  /// absent). A null `value` deletes the key. Answers `swapped`; on a
  /// mismatch, `current` holds the value present when it is valid UTF-8,
  /// and `currentIsBinary` is true when it is not.
  Future<Map<String, dynamic>> cacheCompareAndSwap(
    String namespace,
    String key,
    Object? expected,
    Object? value, {
    Duration? ttl,
    String? database,
  }) async {
    final response = await _cache(
        'CompareAndSwap',
        {
          'namespace': namespace,
          'key': key,
          'expected': expected == null ? null : _bytes(expected, 'expected'),
          'value': value == null ? null : _bytes(value, 'value'),
          'ttl_ms': ttl?.inMilliseconds,
        },
        database);
    return _json(response, 'CompareAndSwap');
  }

  /// Read a key and delete it in one atomic step. Null when the key was
  /// absent or expired.
  Future<Uint8List?> cacheGetAndDelete(
    String namespace,
    String key, {
    String? database,
  }) async {
    final response = await _cache(
        'GetDel',
        {
          'namespace': namespace,
          'key': key,
        },
        database);
    return _cacheValue(response, 'GetDel');
  }

  /// Write a value and return the one it replaced, atomically. Any TTL on
  /// the old value is dropped.
  Future<Uint8List?> cacheGetAndSet(
    String namespace,
    String key,
    Object value, {
    Duration? ttl,
    String? database,
  }) async {
    final response = await _cache(
        'GetSet',
        {
          'namespace': namespace,
          'key': key,
          'value': _bytes(value, 'value'),
          'ttl_ms': ttl?.inMilliseconds,
        },
        database);
    return _cacheValue(response, 'GetSet');
  }

  /// Read a key and change its expiry in the same atomic step. The value
  /// itself is never modified.
  Future<Uint8List?> cacheGetAndExpire(
    String namespace,
    String key,
    CacheExpiry expiry, {
    String? database,
  }) async {
    final response = await _cache(
        'GetEx',
        {
          'namespace': namespace,
          'key': key,
          'expiry': expiry.toJson(),
        },
        database);
    return _cacheValue(response, 'GetEx');
  }

  /// Keep only the inclusive index range of a list, dropping everything
  /// outside it. Answers `length` and `removed`.
  Future<Map<String, dynamic>> cacheListTrim(
    String namespace,
    String key,
    int start,
    int stop, {
    String? database,
  }) async {
    final response = await _cache(
        'LTrim',
        {
          'namespace': namespace,
          'key': key,
          'start': start,
          'stop': stop,
        },
        database);
    return _json(response, 'LTrim');
  }

  /// Every member of every named set, without duplicates. A missing key
  /// is the empty set.
  Future<List<Uint8List>> cacheSetUnion(
    String namespace,
    List<String> keys, {
    String? database,
  }) async {
    final response = await _cache(
        'SUnion',
        {
          'namespace': namespace,
          'keys': keys,
        },
        database);
    return _byteLists(_json(response, 'SUnion')['members']);
  }

  /// The members present in every named set.
  Future<List<Uint8List>> cacheSetIntersection(
    String namespace,
    List<String> keys, {
    String? database,
  }) async {
    final response = await _cache(
        'SInter',
        {
          'namespace': namespace,
          'keys': keys,
        },
        database);
    return _byteLists(_json(response, 'SInter')['members']);
  }

  /// The members of the first set that are in none of the rest.
  Future<List<Uint8List>> cacheSetDifference(
    String namespace,
    List<String> keys, {
    String? database,
  }) async {
    final response = await _cache(
        'SDiff',
        {
          'namespace': namespace,
          'keys': keys,
        },
        database);
    return _byteLists(_json(response, 'SDiff')['members']);
  }

  /// Random members of a set, without removing them. No count: at most
  /// one. A positive count: up to that many distinct members. A negative
  /// count: exactly that many, repeats allowed.
  Future<List<Uint8List>> cacheSetRandomMembers(
    String namespace,
    String key, {
    int? count,
    String? database,
  }) async {
    final response = await _cache(
        'SRandMember',
        {
          'namespace': namespace,
          'key': key,
          'count': count,
        },
        database);
    return _byteLists(_json(response, 'SRandMember')['members']);
  }

  /// Add to one hash field, atomically, returning its new value. A
  /// missing field starts at zero.
  Future<int> cacheHashIncrement(
    String namespace,
    String key,
    Object field,
    int by, {
    String? database,
  }) async {
    final response = await _cache(
        'HIncrBy',
        {
          'namespace': namespace,
          'key': key,
          'field': _bytes(field, 'field'),
          'by': by,
        },
        database);
    return _json(response, 'HIncrBy')['value'] as int;
  }

  /// Run cache operations in order in one round trip, each exactly as if
  /// it had been sent alone. A failing operation does not stop the rest.
  Future<List<CacheOpResult>> cachePipeline(
    List<Object> ops, {
    String? database,
  }) async {
    final response = await _cache(
        'Pipeline',
        {
          'ops': ops,
        },
        database);
    return _opResults(_json(response, 'Pipeline'));
  }

  /// Run cache operations in order holding the cache write lock, so no
  /// other writer interleaves with them. Every operation's inputs are
  /// checked before any runs; after that there is no rollback.
  Future<List<CacheOpResult>> cacheTransaction(
    List<Object> ops, {
    String? database,
  }) async {
    final response = await _cache(
        'Transaction',
        {
          'ops': ops,
        },
        database);
    return _opResults(_json(response, 'Transaction'));
  }

  /// Store a script: operations run as one transaction, where any string
  /// (or value) written `$1`, `$2`, ... is replaced by that argument at
  /// run time. Loading the same name again replaces it.
  Future<void> cacheScriptLoad(
    String name,
    List<Object> ops, {
    String? database,
  }) async {
    await _cache(
        'ScriptLoad',
        {
          'name': name,
          'ops': ops,
        },
        database);
  }

  /// Run a stored script with these arguments as one transaction.
  Future<List<CacheOpResult>> cacheScriptRun(
    String name, {
    List<String> args = const [],
    String? database,
  }) async {
    final response = await _cache(
        'ScriptRun',
        {
          'name': name,
          'args': args,
        },
        database);
    return _opResults(_json(response, 'ScriptRun'));
  }

  /// The most frequently addressed keys on this node since it started,
  /// with estimated counts. The server must be configured with `[cache]
  /// hot_keys` above zero; otherwise it refuses.
  Future<Map<String, dynamic>> cacheHotKeys({
    int? limit,
    String? database,
  }) async {
    final response = await _cache(
        'HotKeys',
        {
          'limit': limit,
        },
        database);
    return _json(response, 'HotKeys');
  }

  /// Delete every expired cache key in this database now, rather than
  /// leaving it to compaction. Returns how many went.
  Future<int> cachePurgeExpired({
    String? database,
  }) async {
    final response =
        await request({'Cache': 'PurgeExpired'}, database: database);
    return _json(response, 'PurgeExpired')['purged'] as int;
  }

  /// Pop the head of the first non-empty list among `keys`, waiting up to
  /// the timeout for one to receive an element. Null when the wait ran
  /// out.
  Future<MapEntry<String, Uint8List>?> cacheBlockingLeftPop(
    String namespace,
    List<String> keys,
    Duration timeout, {
    String? database,
  }) async {
    final response = await _cache(
        'BLPop',
        {
          'namespace': namespace,
          'keys': keys,
          'timeout_ms': timeout.inMilliseconds,
        },
        database);
    return _popped(response, 'BLPop');
  }

  /// Pop the tail of the first non-empty list among `keys`, waiting up to
  /// the timeout for one to receive an element. Null when the wait ran
  /// out.
  Future<MapEntry<String, Uint8List>?> cacheBlockingRightPop(
    String namespace,
    List<String> keys,
    Duration timeout, {
    String? database,
  }) async {
    final response = await _cache(
        'BRPop',
        {
          'namespace': namespace,
          'keys': keys,
          'timeout_ms': timeout.inMilliseconds,
        },
        database);
    return _popped(response, 'BRPop');
  }

  /// Cap a namespace at `maxKeys` live keys and `maxBytes` of values. A
  /// write that would pass either is refused. Both absent removes the
  /// limit.
  Future<Map<String, dynamic>> cacheSetNamespaceLimit(
    String namespace, {
    int? maxKeys,
    int? maxBytes,
    String? database,
  }) async {
    final response = await _cache(
        'SetNamespaceLimit',
        {
          'namespace': namespace,
          'max_keys': maxKeys,
          'max_bytes': maxBytes,
        },
        database);
    return _json(response, 'SetNamespaceLimit');
  }

  /// A namespace's limit and what it holds now: `max_keys`, `max_bytes`,
  /// `keys` and `bytes`.
  Future<Map<String, dynamic>> cacheGetNamespaceLimit(
    String namespace, {
    String? database,
  }) async {
    final response = await _cache(
        'GetNamespaceLimit',
        {
          'namespace': namespace,
        },
        database);
    return _json(response, 'GetNamespaceLimit');
  }

  /// Append a message to a channel, returning the id it was given.
  Future<int> cachePublish(
    String channel,
    String message, {
    String? database,
  }) async {
    final response = await _cache(
        'Publish',
        {
          'channel': channel,
          'message': message,
        },
        database);
    return _json(response, 'Publish')['id'] as int;
  }

  /// The messages after id `after` on these channels (a trailing `*` is a
  /// pattern). Answers `messages` and `next`, the id to pass as `after`
  /// next time.
  Future<Map<String, dynamic>> cachePoll(
    List<String> channels, {
    int after = 0,
    int limit = 100,
    String? database,
  }) async {
    final response = await _cache(
        'Poll',
        {
          'channels': channels,
          'after': after,
          'limit': limit,
        },
        database);
    return _json(response, 'Poll');
  }

  // ---- documents -----------------------------------------------------------

  /// Create a collection.
  Future<void> documentCreateCollection(
    String collection, {
    String? database,
  }) async {
    await _document('CreateCollection', {'collection': collection}, database);
  }

  /// Drop a collection and every document in it.
  Future<void> documentDropCollection(
    String collection, {
    String? database,
  }) async {
    await _document('DropCollection', {'collection': collection}, database);
  }

  /// Every collection in the database.
  Future<List<String>> documentListCollections({String? database}) async {
    final json = _json(
      await request({'Document': 'ListCollections'}, database: database),
      'ListCollections',
    );
    final collections = json['collections'];
    return collections is List ? [for (final c in collections) '$c'] : const [];
  }

  /// Insert a document, returning the id it was stored under.
  ///
  /// Inserting over an existing id is an error rather than an overwrite; see
  /// [documentUpsertOne].
  Future<String> documentInsert(
    String collection,
    Map<String, Object?> document, {
    String? id,
    String? database,
    String? txn,
  }) async {
    final json = _json(
      await _document(
        'Insert',
        {'collection': collection, 'id': id, 'document': document},
        database,
        txn,
      ),
      'Insert',
    );
    return json['id'] as String;
  }

  /// One document by id. Null when there is no such document.
  Future<Map<String, dynamic>?> documentGet(
    String collection,
    String id, {
    String? database,
    String? txn,
  }) async {
    final documents = _documents(
      await _document(
        'Get',
        {'collection': collection, 'id': id},
        database,
        txn,
      ),
      'Get',
    );
    return documents.isEmpty ? null : documents.first;
  }

  /// Every document a filter matches, at most [limit] of them.
  Future<List<Map<String, dynamic>>> documentFind(
    String collection, {
    Object filter = Filter.all,
    int? limit,
    String? database,
    String? txn,
  }) async {
    return _documents(
      await _document(
        'Find',
        {'collection': collection, 'filter': filter, 'limit': limit},
        database,
        txn,
      ),
      'Find',
    );
  }

  /// Set fields on an existing document, by dot path. This is not an upsert.
  Future<void> documentSet(
    String collection,
    String id,
    Map<String, Object?> fields, {
    String? database,
    String? txn,
  }) async {
    await _document(
      'Update',
      {'collection': collection, 'id': id, 'set': fields},
      database,
      txn,
    );
  }

  /// Set or increment fields on one document, optionally inserting it when it
  /// is absent.
  Future<Map<String, dynamic>> documentUpdateOne(
    String collection,
    String id, {
    Map<String, Object?>? set,
    Map<String, Object?>? increment,
    bool upsert = false,
    String? database,
    String? txn,
  }) async {
    return _json(
      await _document(
        'UpdateOne',
        {
          'collection': collection,
          'id': id,
          'update': _updateBody(set, increment),
          'upsert': upsert,
        },
        database,
        txn,
      ),
      'UpdateOne',
    );
  }

  /// Insert the document when the id is absent, update it when it is present.
  Future<Map<String, dynamic>> documentUpsertOne(
    String collection,
    String id, {
    Map<String, Object?>? set,
    Map<String, Object?>? increment,
    String? database,
    String? txn,
  }) =>
      documentUpdateOne(
        collection,
        id,
        set: set,
        increment: increment,
        upsert: true,
        database: database,
        txn: txn,
      );

  /// Update every document a filter matches.
  Future<Map<String, dynamic>> documentUpdateMany(
    String collection, {
    Object filter = Filter.all,
    Map<String, Object?>? set,
    Map<String, Object?>? increment,
    String? database,
    String? txn,
  }) async {
    return _json(
      await _document(
        'UpdateMany',
        {
          'collection': collection,
          'filter': filter,
          'update': _updateBody(set, increment),
        },
        database,
        txn,
      ),
      'UpdateMany',
    );
  }

  /// Delete one document by id.
  Future<void> documentDelete(
    String collection,
    String id, {
    String? database,
    String? txn,
  }) async {
    await _document(
      'Delete',
      {'collection': collection, 'id': id},
      database,
      txn,
    );
  }

  /// Create an index on a field.
  Future<void> documentCreateIndex(
    String collection, {
    required String name,
    required String field,
    bool unique = false,
    String? database,
  }) async {
    await _document(
      'CreateIndex',
      {
        'collection': collection,
        'index_name': name,
        'field': field,
        'unique': unique,
      },
      database,
    );
  }

  /// Drop an index by name.
  Future<void> documentDropIndex(
    String collection,
    String name, {
    String? database,
  }) async {
    await _document(
      'DropIndex',
      {'collection': collection, 'index_name': name},
      database,
    );
  }

  /// The indexes on a collection.
  Future<List<Map<String, dynamic>>> documentListIndexes(
    String collection, {
    String? database,
  }) async {
    final json = _json(
      await _document('ListIndexes', {'collection': collection}, database),
      'ListIndexes',
    );
    final indexes = json['indexes'];
    return indexes is List
        ? [for (final index in indexes) Map<String, dynamic>.from(index as Map)]
        : const [];
  }

  /// Statistics about a collection.
  Future<Map<String, dynamic>> documentAnalyze(
    String collection, {
    String? database,
  }) async {
    return _json(
      await _document('Analyze', {'collection': collection}, database),
      'Analyze',
    );
  }

  /// Run an aggregation pipeline, built with [Stage].
  Future<List<Map<String, dynamic>>> documentAggregate(
    String collection,
    List<Map<String, Object?>> pipeline, {
    String? database,
    String? txn,
  }) async {
    return _documents(
      await _document(
        'Aggregate',
        {'collection': collection, 'pipeline': pipeline},
        database,
        txn,
      ),
      'Aggregate',
    );
  }

  /// Swap a document's whole body for a new one: afterwards the stored
  /// body is exactly `document`, plus `_id`. With `upsert`, a missing
  /// document is created rather than refused. Answers `id`, `replaced`,
  /// `inserted` and, when asked for, `document`.
  Future<Map<String, dynamic>> documentReplace(
    String collection,
    String id,
    Map<String, Object?> document, {
    bool upsert = false,
    ReturnDocument returnDocument = ReturnDocument.none,
    int? expectedVersion,
    String? txn,
    String? database,
  }) async {
    final response = await _document(
        'Replace',
        {
          'collection': collection,
          'id': id,
          'document': document,
          'upsert': upsert,
          'return': returnDocument.wire,
          'expected_version': expectedVersion,
        },
        database,
        txn);
    return _json(response, 'Replace');
  }

  /// Insert several documents as one atomic batch: either every document
  /// lands or none does. Each document's id is its own `_id` when it
  /// carries one, and is generated otherwise. Returns the ids, in order.
  Future<List<String>> documentInsertMany(
    String collection,
    List<Map<String, Object?>> documents, {
    String? txn,
    String? database,
  }) async {
    final response = await _document(
        'InsertMany',
        {
          'collection': collection,
          'documents': documents,
        },
        database,
        txn);
    return _strings(_json(response, 'InsertMany')['ids']);
  }

  /// Delete every document a filter matches, returning how many went.
  Future<int> documentDeleteMany(
    String collection,
    Object filter, {
    String? txn,
    String? database,
  }) async {
    final response = await _document(
        'DeleteMany',
        {
          'collection': collection,
          'filter': filter,
        },
        database,
        txn);
    return _json(response, 'DeleteMany')['deleted'] as int;
  }

  /// Move a collection, with its documents and indexes, to a new name.
  /// Refused when the new name already exists.
  Future<void> documentRenameCollection(
    String collection,
    String to, {
    String? database,
  }) async {
    await _document(
        'RenameCollection',
        {
          'collection': collection,
          'to': to,
        },
        database);
  }

  /// Delete the documents a TTL index says have expired, now. Returns how
  /// many went.
  Future<int> documentPurgeExpired(
    String collection, {
    String? database,
  }) async {
    final response = await _document(
        'PurgeExpired',
        {
          'collection': collection,
        },
        database);
    return _json(response, 'PurgeExpired')['purged'] as int;
  }

  /// Run a find and describe how it ran instead of returning its
  /// documents: the plan, the index, why it scanned, the estimate,
  /// records examined and returned, and the time taken.
  Future<Map<String, dynamic>> documentExplain(
    String collection, {
    Object filter = Filter.all,
    int? limit,
    List<Map<String, Object?>> sort = const [],
    int? skip,
    String? database,
  }) async {
    final response = await _document(
        'Explain',
        {
          'collection': collection,
          'filter': filter,
          'limit': limit,
          'sort': sort,
          'skip': skip,
        },
        database);
    return _json(response, 'Explain');
  }

  /// Open a document transaction spanning requests, returning its id.
  /// Each later call given that id as `txn` reads and writes inside it,
  /// seeing its own writes and nobody else's since it began. Snapshot
  /// isolation, or serializable.
  Future<String> documentBeginTransaction({
    bool serializable = false,
    String? database,
  }) async {
    final response = await _document(
        'BeginTransaction',
        {
          'serializable': serializable,
        },
        database);
    return _json(response, 'BeginTransaction')['txn'] as String;
  }

  /// Commit a document transaction: every write lands as one atomic
  /// batch, or a conflict is returned and nothing lands.
  Future<Map<String, dynamic>> documentCommitTransaction(
    String txn, {
    String? database,
  }) async {
    final response = await request({'Document': 'CommitTransaction'},
        database: database, txn: txn);
    return _json(response, 'CommitTransaction');
  }

  /// Discard a document transaction.
  Future<void> documentRollbackTransaction(
    String txn, {
    String? database,
  }) async {
    await request({'Document': 'RollbackTransaction'},
        database: database, txn: txn);
  }

  /// Rebuild one index from the stored documents. Answers `rebuilt`,
  /// `documents` and `entries`.
  Future<Map<String, dynamic>> documentRebuildIndex(
    String collection,
    String indexName, {
    String? database,
  }) async {
    final response = await _document(
        'RebuildIndex',
        {
          'collection': collection,
          'index_name': indexName,
        },
        database);
    return _json(response, 'RebuildIndex');
  }

  /// Set a collection's options (`schema`, `defaults`, `versioned`),
  /// replacing any it had. Refused when an existing document would break
  /// the new schema.
  Future<Map<String, dynamic>> documentSetCollectionOptions(
    String collection,
    Map<String, Object?> options, {
    String? database,
  }) async {
    final response = await _document(
        'SetCollectionOptions',
        {
          'collection': collection,
          'options': options,
        },
        database);
    return _json(response, 'SetCollectionOptions');
  }

  /// A collection's options.
  Future<Map<String, dynamic>> documentGetCollectionOptions(
    String collection, {
    String? database,
  }) async {
    final response = await _document(
        'GetCollectionOptions',
        {
          'collection': collection,
        },
        database);
    return _map(_json(response, 'GetCollectionOptions')['options']);
  }

  /// Many inserts, updates, replaces and deletes on one collection,
  /// applied in order as one atomic batch: every operation lands or none
  /// does. Answers `inserted`, `inserted_ids`, `updated`, `upserted`,
  /// `replaced` and `deleted`.
  Future<Map<String, dynamic>> documentBulkWrite(
    String collection,
    List<Map<String, Object?>> ops, {
    String? txn,
    String? database,
  }) async {
    final response = await _document(
        'BulkWrite',
        {
          'collection': collection,
          'ops': ops,
        },
        database,
        txn);
    return _json(response, 'BulkWrite');
  }

  // ---- vectors -------------------------------------------------------------

  /// Create a collection. Its dimension and metric are fixed for its lifetime.
  Future<void> vectorCreateCollection(
    String collection, {
    required int dimension,
    VectorMetric metric = VectorMetric.cosine,
    VectorQuantization quantization = VectorQuantization.none,
    String? database,
  }) async {
    await _vector(
      'CreateCollection',
      {
        'collection': collection,
        'dimension': dimension,
        'metric': metric.wire,
        'quantization': quantization.wire,
      },
      database,
    );
  }

  /// Drop a collection and every vector in it.
  Future<void> vectorDropCollection(
    String collection, {
    String? database,
  }) async {
    await _vector('DropCollection', {'collection': collection}, database);
  }

  /// Store a vector under an id, replacing whatever was there.
  ///
  /// Its length must equal the collection's dimension; a mismatch is refused
  /// rather than padded or truncated.
  Future<String> vectorUpsert(
    String collection,
    String id,
    List<double> vector, {
    Map<String, Object?>? metadata,
    String? database,
  }) async {
    final json = _json(
      await _vector(
        'Upsert',
        {
          'collection': collection,
          'id': id,
          'vector': _numbers(vector),
          'metadata': metadata,
        },
        database,
      ),
      'Upsert',
    );
    return json['id'] as String;
  }

  /// One stored vector, with its metadata. Null when there is no such id.
  Future<Map<String, dynamic>?> vectorGet(
    String collection,
    String id, {
    String? database,
  }) async {
    final response = await _vector(
      'Get',
      {'collection': collection, 'id': id},
      database,
    );
    final json = response.arm('Json');
    return json is Map ? Map<String, dynamic>.from(json) : null;
  }

  /// Delete one vector. Deleting an absent id is not an error; a missing
  /// collection is.
  Future<void> vectorDelete(
    String collection,
    String id, {
    String? database,
  }) async {
    await _vector('Delete', {'collection': collection, 'id': id}, database);
  }

  /// The [topK] nearest vectors, best first.
  ///
  /// [filter] keeps only vectors whose metadata matches every entry exactly.
  Future<List<VectorHit>> vectorSearch(
    String collection,
    List<double> vector, {
    required int topK,
    Map<String, Object?>? filter,
    String? database,
  }) async {
    final json = _json(
      await _vector(
        'Search',
        {
          'collection': collection,
          'vector': _numbers(vector),
          'top_k': topK,
          'filter': filter,
        },
        database,
      ),
      'Search',
    );
    final results = json['results'];
    if (results is! List) return const [];
    return [
      for (final hit in results)
        if (hit is Map)
          VectorHit(
            '${hit['id']}',
            hit['score'] is num ? (hit['score'] as num).toDouble() : 0,
            hit['metadata'] is Map
                ? Map<String, dynamic>.from(hit['metadata'] as Map)
                : const {},
          ),
    ];
  }

  /// Every vector collection in the database.
  Future<List<String>> vectorListCollections({String? database}) async {
    final json = _json(
      await request({'Vector': 'ListCollections'}, database: database),
      'ListCollections',
    );
    final collections = json['collections'];
    return collections is List ? [for (final c in collections) '$c'] : const [];
  }

  /// A collection's dimension, metric, quantization and size.
  Future<Map<String, dynamic>> vectorDescribeCollection(
    String collection, {
    String? database,
  }) async {
    return _json(
      await _vector(
        'DescribeCollection',
        {'collection': collection},
        database,
      ),
      'DescribeCollection',
    );
  }

  /// A page of the vectors in a collection.
  Future<Map<String, dynamic>> vectorListVectors(
    String collection, {
    int? limit,
    int? offset,
    String? database,
  }) async {
    return _json(
      await _vector(
        'ListVectors',
        {'collection': collection, 'limit': limit, 'offset': offset},
        database,
      ),
      'ListVectors',
    );
  }

  /// Embed texts with the server's built-in embedder (deterministic
  /// feature hashing: lexical, no model). Nothing is stored. Answers
  /// `vectors`, `sparse`, `embedder` and `dimension`.
  Future<Map<String, dynamic>> vectorEmbed(
    List<String> texts,
    int dimension, {
    String? database,
  }) async {
    final response = await _vector(
        'Embed',
        {
          'texts': texts,
          'dimension': dimension,
        },
        database);
    return _json(response, 'Embed');
  }

  /// Embed each text at the collection's dimension and upsert the vectors
  /// as one atomic batch. Items are `{id, text, metadata}`; the text is
  /// kept in the metadata under `text`. Returns how many were written.
  Future<int> vectorUpsertText(
    String collection,
    List<Map<String, Object?>> items, {
    String? database,
  }) async {
    final response = await _vector(
        'UpsertText',
        {
          'collection': collection,
          'items': items,
        },
        database);
    return _json(response, 'UpsertText')['upserted'] as int;
  }

  /// Point an alias at a collection; a null collection removes it. Every
  /// operation naming the alias acts on the collection.
  Future<void> vectorSetAlias(
    String alias, {
    String? collection,
    String? database,
  }) async {
    await _vector(
        'SetAlias',
        {
          'alias': alias,
          'collection': collection,
        },
        database);
  }

  /// Every alias and the collection it names, as `{alias, collection}`.
  Future<List<Map<String, dynamic>>> vectorListAliases({
    String? database,
  }) async {
    final response =
        await request({'Vector': 'ListAliases'}, database: database);
    return _maps(_json(response, 'ListAliases')['aliases']);
  }

  /// Declare a named vector on a collection: a second vector per item,
  /// with its own dimension and metric.
  Future<void> vectorAddNamedVector(
    String collection,
    String name,
    int dimension, {
    VectorMetric metric = VectorMetric.cosine,
    String? database,
  }) async {
    await _vector(
        'AddNamedVector',
        {
          'collection': collection,
          'name': name,
          'dimension': dimension,
          'metric': metric.wire,
        },
        database);
  }

  /// Write an item's vectors by name (`default` is the collection's own),
  /// each with the same metadata.
  Future<void> vectorUpsertNamed(
    String collection,
    String id,
    Map<String, List<double>> vectors, {
    Map<String, Object?>? metadata,
    String? database,
  }) async {
    await _vector(
        'UpsertNamed',
        {
          'collection': collection,
          'id': id,
          'vectors': vectors,
          'metadata': metadata,
        },
        database);
  }

  /// Write many vectors as one atomic batch. Items are `{id, vector,
  /// metadata}`. Every item is validated before any is written, and a
  /// repeated id is refused. Returns how many were written.
  Future<int> vectorUpsertMany(
    String collection,
    List<Map<String, Object?>> items, {
    String? database,
  }) async {
    final response = await _vector(
        'UpsertMany',
        {
          'collection': collection,
          'items': items,
        },
        database);
    return _json(response, 'UpsertMany')['upserted'] as int;
  }

  /// Delete many vectors by id as one atomic batch, returning how many of
  /// the ids were present.
  Future<int> vectorDeleteMany(
    String collection,
    List<String> ids, {
    String? database,
  }) async {
    final response = await _vector(
        'DeleteMany',
        {
          'collection': collection,
          'ids': ids,
        },
        database);
    return _json(response, 'DeleteMany')['deleted'] as int;
  }

  /// Run several query vectors against one collection in one request. One
  /// result list per query, in the order asked, each what a single search
  /// with the same arguments returns.
  Future<List<List<VectorHit>>> vectorSearchBatch(
    String collection,
    List<List<double>> vectors,
    int topK, {
    Map<String, Object?>? filter,
    double? minScore,
    bool includeVectors = false,
    VectorSearchOptions? options,
    String? database,
  }) async {
    final response = await _vector(
        'SearchBatch',
        {
          'collection': collection,
          'vectors': [for (final vector in vectors) _numbers(vector)],
          'top_k': topK,
          'filter': filter,
          'min_score': minScore,
          'include_vectors': includeVectors,
          ...?options?.toJson(),
        },
        database);
    return _hitLists(_json(response, 'SearchBatch')['results']);
  }

  /// Delete every vector whose metadata matches a filter, as one atomic
  /// batch. An empty filter is refused.
  Future<int> vectorDeleteByFilter(
    String collection,
    Map<String, Object?> filter, {
    String? database,
  }) async {
    final response = await _vector(
        'DeleteByFilter',
        {
          'collection': collection,
          'filter': filter,
        },
        database);
    return _json(response, 'DeleteByFilter')['deleted'] as int;
  }

  /// Merge `set` into the metadata of every vector matching a filter, as
  /// one atomic batch. Vectors are never touched.
  Future<int> vectorUpdateByFilter(
    String collection,
    Map<String, Object?> filter,
    Map<String, Object?> set, {
    String? database,
  }) async {
    final response = await _vector(
        'UpdateByFilter',
        {
          'collection': collection,
          'filter': filter,
          'set': set,
        },
        database);
    return _json(response, 'UpdateByFilter')['updated'] as int;
  }

  /// How many vectors a collection holds, or, with a filter, how many
  /// match it. An exact count.
  Future<int> vectorCount(
    String collection, {
    Map<String, Object?>? filter,
    String? database,
  }) async {
    final response = await _vector(
        'Count',
        {
          'collection': collection,
          'filter': filter,
        },
        database);
    return _json(response, 'Count')['count'] as int;
  }

  /// The partitions of a collection: each distinct value of `field`
  /// (`_namespace` or `_tenant`) with its vector count, under
  /// `partitions`, plus `unpartitioned`.
  Future<Map<String, dynamic>> vectorListPartitions(
    String collection, {
    String field = '_namespace',
    String? database,
  }) async {
    final response = await _vector(
        'ListPartitions',
        {
          'collection': collection,
          'field': field,
        },
        database);
    return _json(response, 'ListPartitions');
  }

  /// Rename a collection, moving its catalog record and every vector with
  /// it. Refused when the new name already exists.
  Future<void> vectorRenameCollection(
    String collection,
    String to, {
    String? database,
  }) async {
    await _vector(
        'RenameCollection',
        {
          'collection': collection,
          'to': to,
        },
        database);
  }

  /// Rebuild the collection's ANN index from its stored vectors and
  /// persist it, clearing every tombstone.
  Future<Map<String, dynamic>> vectorRebuildIndex(
    String collection, {
    String? database,
  }) async {
    final response = await _vector(
        'RebuildIndex',
        {
          'collection': collection,
        },
        database);
    return _json(response, 'RebuildIndex');
  }

  /// The shape of the collection's ANN index: layers, degrees,
  /// tombstones, reachability and memory.
  Future<Map<String, dynamic>> vectorIndexStats(
    String collection, {
    String? database,
  }) async {
    final response = await _vector(
        'IndexStats',
        {
          'collection': collection,
        },
        database);
    return _json(response, 'IndexStats');
  }

  /// Measure the ANN index's recall: sampled stored vectors are each
  /// searched through the index and by an exact scan, and the overlap is
  /// reported under `recall`.
  Future<Map<String, dynamic>> vectorMeasureRecall(
    String collection, {
    int? sample,
    int? topK,
    int? efSearch,
    String? database,
  }) async {
    final response = await _vector(
        'MeasureRecall',
        {
          'collection': collection,
          'sample': sample,
          'top_k': topK,
          'ef_search': efSearch,
        },
        database);
    return _json(response, 'MeasureRecall');
  }

  /// Search with several query vectors and return one fused ranking.
  Future<List<VectorHit>> vectorSearchMulti(
    String collection,
    List<List<double>> vectors,
    int topK, {
    FusionMethod fusion = FusionMethod.rrf,
    Map<String, Object?>? filter,
    String? database,
  }) async {
    final response = await _vector(
        'SearchMulti',
        {
          'collection': collection,
          'vectors': [for (final vector in vectors) _numbers(vector)],
          'top_k': topK,
          'fusion': fusion.wire,
          'filter': filter,
        },
        database);
    return _hits(_json(response, 'SearchMulti')['results']);
  }

  /// Change a collection's configuration, returning it. `normalize` can
  /// only change on an empty collection; `m` and `efConstruction` shape
  /// the HNSW graph; `multiTenant` requires every vector to carry a
  /// `_tenant`.
  Future<Map<String, dynamic>> vectorSetCollectionConfig(
    String collection, {
    bool? normalize,
    int? m,
    int? efConstruction,
    bool? multiTenant,
    String? database,
  }) async {
    final response = await _vector(
        'SetCollectionConfig',
        {
          'collection': collection,
          'normalize': normalize,
          'm': m,
          'ef_construction': efConstruction,
          'multi_tenant': multiTenant,
        },
        database);
    return _json(response, 'SetCollectionConfig');
  }

  /// Copy a collection (configuration and every vector) to a new name.
  /// Refused when the new name exists.
  Future<void> vectorCopyCollection(
    String collection,
    String to, {
    String? database,
  }) async {
    await _vector(
        'CopyCollection',
        {
          'collection': collection,
          'to': to,
        },
        database);
  }

  /// Search with a query built from examples: the sum of the positive
  /// examples minus the sum of the negative ones. An example is a stored
  /// vector's id or a vector given inline. Examples named by id are left
  /// out of the results unless `excludeExamples` is false.
  Future<List<VectorHit>> vectorSearchByExamples(
    String collection,
    int topK, {
    List<Object> positive = const [],
    List<Object> negative = const [],
    Map<String, Object?>? filter,
    double? minScore,
    bool includeVectors = false,
    bool excludeExamples = true,
    VectorSearchOptions? options,
    String? database,
  }) async {
    final response = await _vector(
        'SearchByExamples',
        {
          'collection': collection,
          'top_k': topK,
          'positive': positive,
          'negative': negative,
          'filter': filter,
          'min_score': minScore,
          'include_vectors': includeVectors,
          'exclude_examples': excludeExamples,
          ...?options?.toJson(),
        },
        database);
    return _hits(_json(response, 'SearchByExamples')['results']);
  }

  // ---- graphs --------------------------------------------------------------

  /// Create a graph.
  Future<void> graphCreate(String graph, {String? database}) async {
    await _graph('CreateGraph', {'graph': graph}, database);
  }

  /// Drop a graph, with its nodes and edges.
  Future<void> graphDrop(String graph, {String? database}) async {
    await _graph('DropGraph', {'graph': graph}, database);
  }

  /// Every graph in the database.
  Future<List<String>> graphList({String? database}) async {
    final json = _json(
      await request({'Graph': 'ListGraphs'}, database: database),
      'ListGraphs',
    );
    final graphs = json['graphs'];
    return graphs is List ? [for (final g in graphs) '$g'] : const [];
  }

  /// Add a node, returning its id.
  Future<String> graphAddNode(
    String graph,
    String id, {
    List<String> labels = const [],
    Map<String, Object?> properties = const {},
    String? database,
  }) async {
    final json = _json(
      await _graph(
        'AddNode',
        {
          'graph': graph,
          'id': id,
          'labels': labels,
          'properties': properties,
        },
        database,
      ),
      'AddNode',
    );
    return json['id'] as String;
  }

  /// One node by id. Null when there is no such node.
  Future<Map<String, dynamic>?> graphGetNode(
    String graph,
    String id, {
    String? database,
  }) async {
    final response = await _graph(
      'GetNode',
      {'graph': graph, 'id': id},
      database,
    );
    final json = response.arm('Json');
    return json is Map ? Map<String, dynamic>.from(json) : null;
  }

  /// Delete a node, and the edges that touch it.
  Future<void> graphDeleteNode(
    String graph,
    String id, {
    String? database,
  }) async {
    await _graph('DeleteNode', {'graph': graph, 'id': id}, database);
  }

  /// Add an edge, returning its id.
  Future<String> graphAddEdge(
    String graph,
    String id, {
    required String from,
    required String to,
    required String label,
    Map<String, Object?> properties = const {},
    String? database,
  }) async {
    final json = _json(
      await _graph(
        'AddEdge',
        {
          'graph': graph,
          'id': id,
          'from': from,
          'to': to,
          'label': label,
          'properties': properties,
        },
        database,
      ),
      'AddEdge',
    );
    return json['id'] as String;
  }

  /// One edge by id. Null when there is no such edge.
  Future<Map<String, dynamic>?> graphGetEdge(
    String graph,
    String id, {
    String? database,
  }) async {
    final response = await _graph(
      'GetEdge',
      {'graph': graph, 'id': id},
      database,
    );
    final json = response.arm('Json');
    return json is Map ? Map<String, dynamic>.from(json) : null;
  }

  /// Delete one edge.
  Future<void> graphDeleteEdge(
    String graph,
    String id, {
    String? database,
  }) async {
    await _graph('DeleteEdge', {'graph': graph, 'id': id}, database);
  }

  /// The nodes one hop away.
  Future<List<Map<String, dynamic>>> graphNeighbors(
    String graph,
    String nodeId, {
    GraphDirection direction = GraphDirection.outgoing,
    String? label,
    int? limit,
    String? database,
  }) async {
    final json = _json(
      await _graph(
        'Neighbors',
        {
          'graph': graph,
          'node_id': nodeId,
          'direction': direction.wire,
          'label': label,
          'limit': limit,
        },
        database,
      ),
      'Neighbors',
    );
    final neighbours = json['neighbors'];
    return neighbours is List
        ? [for (final n in neighbours) Map<String, dynamic>.from(n as Map)]
        : const [];
  }

  /// How many edges touch a node.
  Future<int> graphDegree(
    String graph,
    String nodeId, {
    GraphDirection direction = GraphDirection.outgoing,
    String? database,
  }) async {
    final json = _json(
      await _graph(
        'Degree',
        {'graph': graph, 'node_id': nodeId, 'direction': direction.wire},
        database,
      ),
      'Degree',
    );
    return json['degree'] as int;
  }

  /// A bounded breadth-first walk from a node.
  Future<Map<String, dynamic>> graphTraverse(
    String graph,
    String start, {
    GraphDirection direction = GraphDirection.outgoing,
    String? label,
    int? maxDepth,
    int? limit,
    String? database,
  }) async {
    return _json(
      await _graph(
        'Traverse',
        {
          'graph': graph,
          'start': start,
          'direction': direction.wire,
          'label': label,
          'max_depth': maxDepth,
          'limit': limit,
        },
        database,
      ),
      'Traverse',
    );
  }

  /// The path with the fewest hops. "No path" is `found: false`, not an error.
  Future<Map<String, dynamic>> graphShortestPath(
    String graph, {
    required String from,
    required String to,
    GraphDirection direction = GraphDirection.outgoing,
    String? label,
    int? maxDepth,
    String? database,
  }) async {
    return _json(
      await _graph(
        'ShortestPath',
        {
          'graph': graph,
          'from': from,
          'to': to,
          'direction': direction.wire,
          'label': label,
          'max_depth': maxDepth,
        },
        database,
      ),
      'ShortestPath',
    );
  }

  /// The path with the least summed edge weight.
  Future<Map<String, dynamic>> graphWeightedShortestPath(
    String graph, {
    required String from,
    required String to,
    GraphDirection direction = GraphDirection.outgoing,
    String? label,
    String? weightProperty,
    String? database,
  }) async {
    return _json(
      await _graph(
        'WeightedShortestPath',
        {
          'graph': graph,
          'from': from,
          'to': to,
          'direction': direction.wire,
          'label': label,
          'weight_property': weightProperty,
        },
        database,
      ),
      'WeightedShortestPath',
    );
  }

  /// A page of a graph's nodes.
  Future<Map<String, dynamic>> graphListNodes(
    String graph, {
    int? limit,
    int? offset,
    String? database,
  }) async {
    return _json(
      await _graph(
        'ListNodes',
        {'graph': graph, 'limit': limit, 'offset': offset},
        database,
      ),
      'ListNodes',
    );
  }

  /// A page of a graph's edges.
  Future<Map<String, dynamic>> graphListEdges(
    String graph, {
    int? limit,
    int? offset,
    String? database,
  }) async {
    return _json(
      await _graph(
        'ListEdges',
        {'graph': graph, 'limit': limit, 'offset': offset},
        database,
      ),
      'ListEdges',
    );
  }

  /// Run a read-only Cypher query over a graph.
  Future<Map<String, dynamic>> graphQuery(
    String graph,
    String cypher, {
    String? database,
  }) async {
    return _json(
      await _graph('Query', {'graph': graph, 'cypher': cypher}, database),
      'Query',
    );
  }

  /// Write many nodes and edges as one unit: every record lands or none
  /// does. Nodes are `{id, labels, properties}` and edges `{id, from, to,
  /// label, properties}`; an edge may join nodes created in the same
  /// batch. Answers `nodes` and `edges`.
  Future<Map<String, dynamic>> graphAddMany(
    String graph, {
    List<Map<String, Object?>> nodes = const [],
    List<Map<String, Object?>> edges = const [],
    String? database,
  }) async {
    final response = await _graph(
        'AddMany',
        {
          'graph': graph,
          'nodes': nodes,
          'edges': edges,
        },
        database);
    return _json(response, 'AddMany');
  }

  /// Delete many nodes and edges as one unit. An id that is not there is
  /// skipped. With `detach`, every edge touching a removed node goes too.
  /// Answers `nodes_removed` and `edges_removed`.
  Future<Map<String, dynamic>> graphRemoveMany(
    String graph, {
    List<String> nodes = const [],
    List<String> edges = const [],
    bool detach = false,
    String? database,
  }) async {
    final response = await _graph(
        'RemoveMany',
        {
          'graph': graph,
          'nodes': nodes,
          'edges': edges,
          'detach': detach,
        },
        database);
    return _json(response, 'RemoveMany');
  }

  /// Change an existing node in place, returning it. `properties` is a
  /// JSON merge patch: a key set to a value is set, a key set to null is
  /// removed. `removeLabels` applies before `addLabels`.
  Future<Map<String, dynamic>> graphUpdateNode(
    String graph,
    String id, {
    Map<String, Object?> properties = const {},
    List<String> addLabels = const [],
    List<String> removeLabels = const [],
    String? database,
  }) async {
    final response = await _graph(
        'UpdateNode',
        {
          'graph': graph,
          'id': id,
          'properties': properties,
          'add_labels': addLabels,
          'remove_labels': removeLabels,
        },
        database);
    return _json(response, 'UpdateNode');
  }

  /// Change an existing edge in place, returning it. `properties` merges
  /// as for a node, and `label`, when given, changes the edge's type. The
  /// endpoints cannot change.
  Future<Map<String, dynamic>> graphUpdateEdge(
    String graph,
    String id, {
    Map<String, Object?> properties = const {},
    String? label,
    String? database,
  }) async {
    final response = await _graph(
        'UpdateEdge',
        {
          'graph': graph,
          'id': id,
          'properties': properties,
          'label': label,
        },
        database);
    return _json(response, 'UpdateEdge');
  }

  /// Copy a graph (nodes, edges and schema) to a new name. Refused when
  /// the new name exists. Answers `nodes` and `edges`.
  Future<Map<String, dynamic>> graphCopy(
    String graph,
    String to, {
    String? database,
  }) async {
    final response = await _graph(
        'CopyGraph',
        {
          'graph': graph,
          'to': to,
        },
        database);
    return _json(response, 'CopyGraph');
  }

  /// A bounded depth-first walk from a node: the nodes within `maxDepth`
  /// hops (default 3, at most 10), in DFS preorder, each with the
  /// shortest `depth` found.
  Future<Map<String, dynamic>> graphDfs(
    String graph,
    String start, {
    GraphDirection direction = GraphDirection.outgoing,
    String? label,
    int? maxDepth,
    int? limit,
    String? database,
  }) async {
    final response = await _graph(
        'Dfs',
        {
          'graph': graph,
          'start': start,
          'direction': direction.wire,
          'label': label,
          'max_depth': maxDepth,
          'limit': limit,
        },
        database);
    return _json(response, 'Dfs');
  }

  /// Partition the graph's nodes into connected components, largest
  /// first. `weak` ignores edge direction; `strong` groups nodes that can
  /// each reach the other.
  Future<Map<String, dynamic>> graphConnectedComponents(
    String graph, {
    ComponentKind kind = ComponentKind.weak,
    String? label,
    int? limit,
    String? database,
  }) async {
    final response = await _graph(
        'ConnectedComponents',
        {
          'graph': graph,
          'kind': kind.wire,
          'label': label,
          'limit': limit,
        },
        database);
    return _json(response, 'ConnectedComponents');
  }

  /// Whether the graph contains a cycle (`hasCycle`), and one `cycle` as
  /// evidence.
  Future<Map<String, dynamic>> graphDetectCycle(
    String graph, {
    bool directed = true,
    String? label,
    String? database,
  }) async {
    final response = await _graph(
        'DetectCycle',
        {
          'graph': graph,
          'directed': directed,
          'label': label,
        },
        database);
    return _json(response, 'DetectCycle');
  }

  /// Counts describing a graph: nodes and edges, per node label and per
  /// edge type, self-loops, isolated nodes, degree extremes, and edges
  /// whose endpoint no longer exists.
  Future<Map<String, dynamic>> graphStats(
    String graph, {
    String? database,
  }) async {
    final response = await _graph(
        'GraphStats',
        {
          'graph': graph,
        },
        database);
    return _json(response, 'GraphStats');
  }

  /// The part of the graph within `maxDepth` hops of the seeds: the nodes
  /// reached and every edge joining two of them.
  Future<Map<String, dynamic>> graphSubgraph(
    String graph,
    List<String> seeds, {
    GraphDirection direction = GraphDirection.both,
    String? label,
    int? maxDepth,
    int? limit,
    String? database,
  }) async {
    final response = await _graph(
        'Subgraph',
        {
          'graph': graph,
          'seeds': seeds,
          'direction': direction.wire,
          'label': label,
          'max_depth': maxDepth,
          'limit': limit,
        },
        database);
    return _json(response, 'Subgraph');
  }

  /// What the graph's data looks like: its node labels and edge types,
  /// each with a count and the property keys seen on it.
  Future<Map<String, dynamic>> graphSchema(
    String graph, {
    String? database,
  }) async {
    final response = await _graph(
        'GraphSchema',
        {
          'graph': graph,
        },
        database);
    return _json(response, 'GraphSchema');
  }

  /// The `k` cheapest loopless paths between two nodes, cheapest first
  /// (default 3, at most 20). Without `weightProperty` every edge costs
  /// 1.
  Future<Map<String, dynamic>> graphKShortestPaths(
    String graph,
    String from,
    String to, {
    int? k,
    GraphDirection direction = GraphDirection.outgoing,
    String? label,
    String? weightProperty,
    String? database,
  }) async {
    final response = await _graph(
        'KShortestPaths',
        {
          'graph': graph,
          'from': from,
          'to': to,
          'k': k,
          'direction': direction.wire,
          'label': label,
          'weight_property': weightProperty,
        },
        database);
    return _json(response, 'KShortestPaths');
  }

  /// Every simple path between two nodes of at most `maxDepth` edges
  /// (default 5, at most 10). `complete` says whether the enumeration
  /// finished.
  Future<Map<String, dynamic>> graphAllPaths(
    String graph,
    String from,
    String to, {
    GraphDirection direction = GraphDirection.outgoing,
    String? label,
    int? maxDepth,
    int? limit,
    String? database,
  }) async {
    final response = await _graph(
        'AllPaths',
        {
          'graph': graph,
          'from': from,
          'to': to,
          'direction': direction.wire,
          'label': label,
          'max_depth': maxDepth,
          'limit': limit,
        },
        database);
    return _json(response, 'AllPaths');
  }

  /// Betweenness, closeness or eigenvector centrality: the top `limit`
  /// nodes (default 20) under `results`. `iterations` and `tolerance`
  /// tune eigenvector only.
  Future<Map<String, dynamic>> graphCentrality(
    String graph,
    CentralityMeasure measure, {
    bool directed = true,
    String? label,
    int? limit,
    int? iterations,
    double? tolerance,
    String? database,
  }) async {
    final response = await _graph(
        'Centrality',
        {
          'graph': graph,
          'measure': measure.wire,
          'directed': directed,
          'label': label,
          'limit': limit,
          'iterations': iterations,
          'tolerance': tolerance,
        },
        database);
    return _json(response, 'Centrality');
  }

  /// Communities by Louvain modularity optimisation, direction ignored,
  /// largest first, with the partition's `modularity`.
  Future<Map<String, dynamic>> graphCommunities(
    String graph, {
    String? label,
    int? iterations,
    int? limit,
    String? database,
  }) async {
    final response = await _graph(
        'Communities',
        {
          'graph': graph,
          'label': label,
          'iterations': iterations,
          'limit': limit,
        },
        database);
    return _json(response, 'Communities');
  }

  /// Triangles and clustering coefficients, direction ignored.
  Future<Map<String, dynamic>> graphTriangleCount(
    String graph, {
    String? label,
    int? limit,
    String? database,
  }) async {
    final response = await _graph(
        'TriangleCount',
        {
          'graph': graph,
          'label': label,
          'limit': limit,
        },
        database);
    return _json(response, 'TriangleCount');
  }

  /// The nodes whose neighbour sets most resemble a node's, under
  /// `results`.
  Future<Map<String, dynamic>> graphNodeSimilarity(
    String graph,
    String node, {
    GraphDirection direction = GraphDirection.both,
    String? label,
    SimilarityMetric metric = SimilarityMetric.jaccard,
    int? limit,
    String? database,
  }) async {
    final response = await _graph(
        'NodeSimilarity',
        {
          'graph': graph,
          'node': node,
          'direction': direction.wire,
          'label': label,
          'metric': metric.wire,
          'limit': limit,
        },
        database);
    return _json(response, 'NodeSimilarity');
  }

  /// Check a graph's stored records against each other and against the
  /// resident mirror: dangling edges, undecodable records, misfiled
  /// records and any disagreement between mirror and storage.
  Future<Map<String, dynamic>> graphCheckConsistency(
    String graph, {
    String? database,
  }) async {
    final response = await _graph(
        'CheckConsistency',
        {
          'graph': graph,
        },
        database);
    return _json(response, 'CheckConsistency');
  }

  /// Index node properties for finding nodes. With one property it is a
  /// property index, with several a composite one; `label` limits it to
  /// nodes carrying that label.
  Future<void> graphCreateNodeIndex(
    String graph,
    String name,
    List<String> properties, {
    String? label,
    String? database,
  }) async {
    await _graph(
        'CreateNodeIndex',
        {
          'graph': graph,
          'name': name,
          'properties': properties,
          'label': label,
        },
        database);
  }

  /// Enforce a rule on a property of every node (or, with target `edge`,
  /// every relationship) carrying a label. Refused when a stored record
  /// already breaks it; afterwards every write that would break it is
  /// refused.
  Future<void> graphCreateConstraint(
    String graph,
    String name,
    String label,
    String property,
    ConstraintKind kind, {
    ConstraintTarget target = ConstraintTarget.node,
    String? database,
  }) async {
    await _graph(
        'CreateConstraint',
        {
          'graph': graph,
          'name': name,
          'label': label,
          'property': property,
          'kind': kind.wire,
          'target': target.wire,
        },
        database);
  }

  /// Drop an index or a constraint by name.
  Future<void> graphDropSchemaObject(
    String graph,
    String name, {
    String? database,
  }) async {
    await _graph(
        'DropSchemaObject',
        {
          'graph': graph,
          'name': name,
        },
        database);
  }

  /// A graph's `indexes` and `constraints`.
  Future<Map<String, dynamic>> graphListSchemaObjects(
    String graph, {
    String? database,
  }) async {
    final response = await _graph(
        'ListSchemaObjects',
        {
          'graph': graph,
        },
        database);
    return _json(response, 'ListSchemaObjects');
  }

  /// Nodes with a label (when given) whose properties equal the given
  /// values, in id order. `plan` says whether an index, the label index
  /// or a scan answered.
  Future<Map<String, dynamic>> graphFindNodes(
    String graph, {
    String? label,
    Map<String, Object?> properties = const {},
    int? limit,
    String? database,
  }) async {
    final response = await _graph(
        'FindNodes',
        {
          'graph': graph,
          'label': label,
          'properties': properties,
          'limit': limit,
        },
        database);
    return _json(response, 'FindNodes');
  }

  /// Import nodes and edges from CSV text as one atomic batch, so
  /// constraints apply. Answers `nodes` and `edges`.
  Future<Map<String, dynamic>> graphImportCsv(
    String graph, {
    String? nodes,
    String? edges,
    String? database,
  }) async {
    final response = await _graph(
        'ImportCsv',
        {
          'graph': graph,
          'nodes': nodes,
          'edges': edges,
        },
        database);
    return _json(response, 'ImportCsv');
  }

  /// PageRank over edge direction: the top `limit` nodes (default 20)
  /// under `ranks`. `damping` defaults to 0.85.
  Future<Map<String, dynamic>> graphPageRank(
    String graph, {
    double? damping,
    int? iterations,
    double? tolerance,
    String? label,
    int? limit,
    String? database,
  }) async {
    final response = await _graph(
        'PageRank',
        {
          'graph': graph,
          'damping': damping,
          'iterations': iterations,
          'tolerance': tolerance,
          'label': label,
          'limit': limit,
        },
        database);
    return _json(response, 'PageRank');
  }

  /// Run a Cypher write: `CREATE`, `MERGE`, `SET`, `REMOVE` or `[DETACH]
  /// DELETE`, optionally after `MATCH ... [WHERE ...]`. Given a
  /// transaction id as `txn`, the statement is queued rather than
  /// applied.
  Future<Map<String, dynamic>> graphExecute(
    String graph,
    String cypher, {
    String? txn,
    String? database,
  }) async {
    final response = await _graph(
        'Execute',
        {
          'graph': graph,
          'cypher': cypher,
        },
        database,
        txn);
    return _json(response, 'Execute');
  }

  /// Open a transaction on a graph, returning its id. An execute given
  /// that id as `txn` is queued until the transaction commits.
  Future<String> graphBeginTransaction(
    String graph, {
    String? database,
  }) async {
    final response = await _graph(
        'BeginTransaction',
        {
          'graph': graph,
        },
        database);
    return _json(response, 'BeginTransaction')['transaction'] as String;
  }

  /// Apply every statement queued in a graph transaction as one: all of
  /// them or none.
  Future<Map<String, dynamic>> graphCommitTransaction(
    String graph,
    String txn, {
    String? database,
  }) async {
    final response = await _graph(
        'CommitTransaction',
        {
          'graph': graph,
        },
        database,
        txn);
    return _json(response, 'CommitTransaction');
  }

  /// Discard a graph transaction and its queued statements.
  Future<void> graphRollbackTransaction(
    String graph,
    String txn, {
    String? database,
  }) async {
    await _graph(
        'RollbackTransaction',
        {
          'graph': graph,
        },
        database,
        txn);
  }

  // ---- LLM -----------------------------------------------------------------

  /// Export the schema catalogue, rendered for a model or for a person.
  ///
  /// Returns text for `toon` and `markdown`, and a JSON value otherwise.
  Future<Object?> llmSchema({
    LlmFormat format = LlmFormat.toon,
    int? maxRows,
    bool redactSensitive = true,
    bool includeSchema = false,
    String? database,
  }) async {
    return _rendered(
      await request(
        {
          'Llm': {
            'Schema': {
              'format': format.wire,
              'options': _llmOptions(maxRows, redactSensitive, includeSchema),
            },
          },
        },
        database: database,
      ),
    );
  }

  /// Assemble a context bundle from read-only sources, built with [LlmSource].
  Future<Object?> llmContext(
    List<Map<String, Object?>> sources, {
    LlmFormat format = LlmFormat.toon,
    int? maxRows,
    bool redactSensitive = true,
    bool includeSchema = false,
    String? database,
  }) async {
    if (sources.isEmpty) {
      throw ArgumentError('a context bundle needs at least one source');
    }
    return _rendered(
      await request(
        {
          'Llm': {
            'Context': {
              'sources': sources,
              'format': format.wire,
              'options': _llmOptions(maxRows, redactSensitive, includeSchema),
            },
          },
        },
        database: database,
      ),
    );
  }

  // ---- admin ---------------------------------------------------------------

  /// A round trip through the whole pipeline: authentication, routing and
  /// dispatch, not just the socket.
  Future<void> adminPing({String? database}) =>
      request({'Admin': 'Ping'}, database: database);

  /// What the server says about itself.
  Future<Map<String, dynamic>> adminStatus({String? database}) async {
    final response = await request({'Admin': 'Status'}, database: database);
    if (response.hasArm('Message')) {
      return {'message': response.arm('Message')};
    }
    return _json(response, 'Status');
  }

  // ---- internals -----------------------------------------------------------

  /// Hold the connection for one frame pair. Anything that fails between the
  /// write and the end of the read closes the connection: the stream position
  /// is no longer known.
  Future<InboundFrame> _exchange(
    int tag,
    Object? payload, {
    Duration? timeout,
  }) {
    return _serialised(() async {
      if (isClosed) {
        throw const ConnectionException('this connection is closed');
      }
      _transport.writeFrame(tag, payload);
      try {
        return await _transport.readFrame(timeout ?? readTimeout);
      } on Object {
        _transactionOpen = false;
        rethrow;
      }
    });
  }

  /// Run [work] after everything already queued: one request is in flight at a
  /// time, whoever asked for it.
  Future<T> _serialised<T>(Future<T> Function() work) {
    final release = Completer<void>();
    final previous = _gate;
    _gate = release.future;
    return previous.then((_) async {
      try {
        return await work();
      } finally {
        release.complete();
      }
    });
  }

  Never _failStream(TriCoreException error) {
    _transport.close();
    _transactionOpen = false;
    throw error;
  }

  String _nextRequestId() {
    _requestCounter += 1;
    return _lastRequestId = '$_requestPrefix-$_requestCounter';
  }

  void _requireFeature(bool granted, String name, String what) {
    if (granted) return;
    throw FeatureNotGrantedException(
      name,
      'the server did not grant $name in the handshake, so this connection '
      'cannot use $what',
    );
  }

  Map<String, Object?> _sqlBody(String sql, List<Object?>? params) {
    final body = <String, Object?>{'sql': sql};
    if (params == null || params.isEmpty) return body;
    _requireFeature(
      _granted.serverParams,
      'SERVER_PARAMS',
      'server-side `?` parameters; this driver will not render values into '
          'SQL text instead, because escaping and binding are not the same '
          'guarantee',
    );
    body['params'] = Params.encodeAll(params);
    return body;
  }

  Future<Map<String, dynamic>> _transactionControl(
    String keyword,
    String? database,
  ) async {
    Response response;
    try {
      response = await request(
        {
          'Sql': {
            'Exec': {'sql': keyword},
          },
        },
        database: database,
      );
    } on TriCoreException {
      if (keyword != 'BEGIN') _transactionOpen = false;
      rethrow;
    }
    final json = _json(response, keyword);
    // A BEGIN inside an open transaction opens a nested block, and the COMMIT
    // or ROLLBACK that ends it answers with `nested_depth`: the outer
    // transaction is still open.
    _transactionOpen = keyword == 'BEGIN' || json.containsKey('nested_depth');
    return json;
  }

  Future<Response> _cache(
    String variant,
    Map<String, Object?> body,
    String? database,
  ) =>
      request({
        'Cache': {variant: body},
      }, database: database);

  Future<Response> _document(
    String variant,
    Map<String, Object?> body,
    String? database, [
    String? txn,
  ]) =>
      request({
        'Document': {variant: body},
      }, database: database, txn: txn);

  Future<Response> _vector(
    String variant,
    Map<String, Object?> body,
    String? database,
  ) =>
      request({
        'Vector': {variant: body},
      }, database: database);

  Future<Response> _graph(
    String variant,
    Map<String, Object?> body,
    String? database, [
    String? txn,
  ]) =>
      request({
        'Graph': {variant: body},
      }, database: database, txn: txn);

  Future<int> _cacheListPush(
    String variant,
    String namespace,
    String key,
    List<Object> values,
    String? database,
  ) async {
    final json = _json(
      await _cache(
        variant,
        {
          ..._namespaceKey(namespace, key),
          'values': _byteList(values, 'values'),
        },
        database,
      ),
      variant,
    );
    return json['length'] as int;
  }

  static Map<String, Object?> _namespaceKey(String namespace, String key) =>
      {'namespace': namespace, 'key': key};

  static Map<String, dynamic> _json(Response response, String what) {
    if (!response.hasArm('Json')) {
      throw ProtocolException(
        'expected Json for $what, got ${response.kind}',
      );
    }
    final json = response.arm('Json');
    return json is Map ? Map<String, dynamic>.from(json) : const {};
  }

  static List<Map<String, dynamic>> _documents(
    Response response,
    String what,
  ) {
    if (!response.hasArm('Documents')) {
      throw ProtocolException(
        'expected Documents for $what, got ${response.kind}',
      );
    }
    final documents = response.arm('Documents');
    return documents is List
        ? [
            for (final document in documents)
              Map<String, dynamic>.from(document as Map),
          ]
        : const [];
  }

  static Uint8List? _cacheValue(Response response, String what) {
    if (!response.hasArm('CacheValue')) {
      throw ProtocolException(
        'expected CacheValue for $what, got ${response.kind}',
      );
    }
    final value = response.arm('CacheValue');
    return value == null ? null : _toBytes(value);
  }

  static Object? _rendered(Response response) {
    for (final arm in const ['Toon', 'Json', 'Message']) {
      if (response.hasArm(arm)) return response.arm(arm);
    }
    throw ProtocolException(
      'expected a rendered export, got ${response.kind}',
    );
  }

  static Uint8List _toBytes(Object? value) {
    if (value is List) {
      return Uint8List.fromList([for (final byte in value) byte as int]);
    }
    throw ProtocolException('expected a byte array, got $value');
  }

  static List<Uint8List> _byteLists(Object? values) {
    return values is List
        ? [for (final value in values) _toBytes(value)]
        : const [];
  }

  /// A cache value on the wire is an array of byte numbers.
  static List<int> _bytes(Object value, String what) {
    if (value is String) return utf8.encode(value);
    if (value is List<int>) {
      for (final byte in value) {
        if (byte < 0 || byte > 255) {
          throw ArgumentError('$what as a List<int> must hold bytes 0..255');
        }
      }
      return value;
    }
    throw ArgumentError(
      '$what must be a String or a List<int>, got ${value.runtimeType}',
    );
  }

  static List<List<int>> _byteList(List<Object> values, String what) {
    if (values.isEmpty) {
      throw ArgumentError('$what must not be empty');
    }
    return [for (final value in values) _bytes(value, '$what element')];
  }

  static List<List<List<int>>> _pairs(Map<Object, Object> entries) {
    if (entries.isEmpty) {
      throw ArgumentError('entries must not be empty');
    }
    return [
      for (final entry in entries.entries)
        [_bytes(entry.key, 'field'), _bytes(entry.value, 'value')],
    ];
  }

  static List<StreamEntry> _streamEntries(Map<String, dynamic> json) {
    final entries = json['entries'];
    if (entries is! List) return const [];
    return [
      for (final entry in entries)
        if (entry is Map)
          StreamEntry(
            '${entry['id']}',
            [
              if (entry['fields'] is List)
                for (final field in entry['fields'] as List)
                  if (field is List && field.length == 2)
                    MapEntry(_toBytes(field[0]), _toBytes(field[1])),
            ],
          ),
    ];
  }

  static Map<String, Object?> _updateBody(
    Map<String, Object?>? set,
    Map<String, Object?>? increment,
  ) {
    return {
      if (set != null && set.isNotEmpty) 'set': set,
      if (increment != null && increment.isNotEmpty) 'inc': increment,
    };
  }

  static Map<String, Object?> _llmOptions(
    int? maxRows,
    bool redactSensitive,
    bool includeSchema,
  ) =>
      {
        'max_rows': maxRows,
        'redact_sensitive': redactSensitive,
        'include_schema': includeSchema,
      };

  static List<double> _numbers(List<double> values) {
    for (final value in values) {
      if (value.isNaN || value.isInfinite) {
        throw ArgumentError('vector components must be finite numbers');
      }
    }
    return values;
  }

  static int? _intOrNull(Object? value) => value is int ? value : null;

  static double? _doubleOrNull(Object? value) =>
      value is num ? value.toDouble() : null;

  static List<String> _strings(Object? values) =>
      values is List ? [for (final value in values) '$value'] : const [];

  static Map<String, dynamic> _map(Object? value) =>
      value is Map ? Map<String, dynamic>.from(value) : const {};

  static List<Map<String, dynamic>> _maps(Object? values) => values is List
      ? [
          for (final value in values)
            if (value is Map) Map<String, dynamic>.from(value),
        ]
      : const [];

  static List<Uint8List?> _cacheValues(Response response, String what) {
    if (!response.hasArm('CacheValues')) {
      throw ProtocolException(
        'expected CacheValues for $what, got ${response.kind}',
      );
    }
    final values = response.arm('CacheValues');
    return values is List
        ? [for (final value in values) value == null ? null : _toBytes(value)]
        : const [];
  }

  /// A blocking pop answers `[key, value]`, or `[null, null]` when the wait
  /// ran out.
  static MapEntry<String, Uint8List>? _popped(Response response, String what) {
    final values = _cacheValues(response, what);
    if (values.length != 2 || values[0] == null || values[1] == null) {
      return null;
    }
    return MapEntry(utf8.decode(values[0]!, allowMalformed: true), values[1]!);
  }

  static List<Map<String, Object?>> _entries(Map<String, Object> entries) {
    if (entries.isEmpty) {
      throw ArgumentError('entries must not be empty');
    }
    return [
      for (final entry in entries.entries)
        {'key': entry.key, 'value': _bytes(entry.value, 'value')},
    ];
  }

  static List<Map<String, Object?>> _scored(Map<Object, double> members) {
    if (members.isEmpty) {
      throw ArgumentError('members must not be empty');
    }
    return [
      for (final entry in members.entries)
        {
          'score': _score(entry.value),
          'member': _bytes(entry.key, 'member'),
        },
    ];
  }

  static double _score(double score) {
    if (score.isNaN || score.isInfinite) {
      throw ArgumentError('a score must be a finite number');
    }
    return score;
  }

  /// A range read with scores answers `{member, score}` objects; without, the
  /// members alone.
  static List<ScoredMember> _scoredMembers(Map<String, dynamic> json) {
    final members = json['members'];
    if (members is! List) return const [];
    return [
      for (final member in members)
        if (member is Map)
          ScoredMember(
            _toBytes(member['member']),
            _doubleOrNull(member['score']),
          )
        else
          ScoredMember(_toBytes(member), null),
    ];
  }

  static List<CacheOpResult> _opResults(Map<String, dynamic> json) {
    final results = json['results'];
    return results is List
        ? [for (final result in results) CacheOpResult.fromJson(result)]
        : const [];
  }

  static List<VectorHit> _hits(Object? results) {
    if (results is! List) return const [];
    return [
      for (final hit in results)
        if (hit is Map)
          VectorHit(
            '${hit['id']}',
            hit['score'] is num ? (hit['score'] as num).toDouble() : 0,
            hit['metadata'] is Map
                ? Map<String, dynamic>.from(hit['metadata'] as Map)
                : const {},
            vector: hit['vector'] is List
                ? [
                    for (final component in hit['vector'] as List)
                      (component as num).toDouble(),
                  ]
                : null,
            reason: hit['reason'] is String ? hit['reason'] as String : null,
            matched: hit['matched'] is int ? hit['matched'] as int : null,
          ),
    ];
  }

  static List<List<VectorHit>> _hitLists(Object? results) =>
      results is List ? [for (final list in results) _hits(list)] : const [];

  static String _errorText(Object? body) {
    if (body is Map) {
      final message = body['message'] ?? body['error'];
      return message is String ? message : json.encode(body);
    }
    return '$body';
  }

  static String? _errorCode(Object? body) {
    if (body is! Map) return null;
    final code = body['code'];
    return code is String && code.isNotEmpty ? code : null;
  }

  static String? _leaderHint(Object? body) {
    if (body is! Map) return null;
    final hint = body['leader_hint'];
    return hint is String && hint.isNotEmpty ? hint : null;
  }
}

/// One result of a vector search.
class VectorHit {
  /// The vector's id.
  final String id;

  /// How close it is. Higher is closer for every metric — see [VectorMetric].
  final double score;

  /// Whatever metadata was stored with it.
  final Map<String, dynamic> metadata;

  /// The stored vector, when the search asked for vectors.
  final List<double>? vector;

  /// A one-line plain-language reason, when the search asked to explain.
  final String? reason;

  /// How many of a multi-query search's queries ranked this vector.
  final int? matched;

  /// A hit with this id, score and metadata.
  const VectorHit(
    this.id,
    this.score,
    this.metadata, {
    this.vector,
    this.reason,
    this.matched,
  });

  @override
  String toString() => 'VectorHit($id, $score)';
}

/// What [TriCore.connect] was given, to open a connection to the leader the
/// same way.
class _ConnectOptions {
  final String? user;
  final String secret;
  final Duration? connectTimeout;
  final TlsOptions? tls;
  final String? clientName;
  final int features;

  const _ConnectOptions({
    required this.user,
    required this.secret,
    required this.connectTimeout,
    required this.tls,
    required this.clientName,
    required this.features,
  });
}
