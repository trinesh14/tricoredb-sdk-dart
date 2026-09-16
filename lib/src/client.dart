import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

import 'builders.dart';
import 'errors.dart';
import 'frame.dart';
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

  final Transport _transport;

  /// The database used when a call does not name one.
  String database;

  /// How long to wait for each reply. Null waits for as long as the statement
  /// runs.
  Duration? readTimeout;

  /// A server-side deadline sent with every request. Null lets the server use
  /// its own.
  Duration? requestTimeout;

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
    return client;
  }

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
  /// Returns only when the status is `ok`; any other status throws a
  /// [ServerException] carrying the server's code.
  Future<Response> request(
    Object op, {
    String? database,
    String? correlationId,
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
    final response = Response.fromJson(frame.body);
    if (!response.isOk) {
      final error = response.toException(transactionOpen: _transactionOpen);
      if (response.isRedirect) _transactionOpen = false;
      throw error;
    }
    return response;
  }

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
  }) async {
    final json = _json(
      await _document(
        'Insert',
        {'collection': collection, 'id': id, 'document': document},
        database,
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
  }) async {
    final documents = _documents(
      await _document('Get', {'collection': collection, 'id': id}, database),
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
  }) async {
    return _documents(
      await _document(
        'Find',
        {'collection': collection, 'filter': filter, 'limit': limit},
        database,
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
  }) async {
    await _document(
      'Update',
      {'collection': collection, 'id': id, 'set': fields},
      database,
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
  }) =>
      documentUpdateOne(
        collection,
        id,
        set: set,
        increment: increment,
        upsert: true,
        database: database,
      );

  /// Update every document a filter matches.
  Future<Map<String, dynamic>> documentUpdateMany(
    String collection, {
    Object filter = Filter.all,
    Map<String, Object?>? set,
    Map<String, Object?>? increment,
    String? database,
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
      ),
      'UpdateMany',
    );
  }

  /// Delete one document by id.
  Future<void> documentDelete(
    String collection,
    String id, {
    String? database,
  }) async {
    await _document(
      'Delete',
      {'collection': collection, 'id': id},
      database,
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
  }) async {
    return _documents(
      await _document(
        'Aggregate',
        {'collection': collection, 'pipeline': pipeline},
        database,
      ),
      'Aggregate',
    );
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
    _transactionOpen = keyword == 'BEGIN';
    return _json(response, keyword);
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
    String? database,
  ) =>
      request({
        'Document': {variant: body},
      }, database: database);

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
    String? database,
  ) =>
      request({
        'Graph': {variant: body},
      }, database: database);

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

  /// A hit with this id, score and metadata.
  const VectorHit(this.id, this.score, this.metadata);

  @override
  String toString() => 'VectorHit($id, $score)';
}
