import 'dart:async';
import 'dart:collection';

import 'client.dart';
import 'errors.dart';
import 'options.dart';

/// A bounded pool of connections.
///
/// A connection is a single request/response stream, so concurrency means more
/// than one of them. The pool opens connections on demand up to [size], lends
/// each to one caller at a time, and never lends back a connection with a
/// transaction still open.
///
/// ```dart
/// final pool = TriCorePool.to(host: '127.0.0.1', user: 'admin', secret: 'pw');
/// final rows = await pool.withConnection((db) => db.query('SELECT 1'));
/// await pool.close();
/// ```
class TriCorePool {
  final Future<TriCore> Function() _open;

  /// The most connections this pool will open.
  final int size;

  /// How long a caller waits for a free connection. Null waits indefinitely.
  final Duration? checkoutTimeout;

  final Queue<TriCore> _idle = Queue();
  final Queue<Completer<TriCore>> _waiting = Queue();
  int _lent = 0;
  bool _closed = false;

  /// A pool that opens connections with [open].
  TriCorePool(
    Future<TriCore> Function() open, {
    this.size = 8,
    this.checkoutTimeout,
  })  : _open = open,
        assert(size > 0, 'a pool needs room for at least one connection');

  /// A pool of connections to one server, with the usual settings.
  factory TriCorePool.to({
    String host = '127.0.0.1',
    int port = defaultPort,
    String? user,
    String secret = '',
    String database = defaultDatabase,
    Duration? connectTimeout = const Duration(seconds: 10),
    Duration? readTimeout,
    Duration? requestTimeout,
    TlsOptions? tls,
    int size = 8,
    Duration? checkoutTimeout,
  }) {
    return TriCorePool(
      () => TriCore.connect(
        host: host,
        port: port,
        user: user,
        secret: secret,
        database: database,
        connectTimeout: connectTimeout,
        readTimeout: readTimeout,
        requestTimeout: requestTimeout,
        tls: tls,
      ),
      size: size,
      checkoutTimeout: checkoutTimeout,
    );
  }

  /// How many connections are waiting, and how many are out on loan.
  ({int idle, int lent}) get stats => (idle: _idle.length, lent: _lent);

  /// Whether this pool has been closed.
  bool get isClosed => _closed;

  /// Lend a connection to [work] for the duration of the call.
  ///
  /// The connection goes back to the pool afterwards, whether [work] returned
  /// or threw. Do not hold on to it past the call.
  Future<T> withConnection<T>(Future<T> Function(TriCore db) work) async {
    final connection = await _checkout();
    try {
      return await work(connection);
    } finally {
      await _checkin(connection);
    }
  }

  /// Close every connection, and refuse later checkouts.
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    while (_waiting.isNotEmpty) {
      final waiter = _waiting.removeFirst();
      if (!waiter.isCompleted) {
        waiter.completeError(const PoolTimeoutException('the pool is closed'));
      }
    }
    final connections = _idle.toList();
    _idle.clear();
    for (final connection in connections) {
      await connection.close();
    }
  }

  Future<TriCore> _checkout() async {
    if (_closed) {
      throw const PoolTimeoutException('the pool is closed');
    }
    while (_idle.isNotEmpty) {
      final connection = _idle.removeFirst();
      if (!connection.isClosed) {
        _lent += 1;
        return connection;
      }
    }
    if (_lent < size) {
      _lent += 1;
      try {
        return await _open();
      } on Object {
        _lent -= 1;
        rethrow;
      }
    }
    final waiter = Completer<TriCore>();
    _waiting.add(waiter);
    if (checkoutTimeout == null) return waiter.future;
    return waiter.future.timeout(checkoutTimeout!, onTimeout: () {
      _waiting.remove(waiter);
      throw PoolTimeoutException(
        'no connection became free within $checkoutTimeout '
        '($size in use)',
      );
    });
  }

  Future<void> _checkin(TriCore connection) async {
    var usable = !connection.isClosed;
    // A transaction the caller left open would otherwise become the next
    // caller's problem, on a connection they never opened it on.
    if (usable && connection.inTransaction) {
      try {
        await connection.rollback();
      } on Object {
        usable = false;
      }
    }
    if (_closed || !usable) {
      _lent -= 1;
      await connection.close();
      return;
    }
    while (_waiting.isNotEmpty) {
      final waiter = _waiting.removeFirst();
      if (!waiter.isCompleted) {
        waiter.complete(connection);
        return;
      }
    }
    _lent -= 1;
    _idle.add(connection);
  }
}
