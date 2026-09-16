import 'dart:async';
import 'dart:collection';
import 'dart:io';
import 'dart:typed_data';

import 'errors.dart';
import 'frame.dart';
import 'options.dart';

/// One frame read off the wire.
class InboundFrame {
  /// The frame's tag.
  final int tag;

  /// Its decoded payload, or null when the payload was empty.
  final Object? body;

  /// A frame with this tag and payload.
  const InboundFrame(this.tag, this.body);
}

/// A framed byte stream over TCP or TLS.
///
/// Frames are parsed as bytes arrive, and each header is validated before its
/// payload is read, so a declared length can never make this client allocate
/// what the peer did not send.
class Transport {
  final Socket _socket;
  final _ByteQueue _queue = _ByteQueue();
  final Queue<Completer<InboundFrame>> _waiting = Queue();

  FrameHeader? _header;
  TriCoreException? _failure;
  bool _closed = false;

  Transport._(this._socket) {
    _socket.listen(
      _onData,
      onError: (Object error) => _fail(
        ConnectionException('the connection failed: $error'),
      ),
      onDone: () => _fail(
        const ConnectionException('the server closed the connection'),
      ),
      cancelOnError: true,
    );
  }

  /// Whether this transport can no longer be used.
  bool get isClosed => _closed;

  /// Open a TCP connection, wrapped in TLS when [tls] is given.
  static Future<Transport> connect(
    String host,
    int port, {
    Duration? connectTimeout,
    TlsOptions? tls,
  }) async {
    Socket socket;
    try {
      socket = await Socket.connect(host, port, timeout: connectTimeout);
    } on SocketException catch (e) {
      throw ConnectionException(
        'connect to $host:$port failed: ${e.message}',
      );
    } on TimeoutException {
      throw ConnectionException(
        'connect to $host:$port timed out after $connectTimeout',
      );
    }
    try {
      socket.setOption(SocketOption.tcpNoDelay, true);
    } on Object {
      // Request/response round trips are latency-sensitive, so Nagle's
      // algorithm is worth turning off — but some sandboxes refuse the option,
      // and an optimisation must not be the reason a connection fails.
    }
    if (tls == null) return Transport._(socket);

    final serverName = tls.serverName ?? host;
    try {
      final secure = await SecureSocket.secure(
        socket,
        host: _sniName(serverName),
        context: _securityContext(tls),
        onBadCertificate:
            tls.dangerAcceptInvalidCertificates ? (_) => true : null,
      );
      secure.setOption(SocketOption.tcpNoDelay, true);
      return Transport._(secure);
    } on Object catch (e) {
      socket.destroy();
      // The path and the peer name, never a key or a certificate's bytes.
      throw ConnectionException('TLS with $serverName failed: $e');
    }
  }

  static SecurityContext _securityContext(TlsOptions tls) {
    if (tls.hasClientIdentity &&
        (tls.clientCertificateFile == null || tls.clientKeyFile == null)) {
      final missing = tls.clientCertificateFile == null
          ? 'clientCertificateFile'
          : 'clientKeyFile';
      throw ArgumentError(
        'tls $missing is required alongside the other: mutual TLS needs both',
      );
    }
    final context = SecurityContext(withTrustedRoots: tls.caFile == null);
    if (tls.caFile case final caFile?) {
      try {
        context.setTrustedCertificates(caFile);
      } on Object {
        throw ArgumentError('cannot read the TLS CA file at $caFile');
      }
    }
    if (tls.clientCertificateFile case final certificate?) {
      try {
        context.useCertificateChain(certificate);
        context.usePrivateKey(tls.clientKeyFile!,
            password: tls.clientKeyPassword);
      } on Object {
        throw ArgumentError(
          'cannot read the client identity (certificate $certificate, '
          'key ${tls.clientKeyFile})',
        );
      }
    }
    return context;
  }

  /// An IP address is not a valid SNI name.
  static String? _sniName(String name) {
    if (name.contains(':')) return null;
    final parts = name.split('.');
    final isIPv4 = parts.length == 4 &&
        parts.every((part) {
          final octet = int.tryParse(part);
          return octet != null && octet >= 0 && octet <= 255;
        });
    return isIPv4 ? null : name;
  }

  /// Write one frame.
  void writeFrame(int tag, Object? payload) {
    final bytes = Frame.encode(tag, payload);
    if (_closed) {
      throw const ConnectionException('this connection is closed');
    }
    try {
      _socket.add(bytes);
    } on Object catch (e) {
      final failure = ConnectionException('write failed: $e');
      _fail(failure);
      throw failure;
    }
  }

  /// Wait for the next frame, bounded by [timeout] when given.
  Future<InboundFrame> readFrame(Duration? timeout) {
    if (_failure case final failure?) return Future.error(failure);
    if (_closed) {
      return Future.error(
        const ConnectionException('this connection is closed'),
      );
    }
    final completer = Completer<InboundFrame>();
    _waiting.add(completer);
    if (timeout == null) return completer.future;
    return completer.future.timeout(timeout, onTimeout: () {
      // The reply may still be in flight, and on a length-prefixed stream it
      // would be read as the next request's answer. Drop the socket instead.
      const failure = ReadTimeoutException(
        'no reply within the read timeout; the connection is closed because '
        'the reply may still arrive and would be read as the answer to the '
        'next request',
      );
      _fail(failure);
      throw failure;
    });
  }

  /// Close the socket. Idempotent, and never throws.
  void close() {
    _fail(const ConnectionException('this connection is closed'));
  }

  void _onData(Uint8List data) {
    _queue.add(data);
    while (true) {
      if (_header == null) {
        final header = _queue.take(Frame.headerSize);
        if (header == null) return;
        try {
          _header = Frame.decodeHeader(header);
        } on TriCoreException catch (e) {
          _fail(e);
          return;
        }
      }
      final header = _header!;
      final body =
          header.length == 0 ? Uint8List(0) : _queue.take(header.length);
      if (body == null) return;
      _header = null;
      Object? decoded;
      try {
        decoded = Frame.decodeBody(body);
      } on TriCoreException catch (e) {
        _fail(e);
        return;
      }
      if (_waiting.isEmpty) continue;
      _waiting.removeFirst().complete(InboundFrame(header.tag, decoded));
    }
  }

  /// Fail every waiter, and every later one, with the same error: once the
  /// stream is out of step there is nowhere to resynchronise to.
  void _fail(TriCoreException error) {
    _failure ??= error;
    _closed = true;
    while (_waiting.isNotEmpty) {
      final completer = _waiting.removeFirst();
      if (!completer.isCompleted) completer.completeError(_failure!);
    }
    try {
      _socket.destroy();
    } on Object {
      // Already gone.
    }
  }
}

/// Bytes waiting to be read, without copying the whole buffer each time.
class _ByteQueue {
  final Queue<Uint8List> _chunks = Queue();
  int _offset = 0;
  int _length = 0;

  void add(Uint8List data) {
    if (data.isEmpty) return;
    _chunks.add(data);
    _length += data.length;
  }

  /// Exactly [n] bytes, or null when they have not all arrived.
  Uint8List? take(int n) {
    if (_length < n) return null;
    final out = Uint8List(n);
    var written = 0;
    while (written < n) {
      final chunk = _chunks.first;
      final available = chunk.length - _offset;
      final wanted = n - written;
      if (available <= wanted) {
        out.setRange(written, written + available, chunk, _offset);
        written += available;
        _chunks.removeFirst();
        _offset = 0;
      } else {
        out.setRange(written, n, chunk, _offset);
        _offset += wanted;
        written = n;
      }
    }
    _length -= n;
    return out;
  }
}
