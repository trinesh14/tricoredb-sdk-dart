/// The code a not-leader refusal carries in `diagnostics.error_code`.
const String notLeaderCode = 'not_leader';

/// Every failure this package raises.
///
/// Branch on [code], never on [message]: the message is prose the server may
/// reword, the code is contract.
class TriCoreException implements Exception {
  /// What went wrong, in prose.
  final String message;

  /// The machine-readable reason, when the server sent one.
  final String? code;

  /// A `host:port` for the current leader, only alongside `not_leader`.
  final String? leaderHint;

  /// A failure with this message, and the server's code when there was one.
  const TriCoreException(this.message, {this.code, this.leaderHint});

  /// Whether the request was right but reached a node that is not the leader.
  ///
  /// This driver never follows [leaderHint] on its own: the address may not be
  /// reachable from here, a new connection has to authenticate again, and an
  /// open session transaction cannot move to another node at all.
  bool get isRedirect => code == notLeaderCode;

  @override
  String toString() {
    final suffix = code == null ? '' : ' [$code]';
    return '$runtimeType: $message$suffix';
  }
}

/// The server refused the credentials — an AUTH_OK frame carrying `ok: false`,
/// or an ERROR frame in its place.
class AuthException extends TriCoreException {
  /// A refused login.
  const AuthException(super.message, {super.code});
}

/// The byte stream can no longer be trusted: a bad header, a frame above the
/// protocol's ceiling, an unexpected tag, or a refused handshake. The
/// connection is closed, because a length-prefixed stream has no
/// resynchronisation point.
class ProtocolException extends TriCoreException {
  /// A stream this driver will not keep reading.
  const ProtocolException(super.message, {super.code});
}

/// The socket failed or was closed. The connection is unusable.
class ConnectionException extends TriCoreException {
  /// A socket that failed or went away.
  const ConnectionException(super.message, {super.code});
}

/// No reply arrived within the connection's read timeout.
///
/// The connection is closed: the late reply would otherwise be read as the
/// answer to the next request.
class ReadTimeoutException extends ConnectionException {
  /// A reply that never came.
  const ReadTimeoutException(super.message);
}

/// The server processed the request and did not complete it: a RESPONSE whose
/// status is not `ok`, or an ERROR frame answering a request.
class ServerException extends TriCoreException {
  /// `error` or `not_implemented` for a RESPONSE; null for an ERROR frame.
  final String? status;

  /// Whether the refusal arrived as an ERROR frame rather than a RESPONSE.
  final bool fromErrorFrame;

  /// A request the server refused.
  const ServerException(
    super.message, {
    super.code,
    super.leaderHint,
    this.status,
    this.fromErrorFrame = false,
  });
}

/// The server did not grant a capability this call needs. Thrown before
/// anything is sent, so the connection is untouched.
class FeatureNotGrantedException extends TriCoreException {
  /// The capability's name, such as `SERVER_PARAMS`.
  final String feature;

  /// A call that needs a capability the handshake did not grant.
  const FeatureNotGrantedException(this.feature, String message)
      : super(message);
}

/// A SQL parameter this driver will not encode. Thrown before anything is sent.
class ParameterException extends TriCoreException {
  /// The parameter's 1-based position.
  final int index;

  /// A parameter at [index] that has no SQL form, and why.
  ParameterException(this.index, String reason)
      : super('parameter #$index: $reason');
}

/// A pool checkout waited longer than its timeout.
class PoolTimeoutException extends TriCoreException {
  /// A checkout that waited too long.
  const PoolTimeoutException(super.message);
}
