/// How this connection is encrypted.
///
/// Pass `TlsOptions()` for the system trust store, a [caFile] for a private
/// certificate authority, and both [clientCertificateFile] and [clientKeyFile]
/// for mutual TLS.
class TlsOptions {
  /// PEM bundle used to verify the server. Without one the system trust store
  /// is used.
  final String? caFile;

  /// The name the certificate must carry, and the name sent as SNI. Defaults to
  /// the host being connected to.
  final String? serverName;

  /// This client's certificate chain, for mutual TLS.
  final String? clientCertificateFile;

  /// The key for [clientCertificateFile]. Both are needed, or neither.
  final String? clientKeyFile;

  /// The passphrase for an encrypted client key.
  final String? clientKeyPassword;

  /// Accept any certificate, verifying nothing.
  ///
  /// This turns off the guarantee TLS exists to provide. It is for a
  /// development server with a self-signed certificate, never for production.
  final bool dangerAcceptInvalidCertificates;

  /// TLS with these settings.
  const TlsOptions({
    this.caFile,
    this.serverName,
    this.clientCertificateFile,
    this.clientKeyFile,
    this.clientKeyPassword,
    this.dangerAcceptInvalidCertificates = false,
  });

  /// Whether this asks for a client identity as well as a server one.
  bool get hasClientIdentity =>
      clientCertificateFile != null || clientKeyFile != null;
}

/// Optional protocol capabilities, negotiated in the handshake as a bitmap.
abstract final class Features {
  /// The server echoes a correlation id into its logs and traces.
  static const int correlationId = 1;

  /// The server binds `?` placeholders itself, from a typed parameter array.
  static const int serverParams = 2;

  /// `BEGIN`, statements and `COMMIT`/`ROLLBACK` as separate requests on one
  /// connection.
  static const int sessionTxn = 4;

  /// Every capability this driver understands.
  static const int all = correlationId | serverParams | sessionTxn;
}

/// Which capabilities the server granted.
class GrantedFeatures {
  /// The bitmap exactly as the server sent it.
  final int mask;

  /// The capabilities this bitmap names.
  const GrantedFeatures(this.mask);

  /// Whether correlation ids are echoed.
  bool get correlationId => mask & Features.correlationId != 0;

  /// Whether the server binds `?` placeholders.
  bool get serverParams => mask & Features.serverParams != 0;

  /// Whether session transactions are available.
  bool get sessionTxn => mask & Features.sessionTxn != 0;

  /// Whether nothing at all was granted.
  bool get isEmpty => mask == 0;

  @override
  String toString() {
    final granted = [
      if (correlationId) 'CORRELATION_ID',
      if (serverParams) 'SERVER_PARAMS',
      if (sessionTxn) 'SESSION_TXN',
    ];
    return granted.isEmpty ? 'none' : granted.join(' | ');
  }
}
