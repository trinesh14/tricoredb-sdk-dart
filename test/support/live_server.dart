import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:tricoredb/tricoredb.dart';

/// A private `tricore-server` for the tests that need a real one.
///
/// The binary is named by `TRICORE_SERVER_BIN`, or found in a sibling
/// `tricore/tricore-db/target/{release,debug}` checkout. It is never built
/// here: building the server from a test is slow and collides with anything
/// else compiling.
///
/// When there is no binary, [skipReason] says so and the live tests skip.
/// Someone who added this package as a dependency has no server, and
/// `dart test` must still be green for them.
class LiveServer {
  static const String _configuration = '''
[server]
host = "127.0.0.1"
port = 0
protocol = "tricore"
node_id = "sdk-dart-tests"
region_id = "local"

[modules]
sql = true
document = true
cache = true
vector = true
graph = true
llm = true
cluster = true

[security]
auth_mode = "password"
dev_auth = true
allow_default_admin = false

[tls]
enabled = false
''';

  final Process _process;
  final Directory _directory;

  /// The host the server bound to.
  final String host;

  /// The port it is listening on.
  final int port;

  LiveServer._(this._process, this._directory, this.host, this.port);

  /// Why the live tests cannot run, or null when they can.
  static String? get skipReason => _binary == null
      ? 'no tricore-server binary: set TRICORE_SERVER_BIN, or run one from '
          'the Docker image (see the README)'
      : null;

  static final String? _binary = _findBinary();

  static String? _findBinary() {
    final named = Platform.environment['TRICORE_SERVER_BIN'];
    if (named != null && named.isNotEmpty) {
      return File(named).existsSync() ? named : null;
    }
    var directory = Directory.current.absolute;
    while (true) {
      for (final profile in const ['release', 'debug']) {
        final candidate = File(
          '${directory.path}/target/$profile/tricore-server',
        );
        if (candidate.existsSync()) return candidate.path;
      }
      final parent = directory.parent;
      if (parent.path == directory.path) return null;
      directory = parent;
    }
  }

  /// Start a server, and wait for it to say where it is listening.
  static Future<LiveServer> start() async {
    final binary = _binary;
    if (binary == null) {
      throw StateError('no tricore-server binary');
    }
    final directory = Directory.systemTemp.createTempSync('tricoredb-dart-');
    final configuration = File('${directory.path}/tricore.toml')
      ..writeAsStringSync(_configuration);
    final data = Directory('${directory.path}/data')..createSync();

    final process = await Process.start(binary, [
      '--config',
      configuration.path,
      '--port',
      '0',
      '--data-dir',
      data.path,
    ]);
    final address = Completer<String>();
    // Reading continues past the address: a full pipe would stop the server
    // dead.
    process.stdout
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .listen((line) {
      final marker = line.indexOf('listening on ');
      if (marker >= 0 && !address.isCompleted) {
        address
            .complete(line.substring(marker + 'listening on '.length).trim());
      }
    });
    unawaited(process.stderr.drain<void>());

    final listening = await address.future.timeout(
      const Duration(seconds: 60),
      onTimeout: () {
        process.kill();
        throw StateError('the server never said which address it listens on');
      },
    );
    final separator = listening.lastIndexOf(':');
    return LiveServer._(
      process,
      directory,
      listening.substring(0, separator),
      int.parse(listening.substring(separator + 1)),
    );
  }

  /// Connect to this server as `admin`.
  Future<TriCore> connect({Duration? readTimeout}) => TriCore.connect(
        host: host,
        port: port,
        user: 'admin',
        secret: 'pw',
        connectTimeout: const Duration(seconds: 15),
        readTimeout: readTimeout,
      );

  /// A pool of connections to this server.
  TriCorePool pool({int size = 4}) => TriCorePool.to(
        host: host,
        port: port,
        user: 'admin',
        secret: 'pw',
        size: size,
      );

  /// Stop the server and remove its data directory.
  Future<void> stop() async {
    _process.kill();
    await _process.exitCode.timeout(
      const Duration(seconds: 10),
      onTimeout: () => 0,
    );
    try {
      _directory.deleteSync(recursive: true);
    } on FileSystemException {
      // A file the server still holds is not worth failing a test run over.
    }
  }
}

/// A name no other test in this run uses, so tests can share one server.
String uniqueName(String prefix) {
  final stamp = DateTime.now().microsecondsSinceEpoch.toRadixString(36);
  return '${prefix}_$stamp';
}
