import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:tricoredb/tricoredb.dart';

/// What the peer does when the next request arrives.
sealed class Step {
  const Step();
}

/// Answer with a frame carrying this JSON.
class Reply extends Step {
  final int tag;
  final Object? json;

  const Reply(this.tag, this.json);
}

/// Answer with these exact bytes, for the malformed cases a codec would refuse
/// to produce.
class Raw extends Step {
  final List<int> bytes;

  const Raw(this.bytes);
}

/// Close the socket without answering.
class HangUp extends Step {
  const HangUp();
}

/// Answer nothing at all, and hold the socket open.
class Silence extends Step {
  const Silence();
}

/// A header that declares more bytes than the peer will send.
Raw overlongHeader(int tag, int length) => Raw([
      Frame.version,
      tag,
      (length >> 24) & 0xff,
      (length >> 16) & 0xff,
      (length >> 8) & 0xff,
      length & 0xff,
    ]);

/// A peer that speaks the handshake and then plays a scripted answer.
///
/// A real server cannot be asked to answer `not_leader` on demand, to hang up
/// mid-frame, or to declare a payload it does not send. What is under test is
/// this client's reading of those answers, and the shape it reads is the one a
/// real cluster sends.
class ScriptedPeer {
  final ServerSocket _server;
  final List<Step> _steps;

  ScriptedPeer._(this._server, this._steps) {
    _server.listen(_serve);
  }

  /// The port this peer listens on.
  int get port => _server.port;

  /// A peer that grants [features] in the handshake and then runs [steps].
  static Future<ScriptedPeer> start({
    int features = 7,
    List<Step> steps = const [],
  }) {
    return startRaw([
      Reply(Frame.helloOk, {
        'ok': true,
        'server_version': {'major': 1, 'minor': 0},
        'message': 'ok',
        'features': features,
      }),
      Reply(Frame.authOk, const {'ok': true, 'session_id': 's-1'}),
      ...steps,
    ]);
  }

  /// A peer that runs [steps] from the very first frame, handshake included.
  static Future<ScriptedPeer> startRaw(List<Step> steps) async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    return ScriptedPeer._(server, steps);
  }

  /// Connect a client to this peer.
  Future<TriCore> connect({
    String? user = 'admin',
    int features = Features.all,
    Duration? readTimeout,
  }) {
    return TriCore.connect(
      host: '127.0.0.1',
      port: port,
      user: user,
      secret: 'pw',
      features: features,
      connectTimeout: const Duration(seconds: 5),
      readTimeout: readTimeout,
    );
  }

  /// Stop listening and drop any open socket.
  Future<void> shutdown() => _server.close();

  void _serve(Socket socket) {
    var index = 0;
    final queue = Queue<int>();
    var pending = <int>[];

    void play(Step step) {
      switch (step) {
        case Reply(:final tag, :final json):
          socket.add(Frame.encode(tag, json));
        case Raw(:final bytes):
          socket.add(bytes);
        case HangUp():
          socket.destroy();
        case Silence():
          break;
      }
    }

    void onFrame() {
      if (index >= _steps.length) return;
      play(_steps[index]);
      index += 1;
      // A hang-up is not an answer to a request of its own: it follows the step
      // before it, which is what "the peer went away mid-frame" means.
      while (index < _steps.length && _steps[index] is HangUp) {
        play(_steps[index]);
        index += 1;
      }
    }

    socket.listen(
      (data) {
        queue.addAll(data);
        while (true) {
          if (pending.isEmpty) {
            if (queue.length < Frame.headerSize) return;
            final header = <int>[
              for (var i = 0; i < Frame.headerSize; i++) queue.removeFirst(),
            ];
            pending = header;
          }
          final length = (pending[2] << 24) |
              (pending[3] << 16) |
              (pending[4] << 8) |
              pending[5];
          if (queue.length < length) return;
          for (var i = 0; i < length; i++) {
            queue.removeFirst();
          }
          pending = <int>[];
          onFrame();
        }
      },
      onError: (Object _) => socket.destroy(),
      onDone: () => socket.destroy(),
      cancelOnError: true,
    );
  }
}

/// A RESPONSE frame carrying a server payload.
Reply response(Map<String, Object?> json) => Reply(Frame.response, json);

/// The bytes of a frame, for a test that sends half of one.
Uint8List frameBytes(int tag, Object? json) => Frame.encode(tag, json);

/// A name no other test in this run uses.
String unique(String prefix) =>
    '${prefix}_${DateTime.now().microsecondsSinceEpoch.toRadixString(36)}';

/// The UTF-8 bytes of [text], for the cache calls that take bytes.
List<int> bytesOf(String text) => utf8.encode(text);
