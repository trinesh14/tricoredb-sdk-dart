import 'dart:convert';
import 'dart:typed_data';

import 'errors.dart';

/// The native frame format: `version:u8 | tag:u8 | payload_len:u32be | payload`.
abstract final class Frame {
  /// The frame format version this driver writes.
  static const int version = 1;

  /// The highest frame format version this driver can read.
  static const int maxSupportedVersion = 1;

  /// Bytes before the payload.
  static const int headerSize = 6;

  /// Ceiling for REQUEST and RESPONSE payloads.
  static const int maxDataPayload = 16 * 1024 * 1024;

  /// Ceiling for every other frame.
  static const int maxControlPayload = 64 * 1024;

  /// The client's opening frame.
  static const int hello = 0;

  /// Credentials, sent after the handshake.
  static const int auth = 1;

  /// An operation for a module.
  static const int request = 2;

  /// The answer to a request.
  static const int response = 3;

  /// A liveness check on the socket alone.
  static const int ping = 4;

  /// The answer to a ping.
  static const int pong = 5;

  /// A refusal that is not a response.
  static const int error = 6;

  /// A polite goodbye.
  static const int close = 7;

  /// The server's answer to hello.
  static const int helloOk = 8;

  /// The server's verdict on the credentials.
  static const int authOk = 9;

  /// The server's goodbye.
  static const int bye = 10;

  /// A request to stop a running statement.
  static const int cancel = 11;

  /// The answer to a cancellation.
  static const int cancelOk = 12;

  static const Map<int, String> _names = {
    hello: 'HELLO',
    auth: 'AUTH',
    request: 'REQUEST',
    response: 'RESPONSE',
    ping: 'PING',
    pong: 'PONG',
    error: 'ERROR',
    close: 'CLOSE',
    helloOk: 'HELLO_OK',
    authOk: 'AUTH_OK',
    bye: 'BYE',
    cancel: 'CANCEL',
    cancelOk: 'CANCEL_OK',
  };

  /// The frame's name, for messages.
  static String name(int tag) => _names[tag] ?? 'tag $tag';

  /// The payload ceiling for a tag. An unknown tag gets the tighter one.
  static int maxPayloadFor(int tag) =>
      tag == request || tag == response ? maxDataPayload : maxControlPayload;

  /// Encode one frame. A null [payload] sends an empty one.
  static Uint8List encode(int tag, Object? payload) {
    final body =
        payload == null ? const <int>[] : utf8.encode(json.encode(payload));
    final limit = maxPayloadFor(tag);
    if (body.length > limit) {
      throw ProtocolException(
        'refusing to send a ${body.length}-byte ${name(tag)} payload; '
        'the protocol caps it at $limit bytes',
        code: 'frame_too_large',
      );
    }
    final out = Uint8List(headerSize + body.length);
    out[0] = version;
    out[1] = tag;
    final view = ByteData.sublistView(out);
    view.setUint32(2, body.length);
    out.setRange(headerSize, out.length, body);
    return out;
  }

  /// Read and validate a six-byte header **before** a payload byte is read.
  ///
  /// A frame that declares more than its ceiling is refused here, so a wrong or
  /// hostile peer cannot make this client allocate what it claimed.
  static FrameHeader decodeHeader(List<int> header) {
    if (header.length != headerSize) {
      throw ProtocolException('short frame header (${header.length} bytes)');
    }
    final frameVersion = header[0];
    final tag = header[1];
    if (frameVersion > maxSupportedVersion) {
      throw ProtocolException(
        'frame header version $frameVersion is newer than this driver can '
        'read (max $maxSupportedVersion)',
        code: 'frame_version',
      );
    }
    final length =
        (header[2] << 24) | (header[3] << 16) | (header[4] << 8) | header[5];
    final limit = maxPayloadFor(tag);
    if (length > limit) {
      throw ProtocolException(
        '${name(tag)} frame declares a $length-byte payload, above the '
        '$limit-byte limit; refusing to buffer it',
        code: 'frame_too_large',
      );
    }
    return FrameHeader(tag, length);
  }

  /// Parse a payload. An empty payload is null, not an error.
  static Object? decodeBody(List<int> body) {
    if (body.isEmpty) return null;
    try {
      return json.decode(utf8.decode(body));
    } on FormatException catch (e) {
      throw ProtocolException('frame payload is not valid JSON: ${e.message}');
    }
  }
}

/// A decoded frame header: what kind of frame, and how many payload bytes.
class FrameHeader {
  /// The frame's tag.
  final int tag;

  /// How many payload bytes follow the header.
  final int length;

  /// A header naming this tag and payload length.
  const FrameHeader(this.tag, this.length);
}
