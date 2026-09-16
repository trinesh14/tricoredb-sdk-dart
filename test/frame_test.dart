import 'dart:convert';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:tricoredb/tricoredb.dart';

void main() {
  group('Frame', () {
    test('a frame is a six-byte header and its payload', () {
      final bytes = Frame.encode(Frame.request, {'a': 1});
      final body = utf8.encode('{"a":1}');

      expect(bytes[0], Frame.version);
      expect(bytes[1], Frame.request);
      expect(bytes.sublist(2, 6), [0, 0, 0, body.length]);
      expect(bytes.sublist(6), body);
    });

    test('an empty payload is a header on its own', () {
      final bytes = Frame.encode(Frame.ping, null);

      expect(bytes.length, Frame.headerSize);
      expect(bytes.sublist(2, 6), [0, 0, 0, 0]);
    });

    test('a header round trips', () {
      final frame = Frame.encode(Frame.response, {'a': 1});
      final header = Frame.decodeHeader(frame.sublist(0, Frame.headerSize));

      expect(header.tag, Frame.response);
      expect(header.length, 7);
    });

    test('a payload length above 16 MiB uses all four length bytes', () {
      // The length is big-endian, and the data ceiling needs three of the four
      // bytes; a client that read it the other way round would be wrong here.
      final header =
          Uint8List.fromList([Frame.version, Frame.response, 0, 1, 0, 0]);

      expect(Frame.decodeHeader(header).length, 65536);
    });

    test('a control frame above 64 KiB is refused before the payload is read',
        () {
      final header = Uint8List.fromList(
        [Frame.version, Frame.authOk, 0, 1, 0, 1],
      );

      expect(
        () => Frame.decodeHeader(header),
        throwsA(
          isA<ProtocolException>()
              .having((e) => e.code, 'code', 'frame_too_large'),
        ),
      );
    });

    test('a data frame above 16 MiB is refused too', () {
      final header = Uint8List.fromList(
        [Frame.version, Frame.response, 1, 0, 0, 1],
      );

      expect(
        () => Frame.decodeHeader(header),
        throwsA(isA<ProtocolException>()),
      );
    });

    test('a newer frame version is refused rather than guessed at', () {
      final header = Uint8List.fromList([2, Frame.response, 0, 0, 0, 0]);

      expect(
        () => Frame.decodeHeader(header),
        throwsA(
          isA<ProtocolException>()
              .having((e) => e.code, 'code', 'frame_version'),
        ),
      );
    });

    test('a short header is refused', () {
      expect(
        () => Frame.decodeHeader(Uint8List.fromList([1, 2, 3])),
        throwsA(isA<ProtocolException>()),
      );
    });

    test('this client will not send more than the protocol allows', () {
      final oversized = 'x' * (Frame.maxControlPayload + 16);

      expect(
        () => Frame.encode(Frame.auth, {'secret': oversized}),
        throwsA(
          isA<ProtocolException>()
              .having((e) => e.code, 'code', 'frame_too_large'),
        ),
      );
    });

    test('an empty body decodes to null, not to an error', () {
      expect(Frame.decodeBody(const []), isNull);
    });

    test('a payload that is not JSON is a protocol failure', () {
      expect(
        () => Frame.decodeBody(utf8.encode('{not json')),
        throwsA(isA<ProtocolException>()),
      );
    });
  });
}
