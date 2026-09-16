import 'dart:typed_data';

import 'errors.dart';

/// Bytes bound to a BLOB column.
///
/// A `String` is always text; wrap bytes in this to bind them as binary.
///
/// ```dart
/// await db.execute('INSERT INTO files VALUES (?, ?)', [1, Blob(pngBytes)]);
/// ```
class Blob {
  /// The bytes, as given.
  final Uint8List bytes;

  /// Bind these bytes as binary.
  Blob(List<int> bytes) : bytes = Uint8List.fromList(bytes);

  /// The `0x…` hex text a BLOB column parses.
  String toParam() {
    final buffer = StringBuffer('0x');
    for (final byte in bytes) {
      buffer.write(byte.toRadixString(16).padLeft(2, '0'));
    }
    return buffer.toString();
  }

  @override
  String toString() => 'Blob(${bytes.length} bytes)';
}

/// An exact decimal, carried as text so no binary float rounds it.
///
/// ```dart
/// await db.execute('INSERT INTO ledger VALUES (?)', [Decimal('10.25')]);
/// ```
class Decimal {
  /// The digits, exactly as they go on the wire.
  final String text;

  /// An exact decimal, written as plain digits.
  Decimal(String value) : text = value {
    if (!_plain.hasMatch(value)) {
      throw ArgumentError.value(
        value,
        'value',
        'a decimal is plain digits with an optional sign and point '
            '(no exponent, no spaces)',
      );
    }
  }

  static final RegExp _plain = RegExp(r'^[+-]?(\d+(\.\d*)?|\.\d+)$');

  @override
  String toString() => text;
}

/// How Dart values become server-side SQL parameters.
///
/// The wire carries JSON scalars only:
///
/// - `null`, `bool` and `int` as themselves
/// - `double` as a JSON number; NaN and the infinities are refused, because
///   they have no SQL value
/// - `String` as UTF-8 text — never as bytes
/// - [Blob] and `Uint8List` as `0x` + hex, for a BLOB column
/// - [Decimal] and `BigInt` as plain text, so nothing is rounded on the way
/// - `DateTime` as ISO-8601, in UTC when the value is UTC
///
/// Anything else is refused by name rather than pushed through `toString()`.
abstract final class Params {
  /// Encode a whole parameter list.
  static List<Object?> encodeAll(List<Object?> params) {
    final out = <Object?>[];
    for (var i = 0; i < params.length; i++) {
      out.add(encode(params[i], i + 1));
    }
    return out;
  }

  /// Encode one value. [index] is its 1-based position, for the message.
  static Object? encode(Object? value, [int index = 1]) {
    if (value == null || value is bool || value is int) return value;
    if (value is double) {
      if (value.isNaN || value.isInfinite) {
        throw ParameterException(
          index,
          '$value is not a finite number and has no SQL value',
        );
      }
      return value;
    }
    if (value is Blob) return value.toParam();
    if (value is Uint8List) return Blob(value).toParam();
    if (value is Decimal) return value.text;
    if (value is BigInt) return value.toString();
    if (value is String) return value;
    if (value is DateTime) {
      return value.toIso8601String();
    }
    throw ParameterException(
      index,
      'no SQL parameter form for ${value.runtimeType}. Convert it explicitly '
      '(String, int, double, BigInt, Decimal, DateTime, Blob, bool or null)',
    );
  }
}
