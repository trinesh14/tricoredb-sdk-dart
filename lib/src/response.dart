import 'dart:collection';
import 'dart:convert';
import 'dart:typed_data';

import 'errors.dart';

/// A SQL result set.
///
/// Every cell is the server's own text rendering of the value, so a number
/// arrives as its digits and a SQL `NULL` arrives as the text `NULL`. A cell is
/// null only when the server sent no value at all.
class Rows extends IterableBase<List<String?>> {
  /// The column names, in the order the server returned them.
  final List<String> columns;

  /// The rows themselves.
  final List<List<String?>> rows;

  /// A result set with these columns and rows.
  const Rows(this.columns, this.rows);

  @override
  Iterator<List<String?>> get iterator => rows.iterator;

  @override
  int get length => rows.length;

  /// The row at [index].
  List<String?> operator [](int index) => rows[index];

  /// One cell by column name, or null when the row or column is absent.
  String? value(int row, String column) {
    if (row < 0 || row >= rows.length) return null;
    final index = columns.indexOf(column);
    if (index < 0 || index >= rows[row].length) return null;
    return rows[row][index];
  }

  /// The rows keyed by column name, for callers that would rather not count
  /// positions.
  List<Map<String, String?>> toMaps() {
    return [
      for (final row in rows)
        {
          for (var i = 0; i < columns.length; i++)
            columns[i]: i < row.length ? row[i] : null,
        },
    ];
  }

  @override
  String toString() => 'Rows(${columns.join(', ')}; ${rows.length} rows)';
}

/// One entry of a cache stream.
class StreamEntry {
  /// The entry's `<ms>-<seq>` id.
  final String id;

  /// Its fields, in the order the server returned them.
  final List<MapEntry<Uint8List, Uint8List>> fields;

  /// An entry with this id and these fields.
  const StreamEntry(this.id, this.fields);

  /// The fields decoded as UTF-8. Wrong for binary payloads, where [fields]
  /// stays authoritative.
  Map<String, String> text() {
    return {
      for (final field in fields)
        utf8.decode(field.key, allowMalformed: true):
            utf8.decode(field.value, allowMalformed: true),
    };
  }

  @override
  String toString() => 'StreamEntry($id, ${fields.length} fields)';
}

/// A server RESPONSE: typed data, plus how it was produced.
class Response {
  /// The id this answers.
  final String requestId;

  /// `ok`, `error` or `not_implemented`.
  final String status;

  /// The externally tagged response data (`"Empty"`, `{"Json": …}`, …).
  final Object? data;

  /// Timings, routing, warnings and error codes.
  final Map<String, dynamic> diagnostics;

  Response._(this.requestId, this.status, this.data, this.diagnostics);

  /// Read a decoded RESPONSE payload.
  factory Response.fromJson(Object? raw) {
    final map = raw is Map ? raw : const <String, Object?>{};
    return Response._(
      '${map['request_id'] ?? ''}',
      map['status'] is String ? map['status'] as String : 'error',
      map['data'],
      map['diagnostics'] is Map
          ? Map<String, dynamic>.from(map['diagnostics'] as Map)
          : const {},
    );
  }

  /// Whether the server completed the request.
  bool get isOk => status == 'ok';

  /// Non-fatal warnings. A partially applied broadcast reports here while the
  /// status is still `ok`, so these are worth reading on success.
  List<String> get warnings {
    final list = diagnostics['warnings'];
    return list is List ? [for (final w in list) '$w'] : const [];
  }

  /// The machine-readable reason a request failed.
  String? get errorCode {
    final code = diagnostics['error_code'];
    return code is String && code.isNotEmpty ? code : null;
  }

  /// Where the leader is, when this node is not it.
  String? get leaderHint {
    final hint = diagnostics['leader_hint'];
    return hint is String && hint.isNotEmpty ? hint : null;
  }

  /// Which node answered.
  String? get route {
    final route = diagnostics['route'];
    return route is String ? route : null;
  }

  /// How long the server says it took.
  int? get elapsedMilliseconds {
    final elapsed = diagnostics['elapsed_ms'];
    return elapsed is int ? elapsed : null;
  }

  /// Whether the request was right but reached a follower.
  bool get isRedirect => errorCode == notLeaderCode;

  /// How many rows a SQL write changed, when the server reported it.
  int? get rowsAffected {
    final json = arm('Json');
    return json is Map && json['rows_affected'] is int
        ? json['rows_affected'] as int
        : null;
  }

  /// Whether the data carries the named arm (`Json`, `Rows`, …). A null value
  /// still counts: `{"CacheValue": null}` is a miss, not an absent arm.
  bool hasArm(String name) => data is Map && (data as Map).containsKey(name);

  /// The named arm's value, or null.
  Object? arm(String name) => data is Map ? (data as Map)[name] : null;

  /// What kind of data this carries, for messages.
  String get kind {
    if (data is Map) {
      final keys = (data as Map).keys;
      return keys.isEmpty ? '{}' : '${keys.first}';
    }
    return '$data';
  }

  /// Build the exception for a response that is not `ok`.
  ServerException toException({bool transactionOpen = false}) {
    final message = arm('Message') ??
        (hasArm('Json') ? json.encode(arm('Json')) : null) ??
        'request failed';
    final buffer = StringBuffer('$message (server status: $status)');
    if (isRedirect) {
      buffer.write(leaderHint != null
          ? ' [not_leader: the leader serves clients at `$leaderHint`. '
              'This driver does not follow the hint on its own; send the '
              'request there.'
          : ' [not_leader: there is no leader address to name (an election is '
              'in progress, or the leader has no address configured). Wait and '
              'try again.');
      if (transactionOpen) {
        buffer.write(' The open session transaction is over: it cannot '
            'continue on another node.');
      }
      buffer.write(']');
    }
    return ServerException(
      buffer.toString(),
      code: errorCode,
      leaderHint: leaderHint,
      status: status,
    );
  }

  @override
  String toString() => 'Response($status, $kind)';
}
