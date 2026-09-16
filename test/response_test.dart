import 'package:test/test.dart';
import 'package:tricoredb/tricoredb.dart';

void main() {
  group('Response', () {
    test('an ok response carries its data and its diagnostics', () {
      final response = Response.fromJson({
        'request_id': 'r1',
        'status': 'ok',
        'data': {
          'Json': {'rows_affected': 3},
        },
        'diagnostics': {
          'route': 'local',
          'elapsed_ms': 4,
          'warnings': ['shard 2 was unreachable'],
        },
      });

      expect(response.isOk, isTrue);
      expect(response.requestId, 'r1');
      expect(response.rowsAffected, 3);
      expect(response.route, 'local');
      expect(response.elapsedMilliseconds, 4);
      expect(response.warnings, ['shard 2 was unreachable']);
    });

    test('a warning on a successful response is still worth reading', () {
      // A partially applied broadcast reports here while the status stays ok.
      final response = Response.fromJson({
        'status': 'ok',
        'data': 'Empty',
        'diagnostics': {
          'warnings': ['node 3 did not apply the change'],
        },
      });

      expect(response.isOk, isTrue);
      expect(response.warnings, hasLength(1));
    });

    test('an arm holding null is present, and that is what a miss is', () {
      final response = Response.fromJson({
        'status': 'ok',
        'data': {'CacheValue': null},
      });

      expect(response.hasArm('CacheValue'), isTrue);
      expect(response.arm('CacheValue'), isNull);
      expect(response.hasArm('Json'), isFalse);
    });

    test('not_leader is typed and carries the leader address', () {
      final response = Response.fromJson({
        'status': 'error',
        'data': {'Message': 'not the raft leader'},
        'diagnostics': {
          'error_code': 'not_leader',
          'leader_hint': '10.9.9.7:8427',
        },
      });

      expect(response.isRedirect, isTrue);
      expect(response.leaderHint, '10.9.9.7:8427');

      final error = response.toException();
      expect(error.code, 'not_leader');
      expect(error.leaderHint, '10.9.9.7:8427');
      expect(error.message, contains('10.9.9.7:8427'));
    });

    test('mid-election there is a code but no address', () {
      final response = Response.fromJson({
        'status': 'error',
        'data': {'Message': 'not the raft leader'},
        'diagnostics': {'error_code': 'not_leader'},
      });

      expect(response.isRedirect, isTrue);
      expect(response.leaderHint, isNull);
      expect(response.toException().message, contains('election'));
    });

    test('an open transaction is reported as over on a redirect', () {
      final response = Response.fromJson({
        'status': 'error',
        'data': {'Message': 'not the raft leader'},
        'diagnostics': {'error_code': 'not_leader'},
      });

      expect(
        response.toException(transactionOpen: true).message,
        contains('cannot continue on another node'),
      );
    });

    test('a status this client does not know is a failure', () {
      final response = Response.fromJson({
        'status': 'not_implemented',
        'data': {'Message': 'Cache::XGroup is refused in V1'},
      });

      expect(response.isOk, isFalse);
      expect(response.toException().status, 'not_implemented');
      expect(response.toException().message, contains('XGroup'));
    });

    test('a payload that is not a response at all is an error, not a crash',
        () {
      final response = Response.fromJson('surprise');

      expect(response.isOk, isFalse);
      expect(response.status, 'error');
      expect(response.toException().message, contains('request failed'));
    });

    test('rows are addressable by position and by column name', () {
      const rows = Rows(
        ['id', 'name'],
        [
          ['1', 'ada'],
          ['2', null],
        ],
      );

      expect(rows.length, 2);
      expect(rows[0][1], 'ada');
      expect(rows.value(1, 'name'), isNull, reason: 'a NULL cell is null');
      expect(rows.value(0, 'name'), 'ada');
      expect(rows.value(0, 'nope'), isNull);
      expect(rows.toMaps().first, {'id': '1', 'name': 'ada'});
      expect(rows.map((row) => row[0]).toList(), ['1', '2']);
    });
  });
}
