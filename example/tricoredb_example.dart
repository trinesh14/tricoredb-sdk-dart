// A tour of the package against a running server.
//
//   docker run --rm -p 8427:8427 trinesh14/tricoredb:0.1.0-rc.1-r2
//   dart run example/tricoredb_example.dart
import 'dart:io';

import 'package:tricoredb/tricoredb.dart';

Future<void> main() async {
  final db = await TriCore.connect(
    host: Platform.environment['TRICORE_HOST'] ?? '127.0.0.1',
    port: int.parse(Platform.environment['TRICORE_PORT'] ?? '8427'),
    user: Platform.environment['TRICORE_USER'] ?? 'admin',
    secret: Platform.environment['TRICORE_SECRET'] ?? 'pw',
  );
  print('connected: session ${db.sessionId}, granted ${db.grantedFeatures}');

  // SQL, with the values bound by the server rather than pasted into the text.
  await db.execute(
    'CREATE TABLE IF NOT EXISTS people '
    '(id INT PRIMARY KEY, name TEXT, score DOUBLE)',
  );
  await db.execute('INSERT INTO people VALUES (?, ?, ?)', [1, 'ada', 9.5]);

  final rows = await db.query(
    'SELECT name, score FROM people WHERE id = ?',
    [1],
  );
  print('row: ${rows.toMaps().first}');

  // An injection attempt is a value, so it matches nothing and drops nothing.
  final injected = await db.query(
    'SELECT id FROM people WHERE name = ?',
    ["ada'; DROP TABLE people; --"],
  );
  print('injection found ${injected.length} rows, and the table is still here');

  // A transaction: either both writes land, or neither does.
  await db.transaction((tx) async {
    await tx.execute('INSERT INTO people VALUES (?, ?, ?)', [2, 'grace', 8.25]);
    await tx.execute('INSERT INTO people VALUES (?, ?, ?)', [3, 'alan', 9.0]);
  });
  print(
      'after commit: ${(await db.query('SELECT id FROM people')).length} rows');

  // Cache, documents, vectors and graphs live on the same connection.
  await db.cacheSet('demo', 'greeting', 'hello',
      ttl: const Duration(minutes: 1));
  print('cached: ${await db.cacheGetString('demo', 'greeting')}');

  await db.documentCreateCollection('notes');
  await db.documentInsert('notes', {
    'title': 'first',
    'tags': ['a', 'b']
  });
  print('documents: ${await db.documentFind('notes')}');

  await db.vectorCreateCollection('embeddings', dimension: 3);
  await db.vectorUpsert('embeddings', 'a', [1, 0, 0]);
  print('nearest: ${await db.vectorSearch('embeddings', [1, 0, 0], topK: 1)}');

  // A refusal is typed, and leaves the connection usable.
  try {
    await db.query('SELECT * FROM no_such_table');
  } on ServerException catch (e) {
    print('refused with code ${e.code}; still open: ${!db.isClosed}');
  }

  await db.close();
}
