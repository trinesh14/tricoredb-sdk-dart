/// Dart driver for TriCoreDB's native `tricore` protocol.
///
/// One connection speaks SQL, documents, vectors, graphs, cache and LLM
/// context. Values are bound by the server, never rendered into statement
/// text, and everything is `async` — nothing blocks the isolate.
///
/// ```dart
/// import 'package:tricoredb/tricoredb.dart';
///
/// final db = await TriCore.connect(
///   host: '127.0.0.1',
///   user: 'admin',
///   secret: 'secret',
/// );
/// final rows = await db.query('SELECT name FROM people WHERE id = ?', [1]);
/// print(rows[0][0]);
/// await db.close();
/// ```
library;

export 'src/builders.dart';
export 'src/client.dart';
export 'src/errors.dart';
export 'src/frame.dart' show Frame, FrameHeader;
export 'src/options.dart';
export 'src/params.dart' show Blob, Decimal, Params;
export 'src/pool.dart';
export 'src/response.dart';
export 'src/version.dart';
