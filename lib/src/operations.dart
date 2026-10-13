import 'dart:convert';
import 'dart:typed_data';

/// The bytes a cache value travels as: a [String]'s UTF-8 bytes, or the
/// [List<int>] itself.
List<int> cacheBytes(Object value, String what) {
  if (value is String) return utf8.encode(value);
  if (value is List<int>) {
    for (final byte in value) {
      if (byte < 0 || byte > 255) {
        throw ArgumentError('$what as a List<int> must hold bytes 0..255');
      }
    }
    return value;
  }
  throw ArgumentError(
    '$what must be a String or a List<int>, got ${value.runtimeType}',
  );
}

/// When a conditional cache write is allowed to proceed.
enum SetCondition {
  /// Write unconditionally.
  always('Always'),

  /// Write only if the key is absent. An expired key counts as absent.
  ifAbsent('IfAbsent'),

  /// Write only if the key is already present and live.
  ifPresent('IfPresent');

  const SetCondition(this.wire);

  /// The name the server uses.
  final String wire;
}

/// The bitwise operation [TriCore.cacheBitOperation] applies across its
/// sources.
enum BitOperation {
  /// Bits set in every source.
  and('And'),

  /// Bits set in any source.
  or('Or'),

  /// Bits set in an odd number of sources.
  xor('Xor'),

  /// The inverse of one source. Takes exactly one key.
  not('Not');

  const BitOperation(this.wire);

  /// The name the server uses.
  final String wire;
}

/// Which end of a list an element leaves or joins.
enum ListEnd {
  /// The head.
  left('Left'),

  /// The tail.
  right('Right');

  const ListEnd(this.wire);

  /// The name the server uses.
  final String wire;
}

/// Whether a by-id document write hands back the document it touched, and
/// which version of it.
enum ReturnDocument {
  /// Return no document.
  none('None'),

  /// The document as it was before the write.
  before('Before'),

  /// The document as it is after the write.
  after('After');

  const ReturnDocument(this.wire);

  /// The name the server uses.
  final String wire;
}

/// Which connectivity [TriCore.graphConnectedComponents] groups nodes by.
enum ComponentKind {
  /// Connected ignoring edge direction.
  weak,

  /// Mutually reachable following edge direction.
  strong;

  /// The name the server uses.
  String get wire => name;
}

/// Which centrality [TriCore.graphCentrality] computes.
enum CentralityMeasure {
  /// The share of shortest paths between other nodes that pass through a node.
  betweenness,

  /// How near a node is to the nodes it can reach.
  closeness,

  /// A node is central when central nodes point at it.
  eigenvector;

  /// The name the server uses.
  String get wire => name;
}

/// How [TriCore.graphNodeSimilarity] scores two neighbour sets.
enum SimilarityMetric {
  /// Shared neighbours over all neighbours of either node.
  jaccard,

  /// Shared neighbours over the smaller neighbour set.
  overlap;

  /// The name the server uses.
  String get wire => name;
}

/// What a graph constraint enforces.
enum ConstraintKind {
  /// No two records share a value of the property.
  unique,

  /// Every record has the property, and not as null.
  required;

  /// The name the server uses.
  String get wire => name;
}

/// What a constraint's label names.
enum ConstraintTarget {
  /// Node labels.
  node,

  /// Relationship (edge) labels.
  edge;

  /// The name the server uses.
  String get wire => name;
}

/// How [TriCore.vectorSearchMulti] combines its queries' rankings.
enum FusionMethod {
  /// Reciprocal rank fusion.
  rrf,

  /// The best score any query gave the vector.
  max,

  /// The mean score over all queries.
  mean;

  /// The name the server uses.
  String get wire => name;
}

/// What a cache call does to a key's expiry.
class CacheExpiry {
  final Object _wire;

  const CacheExpiry._(this._wire);

  /// Leave the expiry as it is.
  static const CacheExpiry keep = CacheExpiry._('Keep');

  /// Remove the expiry, which is what a plain set does.
  static const CacheExpiry clear = CacheExpiry._('Clear');

  /// Expire this long from now.
  CacheExpiry.after(Duration ttl)
      : _wire = {
          'Set': {'ttl_ms': ttl.inMilliseconds},
        };

  /// The value the server reads.
  Object toJson() => _wire;
}

/// One end of a sorted-set score range.
class ScoreBound {
  /// The score at this end.
  final double value;

  /// Whether a member with exactly this score is left out.
  final bool exclusive;

  /// A bound that includes members scoring exactly [value].
  const ScoreBound.inclusive(this.value) : exclusive = false;

  /// A bound that leaves out members scoring exactly [value].
  const ScoreBound.exclusive(this.value) : exclusive = true;

  /// No lower limit.
  static const ScoreBound lowest =
      ScoreBound.inclusive(double.negativeInfinity);

  /// No upper limit.
  static const ScoreBound highest = ScoreBound.inclusive(double.infinity);

  /// The value the server reads. JSON has no infinity, so an unbounded end is
  /// sent as the largest finite double, which no finite score lies beyond.
  Map<String, Object?> toJson() {
    if (value.isNaN) {
      throw ArgumentError('a score bound must not be NaN');
    }
    final finite = value.isInfinite
        ? (value.isNegative ? -double.maxFinite : double.maxFinite)
        : value;
    return {'value': finite, 'exclusive': exclusive};
  }
}

/// One member of a sorted set, as a range returns it.
class ScoredMember {
  /// The member's bytes.
  final Uint8List member;

  /// Its score. Null when the range was read without scores.
  final double? score;

  /// A member with these bytes and this score.
  const ScoredMember(this.member, this.score);

  /// The member decoded as UTF-8.
  String get text => utf8.decode(member, allowMalformed: true);

  @override
  String toString() => 'ScoredMember($text, $score)';
}

/// What [TriCore.cacheGetFresh] found.
class FreshRead {
  /// `fresh`, `stale`, `missing` (a cached absence) or `miss`.
  final String state;

  /// The value, when the state is `fresh` or `stale`.
  final Uint8List? value;

  /// On a stale read: whether this caller holds the refresh lease and should
  /// recompute the value.
  final bool refresh;

  /// On a miss: whether this caller holds the lease and should compute the
  /// value.
  final bool lease;

  /// On a miss without the lease: how long until asking again is worthwhile.
  final Duration? retryAfter;

  /// A read with this outcome.
  const FreshRead({
    required this.state,
    this.value,
    this.refresh = false,
    this.lease = false,
    this.retryAfter,
  });

  /// Read the server's answer. A value that is valid UTF-8 arrives as text
  /// under `value`; any other arrives as hex under `valueHex`.
  factory FreshRead.fromJson(Map<String, dynamic> json) {
    Uint8List? value;
    final text = json['value'];
    final hex = json['valueHex'];
    if (text is String) {
      value = Uint8List.fromList(utf8.encode(text));
    } else if (hex is String) {
      value = Uint8List.fromList([
        for (var i = 0; i + 1 < hex.length; i += 2)
          int.parse(hex.substring(i, i + 2), radix: 16),
      ]);
    }
    final retry = json['retryAfterMs'];
    return FreshRead(
      state: '${json['state']}',
      value: value,
      refresh: json['refresh'] == true,
      lease: json['lease'] == true,
      retryAfter: retry is int ? Duration(milliseconds: retry) : null,
    );
  }

  /// The value decoded as UTF-8, or null when there is none.
  String? get text =>
      value == null ? null : utf8.decode(value!, allowMalformed: true);

  @override
  String toString() => 'FreshRead($state)';
}

/// The outcome of one operation of a cache pipeline, transaction or script.
class CacheOpResult {
  /// Why the operation failed, or null when it succeeded.
  final String? error;

  /// The operation's response data, externally tagged as a response's is
  /// (`{"Json": …}`, `{"CacheValue": …}`, `{"Message": …}`). Null on failure.
  final Object? data;

  /// A result with this error or this data.
  const CacheOpResult({this.error, this.data});

  /// Read one entry of the server's `results`.
  factory CacheOpResult.fromJson(Object? raw) {
    if (raw is Map && raw.containsKey('error')) {
      return CacheOpResult(error: '${raw['error']}');
    }
    return CacheOpResult(data: raw is Map ? raw['ok'] : raw);
  }

  /// Whether the operation succeeded.
  bool get isOk => error == null;

  /// The `Json` arm of the data, or null when it carries another arm.
  Map<String, dynamic>? get json {
    final arm = data is Map ? (data as Map)['Json'] : null;
    return arm is Map ? Map<String, dynamic>.from(arm) : null;
  }

  /// The `CacheValue` arm of the data as bytes, or null on a miss or when it
  /// carries another arm.
  Uint8List? get value {
    final arm = data is Map ? (data as Map)['CacheValue'] : null;
    return arm is List
        ? Uint8List.fromList([for (final byte in arm) byte as int])
        : null;
  }

  @override
  String toString() =>
      isOk ? 'CacheOpResult(ok)' : 'CacheOpResult(error: $error)';
}

/// Builders for the cache operations a pipeline, a transaction or a script
/// runs. Each is the operation exactly as it would be sent alone.
///
/// ```dart
/// await db.cachePipeline([
///   CacheOp.set('sessions', 'abc', 'ada'),
///   CacheOp.increment('counters', 'logins'),
///   CacheOp.get('sessions', 'abc'),
/// ]);
/// ```
abstract final class CacheOp {
  /// A liveness check.
  static const Object ping = 'Ping';

  /// Any cache operation by its server name, with its fields as the server
  /// names them. Byte fields are arrays of byte values.
  static Map<String, Object?> raw(String variant, Map<String, Object?> body) =>
      {variant: body};

  /// Store a value, expiring after [ttl] when one is given.
  static Map<String, Object?> set(
    String namespace,
    String key,
    Object value, {
    Duration? ttl,
  }) =>
      raw('Set', {
        'namespace': namespace,
        'key': key,
        'value': cacheBytes(value, 'value'),
        'ttl_ms': ttl?.inMilliseconds,
      });

  /// Read a value.
  static Map<String, Object?> get(String namespace, String key) =>
      raw('Get', {'namespace': namespace, 'key': key});

  /// Delete a key.
  static Map<String, Object?> delete(String namespace, String key) =>
      raw('Delete', {'namespace': namespace, 'key': key});

  /// Whether a key is present.
  static Map<String, Object?> exists(String namespace, String key) =>
      raw('Exists', {'namespace': namespace, 'key': key});

  /// Add to a counter.
  static Map<String, Object?> increment(
    String namespace,
    String key, {
    int by = 1,
  }) =>
      raw('Incr', {'namespace': namespace, 'key': key, 'by': by});

  /// Give a key a new expiry.
  static Map<String, Object?> expire(
    String namespace,
    String key,
    Duration ttl,
  ) =>
      raw('Expire', {
        'namespace': namespace,
        'key': key,
        'ttl_ms': ttl.inMilliseconds,
      });
}

/// Builders for the operations of [TriCore.documentBulkWrite].
abstract final class BulkOp {
  static Map<String, Object?> _update(
    Map<String, Object?>? set,
    Map<String, Object?>? increment,
  ) =>
      {
        if (set != null && set.isNotEmpty) 'set': set,
        if (increment != null && increment.isNotEmpty) 'inc': increment,
      };

  /// Insert a document, under [id] when one is given.
  static Map<String, Object?> insert(
    Map<String, Object?> document, {
    String? id,
  }) =>
      {
        'Insert': {'id': id, 'document': document},
      };

  /// Set or increment fields on one document, optionally inserting it when it
  /// is absent. [expectedVersion] applies it only if the document's
  /// `_version` is still that.
  static Map<String, Object?> updateOne(
    String id, {
    Map<String, Object?>? set,
    Map<String, Object?>? increment,
    bool upsert = false,
    int? expectedVersion,
  }) =>
      {
        'UpdateOne': {
          'id': id,
          'update': _update(set, increment),
          'upsert': upsert,
          'expected_version': expectedVersion,
        },
      };

  /// Swap a document's whole body for [document].
  static Map<String, Object?> replace(
    String id,
    Map<String, Object?> document, {
    bool upsert = false,
    int? expectedVersion,
  }) =>
      {
        'Replace': {
          'id': id,
          'document': document,
          'upsert': upsert,
          'expected_version': expectedVersion,
        },
      };

  /// Delete one document by id.
  static Map<String, Object?> delete(String id, {int? expectedVersion}) => {
        'Delete': {'id': id, 'expected_version': expectedVersion},
      };
}

/// How a vector search's results are shaped and how hard it looks, beyond the
/// query itself. Only the fields given are sent; the server's defaults apply to
/// the rest.
class VectorSearchOptions {
  /// Skip this many ranked hits before returning `topK`.
  final int? offset;

  /// Whether each hit carries its metadata. The server's default is true.
  final bool? includeMetadata;

  /// Only hits within this distance of the query, in the metric's own units.
  final double? maxDistance;

  /// The HNSW beam width for this query.
  final int? efSearch;

  /// Score every stored vector instead of walking the index.
  final bool? exact;

  /// Rank by this metric (`cosine`, `dot`, `l2`, `l1` or `linf`) instead of
  /// the collection's own.
  final String? scoreMetric;

  /// How a metadata filter meets the index: `auto`, `graph` or `exact`.
  final String? filterStrategy;

  /// Search this named vector of the collection rather than its default one.
  final String? using;

  /// Confine the search to one tenant's vectors.
  final String? tenant;

  /// Confine the search to one namespace of the collection.
  final String? namespace;

  /// A sparse query, as `{indices, values}`.
  final Map<String, Object?>? sparse;

  /// The dense score's weight in a hybrid search, from 0 to 1.
  final double? hybridAlpha;

  /// Give each hit a one-line plain-language `reason`.
  final bool? explain;

  /// Rank by an expression over `score` and numeric metadata fields.
  final String? scoreExpr;

  /// The query's text, for `explain` to name the words a hit shares with it.
  final String? explainText;

  /// The approximate index to search through: `hnsw`, `ivf` or `ivf_pq`.
  final String? ann;

  /// IVF lists probed per search.
  final int? nprobe;

  /// Options with these fields set.
  const VectorSearchOptions({
    this.offset,
    this.includeMetadata,
    this.maxDistance,
    this.efSearch,
    this.exact,
    this.scoreMetric,
    this.filterStrategy,
    this.using,
    this.tenant,
    this.namespace,
    this.sparse,
    this.hybridAlpha,
    this.explain,
    this.scoreExpr,
    this.explainText,
    this.ann,
    this.nprobe,
  });

  /// The fields that were given, as the server names them.
  Map<String, Object?> toJson() => {
        if (offset != null) 'offset': offset,
        if (includeMetadata != null) 'include_metadata': includeMetadata,
        if (maxDistance != null) 'max_distance': maxDistance,
        if (efSearch != null) 'ef_search': efSearch,
        if (exact != null) 'exact': exact,
        if (scoreMetric != null) 'score_metric': scoreMetric,
        if (filterStrategy != null) 'filter_strategy': filterStrategy,
        if (using != null) 'using': using,
        if (tenant != null) 'tenant': tenant,
        if (namespace != null) 'namespace': namespace,
        if (sparse != null) 'sparse': sparse,
        if (hybridAlpha != null) 'hybrid_alpha': hybridAlpha,
        if (explain != null) 'explain': explain,
        if (scoreExpr != null) 'score_expr': scoreExpr,
        if (explainText != null) 'explain_text': explainText,
        if (ann != null) 'ann': ann,
        if (nprobe != null) 'nprobe': nprobe,
      };
}
