/// Builders for the server's document filters, externally tagged.
///
/// ```dart
/// Filter.allOf([Filter.eq('city', 'Pune'), Filter.gt('visits', 4)]);
/// ```
abstract final class Filter {
  /// Matches every document.
  static const Object all = 'All';

  static Map<String, Object?> _field(
          String variant, String field, Object? value) =>
      {
        variant: {'field': field, 'value': value},
      };

  /// `field` equals `value`.
  static Map<String, Object?> eq(String field, Object? value) =>
      _field('Eq', field, value);

  /// `field` differs from `value`.
  static Map<String, Object?> ne(String field, Object? value) =>
      _field('Ne', field, value);

  /// `field` is greater than `value`.
  static Map<String, Object?> gt(String field, Object? value) =>
      _field('Gt', field, value);

  /// `field` is greater than or equal to `value`.
  static Map<String, Object?> gte(String field, Object? value) =>
      _field('Gte', field, value);

  /// `field` is less than `value`.
  static Map<String, Object?> lt(String field, Object? value) =>
      _field('Lt', field, value);

  /// `field` is less than or equal to `value`.
  static Map<String, Object?> lte(String field, Object? value) =>
      _field('Lte', field, value);

  /// `field` contains `value` — a substring, or a member of an array.
  static Map<String, Object?> contains(String field, Object? value) =>
      _field('Contains', field, value);

  /// `field` equals any of `values`.
  static Map<String, Object?> inList(String field, List<Object?> values) => {
        'In': {'field': field, 'values': values},
      };

  /// Every sub-filter must match.
  static Map<String, Object?> allOf(List<Object?> filters) => {'And': filters};
}

/// Builders for aggregation accumulators.
abstract final class Acc {
  static Map<String, Object?> _op(String output, Object op) =>
      {'output': output, 'op': op};

  /// Sum of a numeric field.
  static Map<String, Object?> sum(String output, String field) =>
      _op(output, {'Sum': field});

  /// Mean of a numeric field.
  static Map<String, Object?> avg(String output, String field) =>
      _op(output, {'Avg': field});

  /// Smallest value of a field.
  static Map<String, Object?> min(String output, String field) =>
      _op(output, {'Min': field});

  /// Largest value of a field.
  static Map<String, Object?> max(String output, String field) =>
      _op(output, {'Max': field});

  /// How many documents are in the group.
  static Map<String, Object?> count(String output) => _op(output, 'Count');
}

/// Builders for aggregation stages. Stages apply strictly in order.
abstract final class Stage {
  /// Keep the documents a filter matches.
  static Map<String, Object?> match(Object filter) => {'Match': filter};

  /// Group by a key, accumulating each group.
  static Map<String, Object?> group(
    Object by, [
    List<Map<String, Object?>> accumulators = const [],
  ]) =>
      {
        'Group': {'by': by, 'accumulators': accumulators},
      };

  /// Sort by fields, each ascending unless named in [descending].
  static Map<String, Object?> sort(
    List<String> fields, {
    List<String> descending = const [],
  }) =>
      {
        'Sort': [
          for (final field in fields)
            {'field': field, 'descending': descending.contains(field)},
        ],
      };

  /// Drop the first [n] documents.
  static Map<String, Object?> skip(int n) => {'Skip': n};

  /// Keep at most [n] documents.
  static Map<String, Object?> limit(int n) => {'Limit': n};

  /// Keep (or, with `include: false`, drop) the named fields.
  static Map<String, Object?> project(
    List<String> fields, {
    bool include = true,
  }) =>
      {
        'Project': {'fields': fields, 'include': include},
      };

  /// Collapse the stream to one document holding the count.
  static Map<String, Object?> count(String field) => {
        'Count': {'field': field},
      };

  /// Group by a document field.
  static Map<String, Object?> byField(String field) => {'Field': field};

  /// Group everything under one constant key.
  static Map<String, Object?> byConstant(Object? value) => {'Constant': value};
}

/// Builders for the read-only sources an LLM context bundle is assembled from.
abstract final class LlmSource {
  /// Rows a query returns.
  static Map<String, Object?> sql(String query) => {
        'Sql': {'query': query},
      };

  /// Documents a filter matches.
  static Map<String, Object?> documents(
    String collection, {
    Object filter = Filter.all,
    int? limit,
  }) =>
      {
        'DocumentFind': {
          'collection': collection,
          'filter': filter,
          'limit': limit,
        },
      };
}

/// Which way an edge points, seen from a node.
enum GraphDirection {
  /// Edges that leave the node.
  outgoing,

  /// Edges that arrive at it.
  incoming,

  /// Both.
  both;

  /// The name the server uses.
  String get wire => name;
}

/// How a vector collection measures closeness. Fixed when it is created.
enum VectorMetric {
  /// Cosine similarity: higher is closer.
  cosine,

  /// Dot product: higher is closer.
  dot,

  /// Squared Euclidean distance, **returned negated** so higher is still
  /// closer: `-0.02` is nearer than `-196.0`.
  l2;

  /// The name the server uses.
  String get wire => name;
}

/// How a vector collection stores its vectors.
enum VectorQuantization {
  /// Full precision.
  none,

  /// Eight-bit, for a smaller index at some loss of precision.
  int8;

  /// The name the server uses.
  String get wire => name;
}

/// How an LLM export is rendered.
enum LlmFormat {
  /// TOON: compact text meant for a model's context window.
  toon,

  /// JSON, for a program.
  json,

  /// Markdown, for a person.
  markdown,

  /// The server's own structure.
  native;

  /// The name the server uses.
  String get wire => name;
}
