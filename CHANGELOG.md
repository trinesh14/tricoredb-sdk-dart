# Changelog

All notable changes to this package are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.1.0

First release.

### Added

- `TriCore`: the native `tricore` wire protocol over TCP or TLS, with handshake
  feature negotiation and password authentication. Every call is `async`, and
  requests on one connection are queued, so two callers can never interleave
  frames on the same socket.
- SQL: `query` and `execute` with `?` placeholders bound **by the server**,
  plus one-request scripts and session transactions (`begin`, `commit`,
  `rollback`, `transaction`).
- `TriCorePool`: a bounded pool that lends a connection to a callback and never
  returns one with a transaction still open.
- Cache (keys, lists, sets, hashes, streams), documents with filters and
  aggregation pipelines, vectors, graphs, LLM context export and the admin
  reads.
- `TriCoreException` and its subclasses, each carrying the server's own `code`
  and, for a redirect, a leader hint.
- TLS and mutual TLS through `TlsOptions`.
- No dependencies: `dart:io` and `dart:convert` are the whole runtime.

### Security

- A call that needs a capability the server did not grant — server-side
  parameters, session transactions — fails before anything is sent, instead of
  falling back to a weaker behaviour.
- A frame's declared length is checked against the protocol's ceiling before a
  payload byte is read, so a wrong or hostile peer cannot make this client
  allocate what it claimed.
- A connection that timed out or lost frame alignment is dropped rather than
  reused: a late reply can never be read as the answer to the next request.
- Errors name a certificate or key file's path, never its contents.
