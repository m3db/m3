# Compatibility

This document describes versioning compatibility guarantees. Any compatibility
outlined here will be maintained for at least the current major version.
Additional guarantees may be added within the same major version, but existing
guarantees may only be modified or removed as part of a major version change.

If you discover a breaking change, please [open an issue][issue].

[issue]: https://github.com/m3db/m3/issues/new

## v1.0

As of version 1.0, m3db/m3 and its components are guaranteed to have **binary**,
**configuration**, **wire**, and **transport API** compatibility:

| Compatibility | Scope | Guarantee |
| :------------ | :---- | :-------- |
| binary | All released executable components. | All executable components will maintain their current functionality. Components may have *additional* functionality added, but no existing functionality will be changed or removed. |
| configuration | All configuration serialization for binary components. | All configuration will maintain serialization compatibility. New configuration properties may be added, but existing properties will not be renamed, removed, re-typed, or changed semantically or behaviorally. |
| wire | All on-the-wire encoding for data to/from binary components. | All wire encoding (e.g. Protobuf, Thrift) will maintain wire compatibility. Any changes to IDLs or other encodings will be backwards-compatible. |
| transport API | All exposed transport APIs for communication to/from binary components. | All transport APIs (e.g. m3msg, Thrift, HTTP+JSON) will maintain their current surface area, definitions, semantics, and behavior. Any changes to APIs will be additive or done semantically and with wire compatibility. |

This version does **not** guarantee library-level compatibility. Users are not
encouraged to integrate libraries from m3db/m3 directly: use at your own risk.
