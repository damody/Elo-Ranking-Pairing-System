# ERPS protocol compatibility

`ApiVersion.major` changes only for incompatible wire or behavior changes. Minor versions add optional fields or capabilities. Published protobuf field numbers are never reused, even after a field is removed; removed numbers must be declared `reserved` in `erps.proto`.

The session RPC is named `OpenSession` because tonic reserves the generated client method name `connect` for transport construction. Rust and C SDKs may expose their own `connect` convenience method.
