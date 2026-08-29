# ERPS C client library

The public ABI is declared in `include/erps_client.h`. The Rust implementation is
the only gRPC implementation; C callers link the generated dynamic library.

## Windows x64

From the monorepo root with the pinned Rust toolchain and a C compiler installed:

```powershell
cargo build --manifest-path erps/Cargo.toml -p erps-client-ffi --release --target-dir erps/target/windows-x64
clang -std=c11 -Wall -Wextra -Werror -I erps/client-ffi/include `
  erps/client-ffi/tests/c_smoke/smoke.c `
  erps/target/windows-x64/release/erps_client_ffi.dll.lib `
  -o erps/target/windows-x64/release/erps-c-smoke.exe
$env:PATH = "$(Resolve-Path erps/target/windows-x64/release);$env:PATH"
erps/target/windows-x64/release/erps-c-smoke.exe
```

The command above is the fast ABI/link smoke. To run the complete network path,
start the deterministic fixture in a second terminal and compile `e2e.c`:

```powershell
# terminal 1
cargo run --manifest-path erps/Cargo.toml -p erps --bin erps-test-fixture

# terminal 2, after ERPS_FIXTURE_READY is printed
clang -std=c11 -Wall -Wextra -Werror -I erps/client-ffi/include `
  erps/client-ffi/tests/c_smoke/e2e.c `
  erps/target/windows-x64/release/erps_client_ffi.dll.lib `
  -o erps/target/windows-x64/release/erps-c-e2e.exe
$env:PATH = "$(Resolve-Path erps/target/windows-x64/release);$env:PATH"
erps/target/windows-x64/release/erps-c-e2e.exe http://127.0.0.1:50059
```

A successful full run prints `C_E2E_PASS`. It covers party invite/join/leave and
rename, concurrent commands, single-consumer poll enforcement, state
reconciliation, enqueue/accept, authoritative ready deadline, party member
rating/credit, ready match delivery, roster accessors, event release, and shutdown.

Distribute `erps_client_ffi.dll`, `erps_client_ffi.dll.lib`, and
`include/erps_client.h`. Do not distribute PDB or Cargo `target` contents as source
artifacts.

## Linux x86_64

```bash
cargo build --manifest-path erps/Cargo.toml -p erps-client-ffi --release \
  --target-dir erps/target/linux-x86_64
gcc -std=c11 -Wall -Wextra -Werror -I erps/client-ffi/include \
  erps/client-ffi/tests/c_smoke/smoke.c \
  -L erps/target/linux-x86_64/release -lerps_client_ffi \
  -Wl,-rpath,'$ORIGIN' -o erps/target/linux-x86_64/release/erps-c-smoke
erps/target/linux-x86_64/release/erps-c-smoke
```

For the complete Linux network path, start `erps-test-fixture` in another
terminal, then run:

```bash
gcc -std=c11 -Wall -Wextra -Werror -pthread -I erps/client-ffi/include \
  erps/client-ffi/tests/c_smoke/e2e.c \
  -L erps/target/linux-x86_64/release -lerps_client_ffi \
  -Wl,-rpath,'$ORIGIN' -o erps/target/linux-x86_64/release/erps-c-e2e
erps/target/linux-x86_64/release/erps-c-e2e http://127.0.0.1:50059
```

Distribute `liberps_client_ffi.so` and `include/erps_client.h`. The application
must either install the shared object in its loader path or ship it beside the
executable with an appropriate rpath.

Use `erps_client_create_tls` with certificates rooted in the platform trust store,
or `erps_client_create_tls_with_ca` for a private PEM CA. Plaintext creation is only
for loopback development servers explicitly configured to allow it.

Use the `ERPS_EVENT_*` constants with `erps_event_kind()`; applications must not
duplicate the numeric values. A proposal's absolute Unix-millisecond deadline is
available through `erps_event_deadline_ms()`. Party and state events expose the
display name, leader, lifecycle state, and member ID/rating/credit through the
`erps_event_party_*` accessors. All returned pointers remain owned by the event
and become invalid after `erps_event_release()`.
Use `erps_event_party_member_rating_for_mode()` with an `ERPS_MODE_*` constant
to display the correct 1v1, 5v5, or FFA8 Elo; the older rating accessor remains
the 1v1 value for ABI compatibility.

After reconnect, a state event exposes the player's profile, active credit
suspension, queue mode, allowed regions, pending proposal ID, and proposal
deadline. Read these with the `erps_event_player_*`,
`erps_event_credit_suspended_until_ms()`, `erps_event_queue_mode()`,
`erps_event_allowed_region_count()`, `erps_event_allowed_region()`, and
`erps_event_deadline_ms()` accessors. A rejected or expired ready check emits
`ERPS_EVENT_PROPOSAL_CANCELLED`; use `erps_event_reason()` and the player credit
accessors to stop the countdown and display the authoritative penalty.
