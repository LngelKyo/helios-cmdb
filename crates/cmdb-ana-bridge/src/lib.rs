//! ana (NATS A2A) bridge.
//!
//! Connects helios-cmdb to the ana bus as agent identity `cmdb`. Does three
//! things:
//!   1. Subscribes to `<prefix>.>.discovery` / `<prefix>.>.pulse` /
//!      `<prefix>.>.alert` and auto-builds `fleet.agent` entities plus
//!      `runs_on host` relations from the envelopes fleet agents already
//!      emit. Zero new wire format on the producer side.
//!   2. Listens on `<prefix>.cmdb.query.>` and answers natural-language or
//!      JSON-encoded queries so any fleet agent can do
//!      `ana send query --to cmdb --query '...'` and get a structured reply.
//!   3. Publishes change events on `<prefix>.cmdb.alert.<event>.<id>` when
//!      entities change (P1.1 — wire to Store change feed).
//!
//! Envelope wire format mirrors ana's Python pydantic models exactly; the
//! protocol is library-agnostic so we are interoperable.

pub mod envelopes;
pub mod subjects;
pub mod bridge;

pub use bridge::serve_bus;
