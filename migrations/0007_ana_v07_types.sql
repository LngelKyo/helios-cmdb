-- ana v0.7 entity types: injector backends, transactions, durable consumers,
-- JetStream streams.

INSERT INTO entity_types (namespace, name, description, attrs_schema) VALUES
    ('cc.fleet', 'fleet.injector',
     'An injector backend on an ana listener (tmux/opencode/hermes).',
     '{"type":"object","properties":{"backend":{"type":"string"},"target":{"type":"string"},"safe_mode":{"type":"boolean"},"allow_tools":{"type":"boolean"},"allowlist":{"type":"array","items":{"type":"string"}}}}'::jsonb),
    ('cc.fleet', 'fleet.txn',
     'A TxnManager transaction tracked in JetStream KV.',
     '{"type":"object","properties":{"status":{"type":"string"},"to":{"type":"string"},"query":{"type":"string"},"retries":{"type":"integer"},"timeout_s":{"type":"number"}}}'::jsonb),
    ('cc.fleet', 'fleet.durable_consumer',
     'A JetStream durable consumer bound to an ana listener.',
     '{"type":"object","properties":{"durable_name":{"type":"string"},"stream_name":{"type":"string"},"subject":{"type":"string"}}}'::jsonb),
    ('cc.fleet', 'nats.stream',
     'A JetStream stream covering cc.fleet.> subjects.',
     '{"type":"object","properties":{"name":{"type":"string"},"subjects":{"type":"array","items":{"type":"string"}},"max_age_s":{"type":"number"},"storage":{"type":"string"}}}'::jsonb)
ON CONFLICT (namespace, name) DO NOTHING;
