-- PG LISTEN/NOTIFY triggers so the cmdb changebus process can forward
-- change events to NATS (cc.fleet.cmdb.alert.*).
--
-- Payload is a compact JSON: {"op": "entity_upsert", "id": "...", "ns": "...", "ts": "..."}.
-- The changebus subscribes to the 'cmdb_changes' channel and republishes.

CREATE OR REPLACE FUNCTION cmdb_notify_change() RETURNS trigger AS $body$
DECLARE
    payload jsonb;
BEGIN
    payload := jsonb_build_object(
        'op',         TG_OP,
        'table',      TG_TABLE_NAME,
        'id',         CASE WHEN TG_OP = 'DELETE' THEN OLD.id::text ELSE NEW.id::text END,
        'namespace',  CASE WHEN TG_OP = 'DELETE' THEN OLD.namespace ELSE NEW.namespace END,
        'ts',         NOW()
    );
    PERFORM pg_notify('cmdb_changes', payload::text);
    RETURN COALESCE(NEW, OLD);
END;
$body$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS entities_change_notify ON entities;
CREATE TRIGGER entities_change_notify
    AFTER INSERT OR UPDATE OR DELETE ON entities
    FOR EACH ROW EXECUTE FUNCTION cmdb_notify_change();

DROP TRIGGER IF EXISTS relations_change_notify ON relations;
CREATE TRIGGER relations_change_notify
    AFTER INSERT OR UPDATE OR DELETE ON relations
    FOR EACH ROW EXECUTE FUNCTION cmdb_notify_change();

DROP TRIGGER IF EXISTS facts_change_notify ON facts;
CREATE TRIGGER facts_change_notify
    AFTER INSERT OR UPDATE OR DELETE ON facts
    FOR EACH ROW EXECUTE FUNCTION cmdb_notify_change();
