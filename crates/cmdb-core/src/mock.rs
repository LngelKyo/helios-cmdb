//! In-memory `Store` implementation. Useful for tests and for running the
//! CLI without a database during development.

use crate::change::{Change, ChangeOp};
use crate::entity::{Entity, EntityInput};
use crate::error::{StoreError, StoreResult};
use crate::fact::{Fact, FactInput, FactQuery};
use crate::id::{ChangeId, EntityId, FactId, RelationId};
use crate::namespace::Namespace;
use crate::relation::{Relation, RelationInput};
use crate::source::Source;
use crate::store::{Direction, QueryFilter, TraverseHit, TraverseStep};
use crate::Store;
use async_trait::async_trait;
use chrono::Utc;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub struct InMemoryStore {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    entities: HashMap<EntityId, Entity>,
    by_name: BTreeMap<(Namespace, String, String), EntityId>,
    relations: HashMap<RelationId, Relation>,
    facts: HashMap<FactId, Fact>,
    changes: Vec<Change>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Store for InMemoryStore {
    fn name(&self) -> &'static str {
        "memory"
    }

    async fn put_entity(&self, input: EntityInput, source: Source) -> StoreResult<Entity> {
        let mut g = self.inner.lock().unwrap();
        let key = (input.namespace.clone(), input.entity_type.clone(), input.name.clone());

        let entity = if let Some(id) = g.by_name.get(&key).copied() {
            let ent = g.entities.get_mut(&id).expect("by_name points to existing entity");
            ent.attrs = input.attrs;
            ent.tags = input.tags;
            ent.updated_at = Utc::now();
            ent.version += 1;
            ent.clone()
        } else {
            let id = EntityId::new();
            let now = Utc::now();
            let entity = Entity {
                id,
                namespace: input.namespace.clone(),
                entity_type: input.entity_type.clone(),
                name: input.name.clone(),
                attrs: input.attrs,
                tags: input.tags,
                created_at: now,
                updated_at: now,
                version: 1,
            };
            g.entities.insert(id, entity.clone());
            g.by_name.insert(key, id);
            entity
        };

        g.changes.push(Change {
            id: ChangeId::new(),
            ts: Utc::now(),
            namespace: entity.namespace.clone(),
            actor: source.identity.clone(),
            op: ChangeOp::EntityUpsert,
            target_type: entity.entity_type.clone(),
            target_id: Some(entity.id),
            before: None,
            after: Some(serde_json::to_value(&entity).map_err(|e| StoreError::Backend(e.to_string()))?),
            reason: None,
        });

        Ok(entity)
    }

    async fn get_entity(
        &self,
        namespace: &str,
        entity_type: &str,
        name: &str,
    ) -> StoreResult<Option<Entity>> {
        let g = self.inner.lock().unwrap();
        Ok(g.by_name
            .get(&(namespace.to_string(), entity_type.to_string(), name.to_string()))
            .and_then(|id| g.entities.get(id).cloned()))
    }

    async fn get_entity_by_id(&self, id: EntityId) -> StoreResult<Option<Entity>> {
        let g = self.inner.lock().unwrap();
        Ok(g.entities.get(&id).cloned())
    }

    async fn query_entities(&self, filter: QueryFilter) -> StoreResult<Vec<Entity>> {
        let g = self.inner.lock().unwrap();
        let mut out: Vec<Entity> = g
            .entities
            .values()
            .filter(|e| {
                if let Some(ns) = &filter.namespace {
                    if &e.namespace != ns {
                        return false;
                    }
                }
                if let Some(t) = &filter.entity_type {
                    if &e.entity_type != t {
                        return false;
                    }
                }
                if let Some(p) = &filter.name_prefix {
                    if !e.name.starts_with(p) {
                        return false;
                    }
                }
                for tag in &filter.tags {
                    if !e.tags.contains(tag) {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();
        out.sort_by_key(|e| e.created_at);
        if let Some(limit) = filter.limit {
            out.truncate(limit as usize);
        }
        Ok(out)
    }

    async fn delete_entity(&self, id: EntityId) -> StoreResult<()> {
        let mut g = self.inner.lock().unwrap();
        if let Some(entity) = g.entities.remove(&id) {
            g.by_name.remove(&(
                entity.namespace.clone(),
                entity.entity_type.clone(),
                entity.name.clone(),
            ));
            g.relations.retain(|_, r| r.from_id != id && r.to_id != id);
            g.facts.retain(|_, f| f.entity_id != id);
            g.changes.push(Change {
                id: ChangeId::new(),
                ts: Utc::now(),
                namespace: entity.namespace,
                actor: "system".into(),
                op: ChangeOp::EntityDelete,
                target_type: entity.entity_type,
                target_id: Some(id),
                before: None,
                after: None,
                reason: None,
            });
        }
        Ok(())
    }

    async fn put_relation(&self, input: RelationInput) -> StoreResult<Relation> {
        let from = self.resolve_ref(&input.namespace, &input.from).await?;
        let to = self.resolve_ref(&input.namespace, &input.to).await?;

        let mut g = self.inner.lock().unwrap();
        let existing = g.relations.iter().find(|(_, r)| {
            r.namespace == input.namespace
                && r.from_id == from.id
                && r.to_id == to.id
                && r.relation_type == input.relation_type
        });

        if let Some((_, r)) = existing {
            let r_clone = r.clone();
            return Ok(r_clone);
        }

        let id = RelationId::new();
        let relation = Relation {
            id,
            namespace: input.namespace.clone(),
            from_id: from.id,
            to_id: to.id,
            relation_type: input.relation_type.clone(),
            props: input.props,
            created_at: Utc::now(),
        };
        g.relations.insert(id, relation.clone());

        g.changes.push(Change {
            id: ChangeId::new(),
            ts: Utc::now(),
            namespace: relation.namespace.clone(),
            actor: "system".into(),
            op: ChangeOp::RelationUpsert,
            target_type: relation.relation_type.clone(),
            target_id: None,
            before: None,
            after: None,
            reason: None,
        });

        Ok(relation)
    }

    async fn delete_relation(&self, id: RelationId) -> StoreResult<()> {
        let mut g = self.inner.lock().unwrap();
        g.relations.remove(&id);
        Ok(())
    }

    async fn traverse(&self, from: EntityId, step: TraverseStep) -> StoreResult<Vec<TraverseHit>> {
        let g = self.inner.lock().unwrap();
        let start = g
            .entities
            .get(&from)
            .ok_or_else(|| StoreError::NotFound(format!("entity {from}")))?;
        let start_ns = start.namespace.clone();
        drop(start);

        let mut visited: BTreeSet<EntityId> = BTreeSet::from([from]);
        let mut frontier: Vec<(EntityId, u32, Vec<EntityId>)> = vec![(from, 0, vec![from])];
        let mut hits: Vec<TraverseHit> = Vec::new();

        while let Some((cur, depth, path)) = frontier.pop() {
            if depth >= step.max_depth {
                continue;
            }
            for r in g.relations.values() {
                if r.namespace != start_ns {
                    continue;
                }
                if let Some(rt) = &step.relation_type {
                    if &r.relation_type != rt {
                        continue;
                    }
                }
                let next = match step.direction {
                    Direction::Outgoing if r.from_id == cur => r.to_id,
                    Direction::Incoming if r.to_id == cur => r.from_id,
                    Direction::Both if r.from_id == cur => r.to_id,
                    Direction::Both if r.to_id == cur => r.from_id,
                    _ => continue,
                };
                if visited.contains(&next) {
                    continue;
                }
                visited.insert(next);
                if let Some(entity) = g.entities.get(&next) {
                    let mut new_path = path.clone();
                    new_path.push(next);
                    hits.push(TraverseHit {
                        entity: entity.clone(),
                        depth: depth + 1,
                        path: new_path.clone(),
                        via_relation_type: Some(r.relation_type.clone()),
                    });
                    frontier.push((next, depth + 1, new_path));
                }
            }
        }

        hits.sort_by_key(|h| h.depth);
        Ok(hits)
    }

    async fn add_fact(&self, input: FactInput) -> StoreResult<Fact> {
        let entity = self.resolve_ref(&input.namespace, &input.entity).await?;

        let mut g = self.inner.lock().unwrap();
        let new_id = FactId::new();
        let to_supersede: Vec<FactId> = g
            .facts
            .values()
            .filter(|f| f.entity_id == entity.id && f.key == input.key && f.superseded_by.is_none())
            .map(|f| f.id)
            .collect();

        for sid in &to_supersede {
            if let Some(f) = g.facts.get_mut(sid) {
                f.superseded_by = Some(new_id);
            }
        }

        let fact = Fact {
            id: new_id,
            namespace: input.namespace.clone(),
            entity_id: entity.id,
            key: input.key,
            value: input.value,
            source: input.source,
            superseded_by: None,
        };
        g.facts.insert(new_id, fact.clone());

        g.changes.push(Change {
            id: ChangeId::new(),
            ts: Utc::now(),
            namespace: fact.namespace.clone(),
            actor: fact.source.identity.clone(),
            op: ChangeOp::FactAdd,
            target_type: fact.key.clone(),
            target_id: Some(entity.id),
            before: None,
            after: Some(fact.value.clone()),
            reason: None,
        });

        Ok(fact)
    }

    async fn effective_facts(
        &self,
        entity_id: EntityId,
        query: FactQuery,
    ) -> StoreResult<Vec<Fact>> {
        let g = self.inner.lock().unwrap();
        let now = Utc::now();
        let mut latest_by_key: HashMap<String, Fact> = HashMap::new();
        for f in g.facts.values() {
            if f.entity_id != entity_id || f.superseded_by.is_some() {
                continue;
            }
            if let Some(min) = query.min_confidence {
                if f.source.confidence < min {
                    continue;
                }
            }
            if !query.include_expired && f.source.is_expired(now) {
                continue;
            }
            if !query.source_kinds.is_empty() && !query.source_kinds.contains(&f.source.kind) {
                continue;
            }
            if let Some(max_age) = query.max_age_seconds {
                let age = now.signed_duration_since(f.source.observed_at).num_seconds();
                if age > max_age {
                    continue;
                }
            }
            let key = f.key.clone();
            latest_by_key
                .entry(key)
                .and_modify(|existing| {
                    if f.source.confidence > existing.source.confidence
                        || f.source.observed_at > existing.source.observed_at
                    {
                        *existing = f.clone();
                    }
                })
                .or_insert_with(|| f.clone());
        }
        let mut out: Vec<Fact> = latest_by_key.into_values().collect();
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    async fn history(
        &self,
        namespace: Option<&str>,
        entity_id: Option<EntityId>,
        limit: u32,
    ) -> StoreResult<Vec<Change>> {
        let g = self.inner.lock().unwrap();
        let mut out: Vec<Change> = g
            .changes
            .iter()
            .filter(|c| {
                if let Some(ns) = namespace {
                    if &c.namespace != ns {
                        return false;
                    }
                }
                if let Some(eid) = entity_id {
                    if c.target_id != Some(eid) {
                        return false;
                    }
                }
                true
            })
            .take(limit as usize)
            .cloned()
            .collect();
        out.reverse();
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::EntityInput;
    use crate::entity::EntityRef;
    use crate::relation::RelationInput;
    use crate::source::Source;

    fn src() -> Source {
        Source::new_cli("user:tester")
    }

    #[tokio::test]
    async fn put_get_entity() {
        let store = InMemoryStore::new();
        let e = store
            .put_entity(
                EntityInput::new("cc.fleet", "fleet.host", "miraku-home")
                    .with_tags(["nixos".into()]),
                src(),
            )
            .await
            .unwrap();
        assert_eq!(e.version, 1);

        let got = store
            .get_entity("cc.fleet", "fleet.host", "miraku-home")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.id, e.id);
        assert!(got.tags.contains("nixos"));
    }

    #[tokio::test]
    async fn upsert_bumps_version() {
        let store = InMemoryStore::new();
        let e1 = store
            .put_entity(EntityInput::new("ns", "t", "a"), src())
            .await
            .unwrap();
        let e2 = store
            .put_entity(
                EntityInput::new("ns", "t", "a")
                    .with_attrs(serde_json::json!({"ram_gb": 32})),
                src(),
            )
            .await
            .unwrap();
        assert_eq!(e1.id, e2.id);
        assert_eq!(e2.version, 2);
        assert_eq!(e2.attrs["ram_gb"], 32);
    }

    #[tokio::test]
    async fn relation_and_traverse() {
        let store = InMemoryStore::new();
        let host = store
            .put_entity(EntityInput::new("ns", "host", "h1"), src())
            .await
            .unwrap();
        let agent = store
            .put_entity(EntityInput::new("ns", "agent", "a1"), src())
            .await
            .unwrap();
        store
            .put_relation(RelationInput::new(
                "ns",
                EntityRef::by_id(agent.id),
                EntityRef::by_id(host.id),
                "runs_on",
            ))
            .await
            .unwrap();

        let hits = store
            .traverse(agent.id, TraverseStep::outgoing(3))
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entity.id, host.id);
        assert_eq!(hits[0].via_relation_type.as_deref(), Some("runs_on"));
        assert_eq!(hits[0].depth, 1);
    }

    #[tokio::test]
    async fn fact_supersession() {
        let store = InMemoryStore::new();
        let e = store
            .put_entity(EntityInput::new("ns", "t", "x"), src())
            .await
            .unwrap();

        let f1 = store
            .add_fact(FactInput {
                namespace: "ns".into(),
                entity: EntityRef::by_id(e.id),
                key: "load".into(),
                value: serde_json::json!(0.5),
                source: src(),
            })
            .await
            .unwrap();
        let f2 = store
            .add_fact(FactInput {
                namespace: "ns".into(),
                entity: EntityRef::by_id(e.id),
                key: "load".into(),
                value: serde_json::json!(0.8),
                source: src(),
            })
            .await
            .unwrap();

        // The local f1 here was returned before f2 existed, so its
        // superseded_by snapshot is None; the in-store f1 was mutated by the
        // second call. We verify the effective behavior instead.
        let _ = (f1, f2);

        let eff = store.effective_facts(e.id, FactQuery::default()).await.unwrap();
        assert_eq!(eff.len(), 1);
        assert_eq!(eff[0].value, serde_json::json!(0.8));
    }

    #[tokio::test]
    async fn history_records_writes() {
        let store = InMemoryStore::new();
        let e = store
            .put_entity(EntityInput::new("ns", "t", "x"), src())
            .await
            .unwrap();
        let h = store.history(Some("ns"), Some(e.id), 10).await.unwrap();
        assert!(!h.is_empty());
        assert_eq!(h[0].op, ChangeOp::EntityUpsert);
    }
}
