//! CLI command implementations.

use crate::output;
use crate::{Cli, Command};
use anyhow::{anyhow, Result};
use clap::Args;
use cmdb_core::entity::{EntityInput, EntityRef};
use cmdb_core::fact::FactInput;
use cmdb_core::id::EntityId;
use cmdb_core::relation::RelationInput;
use cmdb_core::source::Source;
use cmdb_core::store::{Direction, QueryFilter, TraverseStep};
use cmdb_core::Store;
use cmdb_store_pg::PgStore;
use serde_json::Value;
use std::str::FromStr;

pub async fn dispatch(cli: Cli) -> Result<()> {
    let namespace = cli.namespace.clone();
    let actor = cli.actor.clone();
    let store = open_store(&cli).await?;

    match cli.command {
        Command::Migrate => migrate(&store).await?,
        Command::PutEntity(args) => put_entity(&namespace, &actor, &store, args).await?,
        Command::Get(args) => get(&store, args).await?,
        Command::List(args) => list(&namespace, &store, args).await?,
        Command::Relate(args) => relate(&namespace, &store, args).await?,
        Command::Query(args) => query(&store, args).await?,
    }
    Ok(())
}

async fn open_store(cli: &Cli) -> Result<PgStore> {
    let url = cli
        .database_url
        .as_ref()
        .ok_or_else(|| anyhow!("--database-url or CMDB_DATABASE_URL is required"))?;
    PgStore::connect(url).await
}

async fn migrate(store: &PgStore) -> Result<()> {
    print!("applying migrations... ");
    store.run_migrations().await?;
    println!("ok");
    Ok(())
}

#[derive(Args, Debug)]
pub struct PutEntityArgs {
    #[arg(long)]
    pub r#type: String,
    #[arg(long)]
    pub name: String,
    #[arg(long, value_name = "JSON")]
    pub attrs: Option<String>,
    #[arg(long, value_delimiter = ',')]
    pub tags: Vec<String>,
    #[arg(long, default_value_t = false)]
    pub fact: bool,
}

async fn put_entity(
    namespace: &str,
    actor: &str,
    store: &PgStore,
    args: PutEntityArgs,
) -> Result<()> {
    let attrs: Value = match &args.attrs {
        Some(s) => serde_json::from_str(s)?,
        None => Value::Object(Default::default()),
    };
    let input = EntityInput::new(namespace, &args.r#type, &args.name)
        .with_attrs(attrs.clone())
        .with_tags(args.tags.clone().into_iter());
    let source = Source::new_cli(actor);

    let entity = store.put_entity(input, source).await?;
    output::entity(&entity);

    if args.fact {
        let _ = store
            .add_fact(FactInput {
                namespace: namespace.to_string(),
                entity: EntityRef::by_id(entity.id),
                key: "_attrs".into(),
                value: attrs,
                source: Source::new_cli(actor),
            })
            .await?;
    }
    Ok(())
}

#[derive(Args, Debug)]
pub struct GetArgs {
    #[arg(long)]
    pub r#type: Option<String>,
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long)]
    pub id: Option<String>,
    #[arg(long, default_value_t = false)]
    pub facts: bool,
}

async fn get(store: &PgStore, args: GetArgs) -> Result<()> {
    let entity = if let Some(id_str) = &args.id {
        let id = EntityId::from_str(id_str)?;
        store.get_entity_by_id(id).await?
    } else {
        let t = args
            .r#type
            .as_deref()
            .ok_or_else(|| anyhow!("--type is required when not using --id"))?;
        let n = args
            .name
            .as_deref()
            .ok_or_else(|| anyhow!("--name is required when not using --id"))?;
        store.get_entity("cc.fleet", t, n).await?
    }
    .ok_or_else(|| anyhow!("entity not found"))?;

    output::entity(&entity);

    if args.facts {
        let facts = store
            .effective_facts(entity.id, Default::default())
            .await?;
        println!("\n--- facts ---");
        for f in &facts {
            if f.key == "_attrs" {
                continue;
            }
            println!(
                "  {:<20} {} (conf={}, src={})",
                f.key, f.value, f.source.confidence, f.source.identity
            );
        }
    }
    Ok(())
}

#[derive(Args, Debug)]
pub struct ListArgs {
    #[arg(long)]
    pub r#type: Option<String>,
    #[arg(long)]
    pub name_prefix: Option<String>,
    #[arg(long, value_delimiter = ',')]
    pub tags: Vec<String>,
    #[arg(long, default_value_t = 100)]
    pub limit: u32,
}

async fn list(namespace: &str, store: &PgStore, args: ListArgs) -> Result<()> {
    let mut filter = QueryFilter::new()
        .in_namespace(namespace)
        .with_limit(args.limit);
    if let Some(t) = &args.r#type {
        filter = filter.of_type(t);
    }
    if let Some(p) = &args.name_prefix {
        filter.name_prefix = Some(p.clone());
    }
    filter.tags = args.tags;
    let entities = store.query_entities(filter).await?;
    output::entities(&entities);
    Ok(())
}

#[derive(Args, Debug)]
pub struct RelateArgs {
    pub from: String,
    pub relation_type: String,
    pub to: String,
    #[arg(long, value_name = "JSON")]
    pub props: Option<String>,
}

fn parse_ref(s: &str) -> Result<EntityRef> {
    if let Ok(id) = EntityId::from_str(s) {
        return Ok(EntityRef::by_id(id));
    }
    let (t, n) = s
        .split_once(':')
        .ok_or_else(|| anyhow!("'{}' is neither an id nor a type:name ref", s))?;
    Ok(EntityRef::by_name("cc.fleet", t, n))
}

async fn relate(namespace: &str, store: &PgStore, args: RelateArgs) -> Result<()> {
    let from = parse_ref(&args.from)?;
    let to = parse_ref(&args.to)?;
    let props = match args.props {
        Some(s) => serde_json::from_str(&s)?,
        None => Value::Object(Default::default()),
    };
    let input = RelationInput {
        namespace: namespace.to_string(),
        from,
        to,
        relation_type: args.relation_type,
        props,
    };
    let relation = store.put_relation(input).await?;
    println!("{}", serde_json::to_string_pretty(&relation)?);
    Ok(())
}

#[derive(Args, Debug)]
pub struct QueryArgs {
    #[arg(long)]
    pub traverse: bool,
    #[arg(long)]
    pub from: String,
    #[arg(long, default_value_t = 3)]
    pub depth: u32,
    #[arg(long)]
    pub relation_type: Option<String>,
    #[arg(long, default_value = "outgoing")]
    pub direction: String,
}

async fn query(store: &PgStore, args: QueryArgs) -> Result<()> {
    let from_id = match parse_ref(&args.from)? {
        EntityRef::Id { id } => id,
        EntityRef::Name {
            namespace,
            entity_type,
            name,
        } => store
            .get_entity(&namespace, &entity_type, &name)
            .await?
            .ok_or_else(|| anyhow!("entity not found: {namespace}/{entity_type}/{name}"))?
            .id,
    };
    let direction = match args.direction.as_str() {
        "outgoing" | "out" => Direction::Outgoing,
        "incoming" | "in" => Direction::Incoming,
        "both" => Direction::Both,
        other => return Err(anyhow!("invalid --direction '{}'", other)),
    };
    let step = TraverseStep {
        relation_type: args.relation_type,
        direction,
        max_depth: args.depth,
    };
    let hits = store.traverse(from_id, step).await?;
    output::traverse_hits(&hits);
    Ok(())
}
