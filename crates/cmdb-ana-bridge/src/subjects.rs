//! Subject builder/parser — `<prefix>.<agent>.<verb>[.<topic>]`.

pub struct SubjectScheme {
    pub prefix: String,
}

impl SubjectScheme {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self { prefix: prefix.into() }
    }

    pub fn agent(&self, agent: &str) -> String {
        format!("{}.{}", self.prefix, agent)
    }

    pub fn verb(&self, agent: &str, verb: &str, topic: Option<&str>) -> String {
        match topic {
            Some(t) if !t.is_empty() => format!("{}.{}.{}.{}", self.prefix, agent, verb, t),
            _ => format!("{}.{}.{}", self.prefix, agent, verb),
        }
    }

    pub fn query(&self, agent: &str, topic: &str) -> String {
        self.verb(agent, "query", Some(topic))
    }

    pub fn reply(&self, agent: &str, topic: &str) -> String {
        self.verb(agent, "reply", Some(topic))
    }

    pub fn discovery(&self, agent: &str) -> String {
        self.verb(agent, "discovery", None)
    }

    pub fn pulse(&self, agent: &str) -> String {
        self.verb(agent, "pulse", None)
    }

    pub fn alert(&self, agent: &str, event: &str) -> String {
        self.verb(agent, "alert", Some(event))
    }

    pub fn all_under_prefix(&self) -> String {
        format!("{}.>", self.prefix)
    }

    pub fn all_discoveries(&self) -> String {
        format!("{}.*.discovery", self.prefix)
    }

    pub fn all_pulses(&self) -> String {
        format!("{}.*.pulse", self.prefix)
    }

    pub fn all_alerts(&self) -> String {
        format!("{}.*.alert.>", self.prefix)
    }

    pub fn parse(&self, subject: &str) -> ParsedSubject {
        let parts: Vec<&str> = subject.split('.').collect();
        let prefix_parts: Vec<&str> = self.prefix.split('.').collect();
        if parts.len() < prefix_parts.len() || parts[..prefix_parts.len()] != prefix_parts[..] {
            return ParsedSubject::default();
        }
        let rest = &parts[prefix_parts.len()..];
        ParsedSubject {
            prefix: Some(self.prefix.clone()),
            agent: rest.first().map(|s| s.to_string()),
            verb: rest.get(1).map(|s| s.to_string()),
            topic: rest.get(2..).map(|t| t.join(".")),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ParsedSubject {
    pub prefix: Option<String>,
    pub agent: Option<String>,
    pub verb: Option<String>,
    pub topic: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let s = SubjectScheme::new("cc.fleet");
        assert_eq!(s.query("cmdb", "find"), "cc.fleet.cmdb.query.find");
        assert_eq!(s.discovery("e15"), "cc.fleet.e15.discovery");

        let p = s.parse("cc.fleet.alice.query.status");
        assert_eq!(p.agent.as_deref(), Some("alice"));
        assert_eq!(p.verb.as_deref(), Some("query"));
        assert_eq!(p.topic.as_deref(), Some("status"));
    }
}
