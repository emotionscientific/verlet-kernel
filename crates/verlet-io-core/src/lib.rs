//! Protocol-neutral IO contracts for Verlet.
//!
//! This crate defines the boundary above the runtime app-server:
//!
//! ```text
//! external protocol event
//!   -> IngressEnvelope
//!   -> queue / dedupe
//!   -> resolver
//!   -> admission policy
//!   -> Verlet runtime bridge
//!   -> EgressEnvelope
//!   -> protocol delivery
//! ```
//!
//! It intentionally avoids depending on the root `verlet` crate. The daemon
//! bridge is responsible for mapping [`ThreadAddress`] and [`AdmissionDecision`]
//! onto concrete `ThreadCoordinates`, `TurnInput`, and runtime event types.

pub type IoResult<T> = Result<T, IoError>;

#[derive(Debug, thiserror::Error)]
pub enum IoError {
    #[error("invalid IO envelope: {0}")]
    InvalidEnvelope(String),
    #[error("unknown protocol kind {0:?}")]
    UnknownProtocol(String),
    #[error("IO policy rejected event: {0}")]
    PolicyRejected(String),
    #[error("IO queue failed: {0}")]
    Queue(String),
    #[error("IO delivery failed: {0}")]
    Delivery(String),
    /// Delivery cannot succeed however many times it is attempted: the chat is
    /// gone, the bot was blocked, the token is bad. Retrying burns the whole
    /// attempt budget to reach the same dead letter, so the egress loop stops
    /// after the first one.
    #[error("IO delivery permanently failed: {0}")]
    PermanentDelivery(String),
    /// The remote asked us to slow down. `retry_after_ms` is the remote's own
    /// figure when it supplied one; honoring it matters because retrying a
    /// rate limit early typically extends it.
    #[error("IO delivery was rate limited: {message}")]
    RateLimited {
        retry_after_ms: Option<u64>,
        message: String,
    },
    #[error("IO bridge failed: {0}")]
    Bridge(String),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IoSource {
    pub protocol: String,
    pub instance_id: String,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl IoSource {
    pub fn new(protocol: impl Into<String>, instance_id: impl Into<String>) -> Self {
        Self {
            protocol: protocol.into(),
            instance_id: instance_id.into(),
            metadata: std::collections::BTreeMap::new(),
        }
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn stable_scope(&self) -> String {
        format!("{}:{}", self.protocol, self.instance_id)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationKind {
    Direct,
    Group,
    Channel,
    Thread,
    System,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IoConversation {
    pub external_conversation_id: String,
    pub kind: ConversationKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl IoConversation {
    pub fn new(external_conversation_id: impl Into<String>, kind: ConversationKind) -> Self {
        Self {
            external_conversation_id: external_conversation_id.into(),
            kind,
            title: None,
            external_thread_id: None,
            metadata: std::collections::BTreeMap::new(),
        }
    }

    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn with_external_thread_id(mut self, external_thread_id: impl Into<String>) -> Self {
        self.external_thread_id = Some(external_thread_id.into());
        self
    }

    pub fn stable_key(&self) -> String {
        match &self.external_thread_id {
            Some(thread_id) => {
                format!("{}#{thread_id}", self.external_conversation_id)
            }
            None => self.external_conversation_id.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IoActor {
    pub external_actor_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default)]
    pub is_bot: bool,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl IoActor {
    pub fn new(external_actor_id: impl Into<String>) -> Self {
        Self {
            external_actor_id: external_actor_id.into(),
            display_name: None,
            is_bot: false,
            metadata: std::collections::BTreeMap::new(),
        }
    }

    pub fn with_display_name(mut self, display_name: impl Into<String>) -> Self {
        self.display_name = Some(display_name.into());
        self
    }

    pub fn bot(mut self) -> Self {
        self.is_bot = true;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IoDedupeKey {
    pub scope: String,
    pub key: String,
}

impl IoDedupeKey {
    pub fn new(scope: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            scope: scope.into(),
            key: key.into(),
        }
    }

    pub fn for_source(source: &IoSource, key: impl Into<String>) -> Self {
        Self::new(source.stable_scope(), key)
    }

    pub fn stable_key(&self) -> String {
        format!("{}:{}", self.scope, self.key)
    }
}

/// The external system's own identity for one delivery of an ingress event
/// (ADR 0007). The envelope's internal id names our receipt of the event;
/// this names the event as the external system knows it, which is what
/// redelivery dedupes on.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IoDelivery {
    /// A Telegram update id, a webhook delivery id, a queue message id, or a
    /// scheduler occurrence ("{mandate_event_id}:{occurrence_index}").
    pub delivery_id: String,
    /// Redelivery attempt ordinal when the external system reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl IoDelivery {
    pub fn new(delivery_id: impl Into<String>) -> Self {
        Self {
            delivery_id: delivery_id.into(),
            attempt: None,
            metadata: std::collections::BTreeMap::new(),
        }
    }

    pub fn with_attempt(mut self, attempt: u32) -> Self {
        self.attempt = Some(attempt);
        self
    }
}

/// The principal an ingress envelope acts for, stamped before admission
/// (ADR 0007). Self-attributing sources (the clock route) stamp it at
/// construction; protocol adapters leave it unset and the daemon stamps it
/// during resolution from the route binding. `envelope.actor` remains
/// external provenance and is never itself the principal.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IoPrincipal {
    pub tenant_id: String,
    pub principal_id: String,
    /// How attribution happened: "mandate:{event_id}", "route:{route_id}",
    /// or "caller:{session_id}" for authenticated RPC ingress.
    pub via: String,
}

impl IoPrincipal {
    pub fn new(
        tenant_id: impl Into<String>,
        principal_id: impl Into<String>,
        via: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            principal_id: principal_id.into(),
            via: via.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IngressContent {
    Text {
        text: String,
    },
    Command {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        args: Option<String>,
    },
    Event {
        kind: String,
        #[serde(default)]
        payload: serde_json::Value,
    },
}

impl IngressContent {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }

    pub fn text_projection(&self) -> String {
        match self {
            Self::Text { text } => text.clone(),
            Self::Command { name, args } => match args {
                Some(args) if !args.is_empty() => format!("/{name} {args}"),
                _ => format!("/{name}"),
            },
            Self::Event { kind, payload } => {
                if payload.is_null() {
                    kind.clone()
                } else {
                    format!("{kind}: {payload}")
                }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IoAttachment {
    pub id: String,
    pub media_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl IoAttachment {
    pub fn new(id: impl Into<String>, media_type: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            media_type: media_type.into(),
            name: None,
            uri: None,
            size_bytes: None,
            metadata: std::collections::BTreeMap::new(),
        }
    }

    pub fn with_uri(mut self, uri: impl Into<String>) -> Self {
        self.uri = Some(uri.into());
        self
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IngressEnvelope {
    pub id: String,
    pub source: IoSource,
    pub conversation: IoConversation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<IoActor>,
    pub content: IngressContent,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<IoAttachment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedupe_key: Option<IoDedupeKey>,
    /// External delivery identity (ADR 0007). `Option` only for wire
    /// compatibility with pre-contract queued envelopes; the submit boundary
    /// enforces presence via [`IngressEnvelope::require_witnessed`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<IoDelivery>,
    /// Principal attribution (ADR 0007). `Option` because protocol adapters
    /// cannot attribute; the admission boundary enforces presence via
    /// [`IngressEnvelope::require_attributed`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<IoPrincipal>,
    pub received_at_ms: u64,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl IngressEnvelope {
    pub fn new(
        source: IoSource,
        conversation: IoConversation,
        content: IngressContent,
        received_at_ms: u64,
    ) -> Self {
        Self {
            id: new_io_id("ing"),
            source,
            conversation,
            actor: None,
            content,
            attachments: Vec::new(),
            dedupe_key: None,
            delivery: None,
            principal: None,
            received_at_ms,
            metadata: std::collections::BTreeMap::new(),
        }
    }

    pub fn with_actor(mut self, actor: IoActor) -> Self {
        self.actor = Some(actor);
        self
    }

    pub fn with_dedupe_key(mut self, dedupe_key: IoDedupeKey) -> Self {
        self.dedupe_key = Some(dedupe_key);
        self
    }

    pub fn with_delivery(mut self, delivery: IoDelivery) -> Self {
        self.delivery = Some(delivery);
        self
    }

    pub fn with_principal(mut self, principal: IoPrincipal) -> Self {
        self.principal = Some(principal);
        self
    }

    /// The dedupe identity redelivery is judged by (ADR 0007 D1): an
    /// explicitly set `dedupe_key` wins; otherwise the key derives from
    /// `delivery` as `IoDedupeKey::for_source(&source, &delivery.delivery_id)`.
    /// `None` only when the envelope carries neither, which no boundary
    /// accepts.
    pub fn effective_dedupe_key(&self) -> Option<IoDedupeKey> {
        self.dedupe_key.clone().or_else(|| {
            self.delivery
                .as_ref()
                .map(|delivery| IoDedupeKey::for_source(&self.source, delivery.delivery_id.clone()))
        })
    }

    /// Submit-boundary check (ADR 0007 D3): `delivery` present with a
    /// non-empty `delivery_id`, and an effective dedupe key exists. Rejection
    /// is `IoError::InvalidEnvelope` naming the missing attribute.
    pub fn require_witnessed(&self) -> IoResult<()> {
        let delivery = self
            .delivery
            .as_ref()
            .ok_or_else(|| IoError::InvalidEnvelope("delivery is required".to_string()))?;
        if delivery.delivery_id.is_empty() {
            return Err(IoError::InvalidEnvelope(
                "delivery.delivery_id cannot be empty".to_string(),
            ));
        }
        if self.effective_dedupe_key().is_none() {
            return Err(IoError::InvalidEnvelope(
                "effective dedupe key is required".to_string(),
            ));
        }
        Ok(())
    }

    /// Admission-boundary check (ADR 0007 D3): [`Self::require_witnessed`],
    /// `principal` present, and `principal.tenant_id` equal to the resolved
    /// target's `tenant_id` (cross-tenant injection guard). Called after
    /// resolution and before the admission decision; this is the guarantee,
    /// independent of any sink's early check.
    pub fn require_attributed(&self, target: &ResolvedIoTarget) -> IoResult<()> {
        self.require_witnessed()?;
        let principal = self
            .principal
            .as_ref()
            .ok_or_else(|| IoError::InvalidEnvelope("principal is required".to_string()))?;
        if principal.tenant_id != target.address.tenant_id {
            return Err(IoError::InvalidEnvelope(format!(
                "principal tenant {:?} does not match resolved target tenant {:?}",
                principal.tenant_id, target.address.tenant_id
            )));
        }
        Ok(())
    }

    pub fn with_attachment(mut self, attachment: IoAttachment) -> Self {
        self.attachments.push(attachment);
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadAddress {
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
}

impl ThreadAddress {
    pub fn new(
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
        session_id: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
            session_id: session_id.into(),
            thread_id: None,
        }
    }

    pub fn with_thread_id(mut self, thread_id: impl Into<String>) -> Self {
        self.thread_id = Some(thread_id.into());
        self
    }

    pub fn scope_key(&self) -> String {
        format!("{}:{}:{}", self.tenant_id, self.user_id, self.session_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProviderPolicy {
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl ProviderPolicy {
    pub fn new(provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            metadata: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ResolvedIoTarget {
    pub address: ThreadAddress,
    #[serde(default)]
    pub create_thread_if_missing: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_policy: Option<ProviderPolicy>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl ResolvedIoTarget {
    pub fn new(address: ThreadAddress) -> Self {
        Self {
            address,
            create_thread_if_missing: true,
            parent_thread_id: None,
            provider_policy: None,
            metadata: std::collections::BTreeMap::new(),
        }
    }

    pub fn with_provider_policy(mut self, provider_policy: ProviderPolicy) -> Self {
        self.provider_policy = Some(provider_policy);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IoTurnInput {
    pub text: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<IoAttachment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_policy: Option<ProviderPolicy>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl IoTurnInput {
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            attachments: Vec::new(),
            provider_policy: None,
            metadata: std::collections::BTreeMap::new(),
        }
    }

    pub fn from_envelope(envelope: &IngressEnvelope, target: &ResolvedIoTarget) -> Self {
        Self {
            text: envelope.content.text_projection(),
            attachments: envelope.attachments.clone(),
            provider_policy: target.provider_policy.clone(),
            metadata: envelope.metadata.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionMode {
    Queue,
    Steer,
    Interrupt,
    Fork,
    ObserveOnly,
    Reject,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum AdmissionDecision {
    Queue {
        turn_id: String,
        input: IoTurnInput,
    },
    Steer {
        turn_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_active_turn_id: Option<String>,
        input: IoTurnInput,
    },
    Interrupt {
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replacement_turn_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replacement: Option<IoTurnInput>,
    },
    Fork {
        child_key: String,
        input: IoTurnInput,
    },
    ObserveOnly {
        reason: String,
    },
    Reject {
        reason: String,
    },
}

impl AdmissionDecision {
    pub fn mode(&self) -> AdmissionMode {
        match self {
            Self::Queue { .. } => AdmissionMode::Queue,
            Self::Steer { .. } => AdmissionMode::Steer,
            Self::Interrupt { .. } => AdmissionMode::Interrupt,
            Self::Fork { .. } => AdmissionMode::Fork,
            Self::ObserveOnly { .. } => AdmissionMode::ObserveOnly,
            Self::Reject { .. } => AdmissionMode::Reject,
        }
    }

    pub fn queue(turn_id: impl Into<String>, input: IoTurnInput) -> Self {
        Self::Queue {
            turn_id: turn_id.into(),
            input,
        }
    }

    pub fn steer(
        turn_id: impl Into<String>,
        expected_active_turn_id: Option<String>,
        input: IoTurnInput,
    ) -> Self {
        Self::Steer {
            turn_id: turn_id.into(),
            expected_active_turn_id,
            input,
        }
    }

    pub fn reject(reason: impl Into<String>) -> Self {
        Self::Reject {
            reason: reason.into(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IngressState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_turn_id: Option<String>,
    #[serde(default)]
    pub pending_count: usize,
    #[serde(default)]
    pub dedupe_seen: bool,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IngressAck {
    pub envelope_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedupe_key: Option<IoDedupeKey>,
    pub accepted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl IngressAck {
    pub fn accepted(envelope: &IngressEnvelope) -> Self {
        Self {
            envelope_id: envelope.id.clone(),
            dedupe_key: envelope.effective_dedupe_key(),
            accepted: true,
            reason: None,
        }
    }

    pub fn rejected(envelope: &IngressEnvelope, reason: impl Into<String>) -> Self {
        Self {
            envelope_id: envelope.id.clone(),
            dedupe_key: envelope.effective_dedupe_key(),
            accepted: false,
            reason: Some(reason.into()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IoTarget {
    pub source: IoSource,
    pub conversation: IoConversation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<IoActor>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl IoTarget {
    pub fn reply_to(envelope: &IngressEnvelope) -> Self {
        Self {
            source: envelope.source.clone(),
            conversation: envelope.conversation.clone(),
            actor: envelope.actor.clone(),
            metadata: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, strum::AsRefStr)]
#[serde(tag = "type", rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum EgressKind {
    AssistantDelta {
        text: String,
    },
    AssistantMessage {
        text: String,
    },
    Status {
        text: String,
    },
    ToolStarted {
        name: String,
    },
    ToolCompleted {
        name: String,
        success: bool,
    },
    Error {
        message: String,
    },
    PlatformAction {
        action: String,
        #[serde(default)]
        payload: serde_json::Value,
    },
    Silence {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

impl EgressKind {
    pub fn visible_text(&self) -> Option<&str> {
        match self {
            Self::AssistantDelta { text }
            | Self::AssistantMessage { text }
            | Self::Status { text } => Some(text.as_str()),
            Self::Error { message } => Some(message.as_str()),
            Self::ToolStarted { .. }
            | Self::ToolCompleted { .. }
            | Self::PlatformAction { .. }
            | Self::Silence { .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EgressEnvelope {
    pub id: String,
    pub target: IoTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ingress_id: Option<String>,
    pub kind: EgressKind,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<IoAttachment>,
    pub created_at_ms: u64,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl EgressEnvelope {
    pub fn new(target: IoTarget, kind: EgressKind, created_at_ms: u64) -> Self {
        Self {
            id: new_io_id("eg"),
            target,
            source_ingress_id: None,
            kind,
            attachments: Vec::new(),
            created_at_ms,
            metadata: std::collections::BTreeMap::new(),
        }
    }

    pub fn for_ingress(ingress: &IngressEnvelope, kind: EgressKind, created_at_ms: u64) -> Self {
        let mut envelope = Self::new(IoTarget::reply_to(ingress), kind, created_at_ms);
        envelope.source_ingress_id = Some(ingress.id.clone());
        envelope
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DeliveryReceipt {
    pub egress_id: String,
    pub delivered: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl DeliveryReceipt {
    pub fn delivered(egress: &EgressEnvelope, external_message_id: impl Into<String>) -> Self {
        Self {
            egress_id: egress.id.clone(),
            delivered: true,
            external_message_id: Some(external_message_id.into()),
            error: None,
            metadata: std::collections::BTreeMap::new(),
        }
    }

    pub fn failed(egress: &EgressEnvelope, error: impl Into<String>) -> Self {
        Self {
            egress_id: egress.id.clone(),
            delivered: false,
            external_message_id: None,
            error: Some(error.into()),
            metadata: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IoProtocolCapabilities {
    #[serde(default)]
    pub ingress: bool,
    #[serde(default)]
    pub egress: bool,
    #[serde(default)]
    pub streaming: bool,
    #[serde(default)]
    pub durable_offsets: bool,
    #[serde(default)]
    pub attachments: bool,
}

pub trait IoProtocolAdapter: Send + Sync {
    fn kind(&self) -> &'static str;

    fn capabilities(&self) -> IoProtocolCapabilities;
}

#[async_trait::async_trait]
pub trait IngressSink: Send + Sync {
    async fn submit(&self, envelope: IngressEnvelope) -> IoResult<IngressAck>;
}

/// Controls whether an ingress adapter must persist events before the kernel
/// sees them.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    strum::IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum IngressPersistenceMode {
    /// Persist inbound envelopes through an [`IngressQueueStore`] before a
    /// worker resolves/admits them into the runtime.
    #[default]
    DurableQueue,
    /// Let the adapter/daemon submit directly to the resolver/admission path.
    /// This is useful for local development, but in-flight events can be lost
    /// on restart or upgrade.
    BestEffortDirect,
}

impl IngressPersistenceMode {
    pub fn requires_queue(self) -> bool {
        matches!(self, Self::DurableQueue)
    }

    pub fn can_lose_inflight_on_restart(self) -> bool {
        matches!(self, Self::BestEffortDirect)
    }
}

/// Operator-facing persistence settings for an ingress route.
///
/// The daemon should default to [`IngressPersistenceMode::DurableQueue`] for
/// webhook and managed-service ingress. Local/dev routes can opt into
/// [`IngressPersistenceMode::BestEffortDirect`] when losing in-flight messages
/// is acceptable and avoiding queue storage growth matters more.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IngressPersistenceConfig {
    #[serde(default)]
    pub mode: IngressPersistenceMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_name: Option<String>,
    #[serde(default = "default_visibility_timeout_secs")]
    pub visibility_timeout_secs: u32,
}

impl Default for IngressPersistenceConfig {
    fn default() -> Self {
        Self {
            mode: IngressPersistenceMode::DurableQueue,
            queue_name: None,
            visibility_timeout_secs: default_visibility_timeout_secs(),
        }
    }
}

impl IngressPersistenceConfig {
    pub fn durable_queue(queue_name: impl Into<String>) -> Self {
        Self {
            mode: IngressPersistenceMode::DurableQueue,
            queue_name: Some(queue_name.into()),
            visibility_timeout_secs: default_visibility_timeout_secs(),
        }
    }

    pub fn best_effort_direct() -> Self {
        Self {
            mode: IngressPersistenceMode::BestEffortDirect,
            queue_name: None,
            visibility_timeout_secs: default_visibility_timeout_secs(),
        }
    }

    pub fn with_visibility_timeout_secs(mut self, seconds: u32) -> Self {
        self.visibility_timeout_secs = seconds;
        self
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LeasedIngressEnvelope {
    pub message_id: String,
    pub envelope: IngressEnvelope,
    #[serde(default)]
    pub attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_owner: Option<String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl LeasedIngressEnvelope {
    pub fn new(message_id: impl Into<String>, envelope: IngressEnvelope) -> Self {
        Self {
            message_id: message_id.into(),
            envelope,
            attempt: 0,
            lease_owner: None,
            metadata: std::collections::BTreeMap::new(),
        }
    }
}

#[async_trait::async_trait]
pub trait IngressQueueStore: IngressSink {
    async fn lease_ingress(
        &self,
        worker_id: &str,
        max_messages: usize,
        visibility_timeout_secs: u32,
    ) -> IoResult<Vec<LeasedIngressEnvelope>>;

    async fn complete_ingress(&self, message_id: &str) -> IoResult<()>;

    async fn hold_ingress_until(&self, message_id: &str, visible_at_ms: u64) -> IoResult<()>;

    async fn retry_ingress(&self, message_id: &str, reason: &str) -> IoResult<()>;
}

#[async_trait::async_trait]
pub trait IngressAdapter: IoProtocolAdapter {
    async fn start(&self, sink: &dyn IngressSink) -> IoResult<()>;
}

#[async_trait::async_trait]
pub trait EgressAdapter: IoProtocolAdapter {
    async fn deliver(&self, envelope: EgressEnvelope) -> IoResult<DeliveryReceipt>;
}

#[async_trait::async_trait]
pub trait IoResolver: Send + Sync {
    async fn resolve(&self, envelope: &IngressEnvelope) -> IoResult<ResolvedIoTarget>;
}

#[async_trait::async_trait]
pub trait AdmissionPolicy: Send + Sync {
    async fn decide(
        &self,
        envelope: &IngressEnvelope,
        target: &ResolvedIoTarget,
        state: &IngressState,
    ) -> IoResult<AdmissionDecision>;
}

#[async_trait::async_trait]
pub trait KernelIoBridge: Send + Sync {
    async fn apply(
        &self,
        envelope: &IngressEnvelope,
        target: &ResolvedIoTarget,
        decision: &AdmissionDecision,
    ) -> IoResult<KernelIoReceipt>;
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct KernelIoReceipt {
    pub envelope_id: String,
    pub target: ResolvedIoTarget,
    pub mode: AdmissionMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    /// Attribution of the admitted outcome (ADR 0007 D5), copied from the
    /// validated envelope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<IoPrincipal>,
}

impl KernelIoReceipt {
    pub fn new(
        envelope: &IngressEnvelope,
        target: ResolvedIoTarget,
        decision: &AdmissionDecision,
    ) -> Self {
        let turn_id = match decision {
            AdmissionDecision::Queue { turn_id, .. } | AdmissionDecision::Steer { turn_id, .. } => {
                Some(turn_id.clone())
            }
            AdmissionDecision::Interrupt {
                replacement_turn_id,
                ..
            } => replacement_turn_id.clone(),
            AdmissionDecision::Fork { child_key, .. } => Some(child_key.clone()),
            AdmissionDecision::ObserveOnly { .. } | AdmissionDecision::Reject { .. } => None,
        };
        Self {
            envelope_id: envelope.id.clone(),
            thread_id: target.address.thread_id.clone(),
            target,
            mode: decision.mode(),
            turn_id,
            principal: envelope.principal.clone(),
        }
    }
}

fn new_io_id(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::now_v7().simple())
}

fn default_visibility_timeout_secs() -> u32 {
    30
}

#[cfg(test)]
mod tests {

    /// `AsRef<str>` names the kind for egress bookkeeping. `PlatformAction`
    /// carries a field the derive cannot interpolate; callers append it.
    #[test]
    fn egress_kind_names_are_pinned() {
        let kinds = [
            crate::EgressKind::AssistantDelta {
                text: String::new(),
            },
            crate::EgressKind::AssistantMessage {
                text: String::new(),
            },
            crate::EgressKind::Status {
                text: String::new(),
            },
            crate::EgressKind::ToolStarted {
                name: String::new(),
            },
            crate::EgressKind::ToolCompleted {
                name: String::new(),
                success: true,
            },
            crate::EgressKind::Error {
                message: String::new(),
            },
            crate::EgressKind::Silence { reason: None },
            crate::EgressKind::PlatformAction {
                action: String::new(),
                payload: serde_json::Value::Null,
            },
        ];
        let names: Vec<&str> = kinds.iter().map(|kind| kind.as_ref()).collect();
        assert_eq!(
            names,
            vec![
                "assistant_delta",
                "assistant_message",
                "status",
                "tool_started",
                "tool_completed",
                "error",
                "silence",
                "platform_action",
            ]
        );
    }

    #[test]
    fn ingress_persistence_mode_names_are_pinned() {
        let durable: &'static str = crate::IngressPersistenceMode::DurableQueue.into();
        let best_effort: &'static str = crate::IngressPersistenceMode::BestEffortDirect.into();
        assert_eq!(durable, "durable_queue");
        assert_eq!(best_effort, "best_effort_direct");
    }

    fn telegram_like_envelope() -> crate::IngressEnvelope {
        let source = crate::IoSource::new("telegram.bot", "main");
        crate::IngressEnvelope::new(
            source.clone(),
            crate::IoConversation::new("telegram:chat:123", crate::ConversationKind::Direct),
            crate::IngressContent::text("hello from telegram"),
            1_777_000_000_000,
        )
        .with_actor(crate::IoActor::new("telegram:user:42").with_display_name("Ada"))
        .with_dedupe_key(crate::IoDedupeKey::for_source(&source, "update:999"))
    }

    fn attributed_envelope() -> crate::IngressEnvelope {
        telegram_like_envelope()
            .with_delivery(crate::IoDelivery::new("update:999").with_attempt(2))
            .with_principal(crate::IoPrincipal::new("tenant-a", "user-a", "route:main"))
    }

    fn attributed_target(tenant_id: &str) -> crate::ResolvedIoTarget {
        crate::ResolvedIoTarget::new(crate::ThreadAddress::new(tenant_id, "user-a", "session-a"))
    }

    #[test]
    fn effective_dedupe_prefers_explicit_key_and_derives_when_absent() {
        let explicit = attributed_envelope();
        assert_eq!(
            explicit
                .effective_dedupe_key()
                .as_ref()
                .map(crate::IoDedupeKey::stable_key),
            Some("telegram.bot:main:update:999".to_string())
        );

        let mut derived = explicit;
        derived.dedupe_key = None;
        assert_eq!(
            derived
                .effective_dedupe_key()
                .as_ref()
                .map(crate::IoDedupeKey::stable_key),
            Some("telegram.bot:main:update:999".to_string())
        );
    }

    #[test]
    fn witnessed_validation_pins_missing_and_empty_delivery_errors() {
        let legacy = telegram_like_envelope();
        assert!(matches!(
            legacy.require_witnessed(),
            Err(crate::IoError::InvalidEnvelope(message)) if message == "delivery is required"
        ));

        let empty = legacy.with_delivery(crate::IoDelivery::new(""));
        assert!(matches!(
            empty.require_witnessed(),
            Err(crate::IoError::InvalidEnvelope(message))
                if message == "delivery.delivery_id cannot be empty"
        ));

        let source = crate::IoSource::new("external.test", "main");
        let witnessed = crate::IngressEnvelope::new(
            source,
            crate::IoConversation::new("conversation", crate::ConversationKind::Direct),
            crate::IngressContent::text("hello"),
            1,
        )
        .with_delivery(crate::IoDelivery::new("delivery-1"));
        assert!(witnessed.require_witnessed().is_ok());
    }

    #[test]
    fn attributed_validation_pins_absence_and_tenant_mismatch_errors() {
        let target = attributed_target("tenant-a");
        let unattributed =
            telegram_like_envelope().with_delivery(crate::IoDelivery::new("update:999"));
        assert!(matches!(
            unattributed.require_attributed(&target),
            Err(crate::IoError::InvalidEnvelope(message)) if message == "principal is required"
        ));

        let mismatched = attributed_envelope();
        let target = attributed_target("tenant-b");
        assert!(matches!(
            mismatched.require_attributed(&target),
            Err(crate::IoError::InvalidEnvelope(message))
                if message
                    == "principal tenant \"tenant-a\" does not match resolved target tenant \"tenant-b\""
        ));

        assert!(
            attributed_envelope()
                .require_attributed(&attributed_target("tenant-a"))
                .is_ok()
        );
    }

    #[test]
    fn telegram_like_envelope_has_stable_dedupe_and_text_projection() {
        let envelope = telegram_like_envelope();

        assert_eq!(envelope.content.text_projection(), "hello from telegram");
        assert_eq!(
            envelope
                .dedupe_key
                .as_ref()
                .map(crate::IoDedupeKey::stable_key),
            Some("telegram.bot:main:update:999".to_string())
        );
        assert_eq!(envelope.conversation.stable_key(), "telegram:chat:123");
    }

    #[test]
    fn ingress_envelope_round_trips_camel_free_stable_shape() {
        let envelope = telegram_like_envelope();

        let value = serde_json::to_value(&envelope).unwrap();
        assert_eq!(value["source"]["protocol"], "telegram.bot");
        assert_eq!(value["conversation"]["kind"], "direct");
        assert_eq!(value["content"]["type"], "text");
        assert_eq!(value["dedupe_key"]["scope"], "telegram.bot:main");

        let roundtrip: crate::IngressEnvelope = serde_json::from_value(value).unwrap();
        assert_eq!(roundtrip, envelope);
    }

    #[test]
    fn pre_contract_and_attributed_envelope_shapes_both_round_trip() {
        let legacy_value = serde_json::to_value(telegram_like_envelope()).unwrap();
        assert!(legacy_value.get("delivery").is_none());
        assert!(legacy_value.get("principal").is_none());
        let legacy: crate::IngressEnvelope = serde_json::from_value(legacy_value).unwrap();
        assert_eq!(legacy.delivery, None);
        assert_eq!(legacy.principal, None);

        let attributed = attributed_envelope();
        let value = serde_json::to_value(&attributed).unwrap();
        assert_eq!(value["delivery"]["delivery_id"], "update:999");
        assert_eq!(value["delivery"]["attempt"], 2);
        assert_eq!(value["principal"]["tenant_id"], "tenant-a");
        assert_eq!(value["principal"]["principal_id"], "user-a");
        assert_eq!(value["principal"]["via"], "route:main");
        let roundtrip: crate::IngressEnvelope = serde_json::from_value(value).unwrap();
        assert_eq!(roundtrip, attributed);
    }

    #[test]
    fn admission_decision_names_queue_steer_interrupt_modes() {
        let input = crate::IoTurnInput::text("hello");
        assert_eq!(
            crate::AdmissionDecision::queue("turn-1", input.clone()).mode(),
            crate::AdmissionMode::Queue
        );
        assert_eq!(
            crate::AdmissionDecision::steer("turn-2", Some("turn-1".to_string()), input.clone())
                .mode(),
            crate::AdmissionMode::Steer
        );
        assert_eq!(
            crate::AdmissionDecision::Interrupt {
                reason: "replace active turn".to_string(),
                replacement_turn_id: Some("turn-3".to_string()),
                replacement: Some(input),
            }
            .mode(),
            crate::AdmissionMode::Interrupt
        );
    }

    #[test]
    fn target_and_turn_input_keep_provider_policy_protocol_neutral() {
        let envelope = telegram_like_envelope().with_metadata("external_message_id", "555");
        let target = crate::ResolvedIoTarget::new(crate::ThreadAddress::new(
            "local",
            "telegram-user-42",
            "telegram-chat-123",
        ))
        .with_provider_policy(crate::ProviderPolicy::new("bifrost", "openai/gpt-5.5"));

        let input = crate::IoTurnInput::from_envelope(&envelope, &target);

        assert_eq!(
            target.address.scope_key(),
            "local:telegram-user-42:telegram-chat-123"
        );
        assert_eq!(input.text, "hello from telegram");
        assert_eq!(
            input.provider_policy,
            Some(crate::ProviderPolicy::new("bifrost", "openai/gpt-5.5"))
        );
        assert_eq!(
            input
                .metadata
                .get("external_message_id")
                .map(String::as_str),
            Some("555")
        );
    }

    #[test]
    fn egress_envelope_replies_to_source_conversation() {
        let ingress = telegram_like_envelope();
        let egress = crate::EgressEnvelope::for_ingress(
            &ingress,
            crate::EgressKind::AssistantMessage {
                text: "hello back".to_string(),
            },
            1_777_000_000_123,
        );

        assert_eq!(
            egress.source_ingress_id.as_deref(),
            Some(ingress.id.as_str())
        );
        assert_eq!(
            egress.target.conversation.external_conversation_id,
            "telegram:chat:123"
        );
        assert_eq!(egress.kind.visible_text(), Some("hello back"));
    }

    #[test]
    fn egress_kind_round_trips_platform_action_and_silence() {
        let action = crate::EgressKind::PlatformAction {
            action: "reaction".to_string(),
            payload: serde_json::json!({
                "message_id": 555,
                "emoji": "👍",
            }),
        };
        let value = serde_json::to_value(&action).unwrap();
        assert_eq!(value["type"], "platform_action");
        assert_eq!(value["action"], "reaction");
        assert_eq!(value["payload"]["emoji"], "👍");

        let roundtrip: crate::EgressKind = serde_json::from_value(value).unwrap();
        assert_eq!(roundtrip, action);
        assert_eq!(roundtrip.visible_text(), None);

        let silence = crate::EgressKind::Silence {
            reason: Some("agent_declined".to_string()),
        };
        let value = serde_json::to_value(&silence).unwrap();
        assert_eq!(value["type"], "silence");
        assert_eq!(value["reason"], "agent_declined");
        assert_eq!(silence.visible_text(), None);
    }

    #[test]
    fn ingress_persistence_config_makes_lossy_direct_mode_explicit() {
        let durable = crate::IngressPersistenceConfig::durable_queue("verlet-ingress")
            .with_visibility_timeout_secs(45);
        assert!(durable.mode.requires_queue());
        assert!(!durable.mode.can_lose_inflight_on_restart());
        assert_eq!(durable.queue_name.as_deref(), Some("verlet-ingress"));
        assert_eq!(durable.visibility_timeout_secs, 45);

        let direct = crate::IngressPersistenceConfig::best_effort_direct();
        assert!(!direct.mode.requires_queue());
        assert!(direct.mode.can_lose_inflight_on_restart());
        assert_eq!(direct.queue_name, None);

        let value = serde_json::to_value(&direct).unwrap();
        assert_eq!(value["mode"], "best_effort_direct");
    }

    #[test]
    fn kernel_receipt_extracts_submission_identity_from_decision() {
        let envelope = attributed_envelope();
        let target = crate::ResolvedIoTarget::new(
            crate::ThreadAddress::new("tenant-a", "user-a", "session").with_thread_id("thread-1"),
        );
        let decision = crate::AdmissionDecision::queue("turn-1", crate::IoTurnInput::text("hello"));

        let receipt = crate::KernelIoReceipt::new(&envelope, target, &decision);

        assert_eq!(receipt.envelope_id, envelope.id);
        assert_eq!(receipt.mode, crate::AdmissionMode::Queue);
        assert_eq!(receipt.thread_id.as_deref(), Some("thread-1"));
        assert_eq!(receipt.turn_id.as_deref(), Some("turn-1"));
        assert_eq!(receipt.principal, envelope.principal);
    }

    #[test]
    fn kernel_receipts_copy_principal_for_every_admitted_runtime_mode() {
        let envelope = attributed_envelope();
        let target = attributed_target("tenant-a");
        let input = crate::IoTurnInput::text("hello");
        let decisions = [
            crate::AdmissionDecision::queue("turn-queue", input.clone()),
            crate::AdmissionDecision::steer(
                "turn-steer",
                Some("active".to_string()),
                input.clone(),
            ),
            crate::AdmissionDecision::Interrupt {
                reason: "replace".to_string(),
                replacement_turn_id: Some("turn-interrupt".to_string()),
                replacement: Some(input.clone()),
            },
            crate::AdmissionDecision::Fork {
                child_key: "child-fork".to_string(),
                input,
            },
        ];

        for decision in decisions {
            let receipt = crate::KernelIoReceipt::new(&envelope, target.clone(), &decision);
            assert_eq!(
                receipt.principal,
                envelope.principal,
                "{:?}",
                decision.mode()
            );
        }
    }
}
