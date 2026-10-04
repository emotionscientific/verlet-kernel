//! Telegram protocol adapter pieces for Verlet IO.
//!
//! This crate owns the Telegram Bot API wire subset and maps it to
//! `verlet-io-core` envelopes. It intentionally stops before product policy:
//! a daemon or product adapter still decides tenant mapping, queueing, dedupe
//! persistence, and whether an inbound Telegram event queues, steers, or
//! interrupts a turn.

use serde::ser::SerializeStruct as _;

pub const TELEGRAM_PROTOCOL: &str = "telegram.bot";

#[derive(Debug, thiserror::Error)]
pub enum TelegramError {
    #[error("Telegram update {0} does not contain a supported inbound event")]
    UnsupportedUpdate(i64),
    #[error("Telegram egress target uses protocol {0:?}")]
    InvalidProtocol(String),
    #[error("Telegram egress target uses instance {target:?}, expected {expected:?}")]
    InvalidInstance { target: String, expected: String },
    #[error("Telegram target is missing chat id")]
    MissingChatId,
    #[error("invalid Telegram chat id {0:?}")]
    InvalidChatId(String),
    #[error("invalid Telegram message thread id {0:?}")]
    InvalidThreadId(String),
    #[error("Telegram egress has no visible text to deliver")]
    NoVisibleText,
    #[error("unknown Telegram platform action {0:?}")]
    UnknownPlatformAction(String),
    #[error("Telegram platform action {action:?} is missing or has invalid field {field:?}")]
    InvalidPlatformActionPayload { action: String, field: &'static str },
    #[error("Telegram API returned {status}: {body}")]
    Api {
        status: u16,
        body: String,
        /// Seconds from the `parameters.retry_after` Telegram sends with a 429.
        retry_after_secs: Option<u64>,
    },
    #[error("Telegram API response was missing result")]
    MissingApiResult,
    #[error("Telegram API transport failed: {0}")]
    Transport(String),
    #[error("Telegram API response decode failed: {0}")]
    Decode(String),
}

impl From<TelegramError> for verlet_io_core::IoError {
    fn from(value: TelegramError) -> Self {
        match value {
            TelegramError::UnsupportedUpdate(_)
            | TelegramError::InvalidProtocol(_)
            | TelegramError::InvalidInstance { .. }
            | TelegramError::MissingChatId
            | TelegramError::InvalidChatId(_)
            | TelegramError::InvalidThreadId(_) => {
                verlet_io_core::IoError::InvalidEnvelope(value.to_string())
            }
            // 429 is the only status that carries a wait; honor Telegram's own
            // figure rather than guessing with a backoff curve.
            TelegramError::Api {
                status: 429,
                retry_after_secs,
                ..
            } => verlet_io_core::IoError::RateLimited {
                retry_after_ms: retry_after_secs.map(|secs| secs.saturating_mul(1_000)),
                message: value.to_string(),
            },
            // 400 "chat not found", 401 bad token, 403 "bot was blocked by the
            // user": the same request will fail the same way every time, so
            // spending the remaining attempts on it only delays the dead
            // letter.
            TelegramError::Api {
                status: 400 | 401 | 403,
                ..
            } => verlet_io_core::IoError::PermanentDelivery(value.to_string()),
            TelegramError::NoVisibleText
            | TelegramError::UnknownPlatformAction(_)
            | TelegramError::InvalidPlatformActionPayload { .. }
            | TelegramError::Api { .. }
            | TelegramError::MissingApiResult
            | TelegramError::Transport(_)
            | TelegramError::Decode(_) => verlet_io_core::IoError::Delivery(value.to_string()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramNormalizer {
    instance_id: String,
}

impl TelegramNormalizer {
    pub fn new(instance_id: impl Into<String>) -> Self {
        Self {
            instance_id: instance_id.into(),
        }
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn source(&self) -> verlet_io_core::IoSource {
        verlet_io_core::IoSource::new(TELEGRAM_PROTOCOL, self.instance_id.clone())
    }

    pub fn normalize_update(
        &self,
        update: &TelegramUpdate,
        received_at_ms: u64,
    ) -> verlet_io_core::IoResult<Option<verlet_io_core::IngressEnvelope>> {
        if let Some(message) = update.message.as_ref() {
            return Ok(Some(self.envelope_from_message(
                update.update_id,
                "message",
                message,
                None,
                message_content(message),
                received_at_ms,
            )));
        }

        if let Some(message) = update.edited_message.as_ref() {
            return Ok(Some(self.envelope_from_message(
                update.update_id,
                "edited_message",
                message,
                None,
                message_content(message),
                received_at_ms,
            )));
        }

        if let Some(message) = update.channel_post.as_ref() {
            return Ok(Some(self.envelope_from_message(
                update.update_id,
                "channel_post",
                message,
                None,
                message_content(message),
                received_at_ms,
            )));
        }

        if let Some(message) = update.edited_channel_post.as_ref() {
            return Ok(Some(self.envelope_from_message(
                update.update_id,
                "edited_channel_post",
                message,
                None,
                message_content(message),
                received_at_ms,
            )));
        }

        if let Some(reaction) = update.message_reaction.as_ref() {
            return Ok(Some(self.envelope_from_message_reaction(
                update.update_id,
                reaction,
                received_at_ms,
            )));
        }

        if let Some(callback) = update.callback_query.as_ref() {
            let Some(message) = callback.message.as_ref() else {
                return Ok(None);
            };
            return Ok(Some(self.envelope_from_message(
                update.update_id,
                "callback_query",
                message,
                Some(actor_from_user(&callback.from)),
                verlet_io_core::IngressContent::Event {
                    kind: "telegram.callback_query".to_string(),
                    payload: serde_json::json!({
                        "id": callback.id,
                        "data": callback.data,
                    }),
                },
                received_at_ms,
            )));
        }

        Ok(None)
    }

    fn envelope_from_message(
        &self,
        update_id: i64,
        update_kind: &'static str,
        message: &TelegramMessage,
        actor: Option<verlet_io_core::IoActor>,
        content: verlet_io_core::IngressContent,
        received_at_ms: u64,
    ) -> verlet_io_core::IngressEnvelope {
        let source = self.source();
        let mut envelope = verlet_io_core::IngressEnvelope::new(
            source.clone(),
            conversation_from_chat(&message.chat, message.message_thread_id),
            content,
            received_at_ms,
        )
        .with_dedupe_key(verlet_io_core::IoDedupeKey::for_source(
            &source,
            format!("update:{update_id}"),
        ))
        .with_delivery(verlet_io_core::IoDelivery::new(format!(
            "update:{update_id}"
        )))
        .with_metadata("telegram_update_id", update_id.to_string())
        .with_metadata("telegram_update_kind", update_kind)
        .with_metadata("telegram_message_id", message.message_id.to_string())
        .with_metadata("telegram_message_date", message.date.to_string());

        envelope.actor = actor.or_else(|| actor_from_message(message));
        envelope.attachments = message_attachments(message);
        envelope
    }

    fn envelope_from_message_reaction(
        &self,
        update_id: i64,
        reaction: &TelegramMessageReactionUpdated,
        received_at_ms: u64,
    ) -> verlet_io_core::IngressEnvelope {
        let source = self.source();
        let mut envelope = verlet_io_core::IngressEnvelope::new(
            source.clone(),
            conversation_from_chat(&reaction.chat, None),
            verlet_io_core::IngressContent::Event {
                kind: "telegram.message_reaction".to_string(),
                payload: serde_json::json!({
                    "message_id": reaction.message_id,
                    "old_reaction": reaction.old_reaction,
                    "new_reaction": reaction.new_reaction,
                }),
            },
            received_at_ms,
        )
        .with_dedupe_key(verlet_io_core::IoDedupeKey::for_source(
            &source,
            format!("update:{update_id}"),
        ))
        .with_delivery(verlet_io_core::IoDelivery::new(format!(
            "update:{update_id}"
        )))
        .with_metadata("telegram_update_id", update_id.to_string())
        .with_metadata("telegram_update_kind", "message_reaction")
        .with_metadata("telegram_message_id", reaction.message_id.to_string())
        .with_metadata("telegram_message_date", reaction.date.to_string());

        envelope.actor = reaction
            .user
            .as_ref()
            .map(actor_from_user)
            .or_else(|| reaction.actor_chat.as_ref().map(actor_from_chat));
        envelope
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramWebhookAdapter {
    normalizer: TelegramNormalizer,
}

impl TelegramWebhookAdapter {
    pub fn new(instance_id: impl Into<String>) -> Self {
        Self {
            normalizer: TelegramNormalizer::new(instance_id),
        }
    }

    pub fn normalizer(&self) -> &TelegramNormalizer {
        &self.normalizer
    }

    pub async fn submit_update(
        &self,
        sink: &dyn verlet_io_core::IngressSink,
        update: &TelegramUpdate,
        received_at_ms: u64,
    ) -> verlet_io_core::IoResult<Option<verlet_io_core::IngressAck>> {
        let Some(envelope) = self.normalizer.normalize_update(update, received_at_ms)? else {
            return Ok(None);
        };
        sink.submit(envelope).await.map(Some)
    }
}

impl verlet_io_core::IoProtocolAdapter for TelegramWebhookAdapter {
    fn kind(&self) -> &'static str {
        TELEGRAM_PROTOCOL
    }

    fn capabilities(&self) -> verlet_io_core::IoProtocolCapabilities {
        verlet_io_core::IoProtocolCapabilities {
            ingress: true,
            egress: false,
            streaming: false,
            durable_offsets: true,
            attachments: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TelegramEgressAdapter {
    instance_id: String,
    client: TelegramBotClient,
}

impl TelegramEgressAdapter {
    pub fn new(instance_id: impl Into<String>, bot_token: impl Into<String>) -> Self {
        Self {
            instance_id: instance_id.into(),
            client: TelegramBotClient::new(bot_token),
        }
    }

    pub fn with_client(instance_id: impl Into<String>, client: TelegramBotClient) -> Self {
        Self {
            instance_id: instance_id.into(),
            client,
        }
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn build_send_message(
        &self,
        envelope: &verlet_io_core::EgressEnvelope,
    ) -> verlet_io_core::IoResult<Option<TelegramSendMessageRequest>> {
        self.validate_target(&envelope.target)?;
        build_send_message_request(envelope)
    }

    pub fn build_platform_action(
        &self,
        envelope: &verlet_io_core::EgressEnvelope,
    ) -> verlet_io_core::IoResult<Option<TelegramPlatformActionRequest>> {
        self.validate_target(&envelope.target)?;
        build_platform_action_request(envelope)
    }

    fn validate_target(&self, target: &verlet_io_core::IoTarget) -> verlet_io_core::IoResult<()> {
        if target.source.protocol != TELEGRAM_PROTOCOL {
            return Err(TelegramError::InvalidProtocol(target.source.protocol.clone()).into());
        }
        if target.source.instance_id != self.instance_id {
            return Err(TelegramError::InvalidInstance {
                target: target.source.instance_id.clone(),
                expected: self.instance_id.clone(),
            }
            .into());
        }
        Ok(())
    }
}

impl verlet_io_core::IoProtocolAdapter for TelegramEgressAdapter {
    fn kind(&self) -> &'static str {
        TELEGRAM_PROTOCOL
    }

    fn capabilities(&self) -> verlet_io_core::IoProtocolCapabilities {
        verlet_io_core::IoProtocolCapabilities {
            ingress: false,
            egress: true,
            streaming: false,
            durable_offsets: false,
            attachments: false,
        }
    }
}

#[async_trait::async_trait]
impl verlet_io_core::EgressAdapter for TelegramEgressAdapter {
    async fn deliver(
        &self,
        envelope: verlet_io_core::EgressEnvelope,
    ) -> verlet_io_core::IoResult<verlet_io_core::DeliveryReceipt> {
        self.validate_target(&envelope.target)?;
        match &envelope.kind {
            verlet_io_core::EgressKind::PlatformAction { .. } => {
                let Some(request) = build_platform_action_request(&envelope)? else {
                    return Ok(suppressed_receipt(&envelope, "no_platform_action"));
                };
                return match request {
                    TelegramPlatformActionRequest::SendChatAction(request) => {
                        self.client.send_chat_action(&request).await?;
                        Ok(action_receipt(&envelope, "typing"))
                    }
                    TelegramPlatformActionRequest::SetMessageReaction(request) => {
                        self.client.set_message_reaction(&request).await?;
                        Ok(action_receipt(&envelope, "reaction"))
                    }
                    TelegramPlatformActionRequest::SendSticker(request) => {
                        let message = self.client.send_sticker(&request).await?;
                        Ok(verlet_io_core::DeliveryReceipt::delivered(
                            &envelope,
                            message.message_id.to_string(),
                        ))
                    }
                };
            }
            verlet_io_core::EgressKind::Silence { .. } => {
                return Ok(suppressed_receipt(&envelope, "silence"));
            }
            _ => {}
        }
        let Some(request) = build_send_message_request(&envelope)? else {
            return Ok(suppressed_receipt(&envelope, "no_visible_text"));
        };

        let message = self.client.send_message(&request).await?;
        Ok(verlet_io_core::DeliveryReceipt::delivered(
            &envelope,
            message.message_id.to_string(),
        ))
    }
}

#[derive(Clone, Debug)]
pub struct TelegramBotClient {
    token: String,
    api_base: String,
    http: reqwest::Client,
}

impl TelegramBotClient {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            api_base: "https://api.telegram.org".to_string(),
            http: reqwest::Client::new(),
        }
    }

    pub fn with_api_base(mut self, api_base: impl Into<String>) -> Self {
        self.api_base = api_base.into();
        self
    }

    pub async fn send_message(
        &self,
        request: &TelegramSendMessageRequest,
    ) -> verlet_io_core::IoResult<TelegramMessage> {
        self.post_api("sendMessage", request).await
    }

    pub async fn send_chat_action(
        &self,
        request: &TelegramSendChatActionRequest,
    ) -> verlet_io_core::IoResult<()> {
        let delivered: bool = self.post_api("sendChatAction", request).await?;
        if delivered {
            Ok(())
        } else {
            Err(TelegramError::MissingApiResult.into())
        }
    }

    pub async fn set_message_reaction(
        &self,
        request: &TelegramSetMessageReactionRequest,
    ) -> verlet_io_core::IoResult<()> {
        let delivered: bool = self.post_api("setMessageReaction", request).await?;
        if delivered {
            Ok(())
        } else {
            Err(TelegramError::MissingApiResult.into())
        }
    }

    pub async fn send_sticker(
        &self,
        request: &TelegramSendStickerRequest,
    ) -> verlet_io_core::IoResult<TelegramMessage> {
        self.post_api("sendSticker", request).await
    }

    async fn post_api<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        request: &impl serde::Serialize,
    ) -> verlet_io_core::IoResult<T> {
        let url = format!(
            "{}/bot{}/{}",
            self.api_base.trim_end_matches('/'),
            self.token,
            method
        );
        let response = self
            .http
            .post(url)
            .json(request)
            .send()
            .await
            .map_err(|err| TelegramError::Transport(sanitize_reqwest_error(err)))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| TelegramError::Transport(sanitize_reqwest_error(err)))?;

        if !status.is_success() {
            // A 429 carries its retry_after in the JSON body, so decode the
            // body opportunistically here too; a body that will not parse
            // simply yields no hint and falls back to the backoff curve.
            let retry_after_secs =
                serde_json::from_str::<TelegramApiResponse<serde_json::Value>>(&body)
                    .ok()
                    .and_then(|decoded| decoded.parameters)
                    .and_then(|parameters| parameters.retry_after);
            return Err(TelegramError::Api {
                status: status.as_u16(),
                body: truncate_body(&body),
                retry_after_secs,
            }
            .into());
        }

        let decoded: TelegramApiResponse<T> =
            serde_json::from_str(&body).map_err(|err| TelegramError::Decode(err.to_string()))?;
        if !decoded.ok {
            return Err(TelegramError::Api {
                status: decoded.error_code.unwrap_or(status.as_u16() as i64) as u16,
                body: decoded.description.unwrap_or_else(|| "not ok".to_string()),
                retry_after_secs: decoded
                    .parameters
                    .and_then(|parameters| parameters.retry_after),
            }
            .into());
        }
        decoded.result.ok_or(TelegramError::MissingApiResult.into())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TelegramUpdate {
    pub update_id: i64,
    #[serde(default)]
    pub message: Option<TelegramMessage>,
    #[serde(default)]
    pub edited_message: Option<TelegramMessage>,
    #[serde(default)]
    pub channel_post: Option<TelegramMessage>,
    #[serde(default)]
    pub edited_channel_post: Option<TelegramMessage>,
    #[serde(default)]
    pub message_reaction: Option<TelegramMessageReactionUpdated>,
    #[serde(default)]
    pub callback_query: Option<TelegramCallbackQuery>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TelegramMessageReactionUpdated {
    pub chat: TelegramChat,
    pub message_id: i64,
    #[serde(default)]
    pub user: Option<TelegramUser>,
    #[serde(default)]
    pub actor_chat: Option<TelegramChat>,
    pub date: i64,
    #[serde(default)]
    pub old_reaction: Vec<TelegramReactionType>,
    #[serde(default)]
    pub new_reaction: Vec<TelegramReactionType>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TelegramCallbackQuery {
    pub id: String,
    pub from: TelegramUser,
    #[serde(default)]
    pub message: Option<TelegramMessage>,
    #[serde(default)]
    pub data: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TelegramMessage {
    pub message_id: i64,
    #[serde(default)]
    pub message_thread_id: Option<i64>,
    #[serde(default)]
    pub from: Option<TelegramUser>,
    #[serde(default)]
    pub sender_chat: Option<TelegramChat>,
    pub chat: TelegramChat,
    pub date: i64,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub caption: Option<String>,
    #[serde(default)]
    pub document: Option<TelegramDocument>,
    #[serde(default)]
    pub photo: Vec<TelegramPhotoSize>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TelegramUser {
    pub id: i64,
    #[serde(default)]
    pub is_bot: bool,
    pub first_name: String,
    #[serde(default)]
    pub last_name: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub language_code: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TelegramChat {
    pub id: i64,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub first_name: Option<String>,
    #[serde(default)]
    pub last_name: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TelegramDocument {
    pub file_id: String,
    #[serde(default)]
    pub file_unique_id: Option<String>,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub file_size: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TelegramPhotoSize {
    pub file_id: String,
    #[serde(default)]
    pub file_unique_id: Option<String>,
    pub width: u32,
    pub height: u32,
    #[serde(default)]
    pub file_size: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct TelegramSendMessageRequest {
    pub chat_id: TelegramChatId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_thread_id: Option<i64>,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_to_message_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disable_notification: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TelegramPlatformActionRequest {
    SendChatAction(TelegramSendChatActionRequest),
    SetMessageReaction(TelegramSetMessageReactionRequest),
    SendSticker(TelegramSendStickerRequest),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct TelegramSendChatActionRequest {
    pub chat_id: TelegramChatId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_thread_id: Option<i64>,
    pub action: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct TelegramSetMessageReactionRequest {
    pub chat_id: TelegramChatId,
    pub message_id: i64,
    pub reaction: Vec<TelegramReactionType>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct TelegramSendStickerRequest {
    pub chat_id: TelegramChatId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_thread_id: Option<i64>,
    pub sticker: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disable_notification: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TelegramReactionType {
    Emoji { emoji: String },
    CustomEmoji { custom_emoji_id: String },
    Other(serde_json::Value),
}

impl serde::Serialize for TelegramReactionType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Emoji { emoji } => {
                let mut state = serializer.serialize_struct("TelegramReactionType", 2)?;
                state.serialize_field("type", "emoji")?;
                state.serialize_field("emoji", emoji)?;
                state.end()
            }
            Self::CustomEmoji { custom_emoji_id } => {
                let mut state = serializer.serialize_struct("TelegramReactionType", 2)?;
                state.serialize_field("type", "custom_emoji")?;
                state.serialize_field("custom_emoji_id", custom_emoji_id)?;
                state.end()
            }
            Self::Other(value) => value.serialize(serializer),
        }
    }
}

impl<'de> serde::Deserialize<'de> for TelegramReactionType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if let Some(object) = value.as_object() {
            match object.get("type").and_then(serde_json::Value::as_str) {
                Some("emoji") if object.len() == 2 => {
                    if let Some(emoji) = object.get("emoji").and_then(serde_json::Value::as_str) {
                        return Ok(Self::Emoji {
                            emoji: emoji.to_string(),
                        });
                    }
                }
                Some("custom_emoji") if object.len() == 2 => {
                    if let Some(custom_emoji_id) = object
                        .get("custom_emoji_id")
                        .and_then(serde_json::Value::as_str)
                    {
                        return Ok(Self::CustomEmoji {
                            custom_emoji_id: custom_emoji_id.to_string(),
                        });
                    }
                }
                _ => {}
            }
        }
        Ok(Self::Other(value))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(untagged)]
pub enum TelegramChatId {
    Id(i64),
    Username(String),
}

#[derive(Debug, serde::Deserialize)]
struct TelegramApiResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
    error_code: Option<i64>,
    parameters: Option<TelegramResponseParameters>,
}

/// The `parameters` object Telegram attaches to some errors. Only
/// `retry_after` is read: it is how a 429 says how long to wait, and
/// discarding it means retrying early and extending the limit.
#[derive(Debug, serde::Deserialize)]
struct TelegramResponseParameters {
    retry_after: Option<u64>,
}

pub fn build_send_message_request(
    envelope: &verlet_io_core::EgressEnvelope,
) -> verlet_io_core::IoResult<Option<TelegramSendMessageRequest>> {
    let Some(text) = envelope.kind.visible_text() else {
        return Ok(None);
    };
    if text.is_empty() {
        return Ok(None);
    }

    Ok(Some(TelegramSendMessageRequest {
        chat_id: parse_chat_id(&envelope.target.conversation.external_conversation_id)?,
        message_thread_id: envelope
            .target
            .conversation
            .external_thread_id
            .as_deref()
            .map(parse_thread_id)
            .transpose()?,
        text: text.to_string(),
        reply_to_message_id: envelope
            .target
            .metadata
            .get("telegram_reply_to_message_id")
            .map(|value| parse_i64(value, TelegramError::InvalidThreadId(value.clone())))
            .transpose()?,
        disable_notification: envelope
            .target
            .metadata
            .get("telegram_disable_notification")
            .and_then(|value| value.parse::<bool>().ok()),
    }))
}

pub fn build_platform_action_request(
    envelope: &verlet_io_core::EgressEnvelope,
) -> verlet_io_core::IoResult<Option<TelegramPlatformActionRequest>> {
    let verlet_io_core::EgressKind::PlatformAction { action, payload } = &envelope.kind else {
        return Ok(None);
    };

    match action.as_str() {
        "typing" => Ok(Some(TelegramPlatformActionRequest::SendChatAction(
            TelegramSendChatActionRequest {
                chat_id: parse_chat_id(&envelope.target.conversation.external_conversation_id)?,
                message_thread_id: envelope
                    .target
                    .conversation
                    .external_thread_id
                    .as_deref()
                    .map(parse_thread_id)
                    .transpose()?,
                action: "typing".to_string(),
            },
        ))),
        "reaction" => {
            let message_id = payload_i64(payload, action, "message_id")
                .or_else(|_| metadata_i64(&envelope.metadata, "telegram_message_id"))
                .or_else(|_| {
                    metadata_i64(&envelope.target.metadata, "telegram_reply_to_message_id")
                })?;
            let emoji = payload_string(payload, action, "emoji")?;
            Ok(Some(TelegramPlatformActionRequest::SetMessageReaction(
                TelegramSetMessageReactionRequest {
                    chat_id: parse_chat_id(&envelope.target.conversation.external_conversation_id)?,
                    message_id,
                    reaction: vec![TelegramReactionType::Emoji { emoji }],
                },
            )))
        }
        "sticker" => Ok(Some(TelegramPlatformActionRequest::SendSticker(
            TelegramSendStickerRequest {
                chat_id: parse_chat_id(&envelope.target.conversation.external_conversation_id)?,
                message_thread_id: envelope
                    .target
                    .conversation
                    .external_thread_id
                    .as_deref()
                    .map(parse_thread_id)
                    .transpose()?,
                sticker: payload_string(payload, action, "file_id")?,
                disable_notification: envelope
                    .target
                    .metadata
                    .get("telegram_disable_notification")
                    .and_then(|value| value.parse::<bool>().ok()),
            },
        ))),
        other => Err(TelegramError::UnknownPlatformAction(other.to_string()).into()),
    }
}

pub fn target_from_message(
    instance_id: impl Into<String>,
    message: &TelegramMessage,
) -> verlet_io_core::IoTarget {
    verlet_io_core::IoTarget {
        source: verlet_io_core::IoSource::new(TELEGRAM_PROTOCOL, instance_id),
        conversation: conversation_from_chat(&message.chat, message.message_thread_id),
        actor: actor_from_message(message),
        metadata: std::collections::BTreeMap::new(),
    }
}

fn conversation_from_chat(
    chat: &TelegramChat,
    message_thread_id: Option<i64>,
) -> verlet_io_core::IoConversation {
    let kind = match chat.kind.as_str() {
        "private" => verlet_io_core::ConversationKind::Direct,
        "channel" => verlet_io_core::ConversationKind::Channel,
        "supergroup" | "group" => verlet_io_core::ConversationKind::Group,
        _ => verlet_io_core::ConversationKind::Group,
    };

    let mut conversation =
        verlet_io_core::IoConversation::new(format!("telegram:chat:{}", chat.id), kind)
            .with_title(chat_title(chat));

    if let Some(thread_id) = message_thread_id {
        conversation = conversation.with_external_thread_id(thread_id.to_string());
    }

    conversation
        .metadata
        .insert("telegram_chat_type".to_string(), chat.kind.clone());
    if let Some(username) = chat.username.as_ref() {
        conversation
            .metadata
            .insert("telegram_chat_username".to_string(), username.clone());
    }
    conversation
}

fn actor_from_message(message: &TelegramMessage) -> Option<verlet_io_core::IoActor> {
    message
        .from
        .as_ref()
        .map(actor_from_user)
        .or_else(|| message.sender_chat.as_ref().map(actor_from_chat))
}

fn actor_from_user(user: &TelegramUser) -> verlet_io_core::IoActor {
    let mut actor = verlet_io_core::IoActor::new(format!("telegram:user:{}", user.id));
    actor.display_name = Some(user_display_name(user));
    actor.is_bot = user.is_bot;
    if let Some(username) = user.username.as_ref() {
        actor
            .metadata
            .insert("telegram_username".to_string(), username.clone());
    }
    if let Some(language_code) = user.language_code.as_ref() {
        actor
            .metadata
            .insert("telegram_language_code".to_string(), language_code.clone());
    }
    actor
}

fn actor_from_chat(chat: &TelegramChat) -> verlet_io_core::IoActor {
    let mut actor = verlet_io_core::IoActor::new(format!("telegram:chat:{}", chat.id));
    actor.display_name = Some(chat_title(chat));
    actor
        .metadata
        .insert("telegram_chat_type".to_string(), chat.kind.clone());
    actor
}

fn message_content(message: &TelegramMessage) -> verlet_io_core::IngressContent {
    if let Some(text) = message.text.as_ref().filter(|text| !text.is_empty()) {
        return text_or_command(text);
    }
    if let Some(caption) = message
        .caption
        .as_ref()
        .filter(|caption| !caption.is_empty())
    {
        return text_or_command(caption);
    }

    verlet_io_core::IngressContent::Event {
        kind: "telegram.message".to_string(),
        payload: serde_json::json!({
            "message_id": message.message_id,
            "has_document": message.document.is_some(),
            "photo_count": message.photo.len(),
        }),
    }
}

fn text_or_command(text: &str) -> verlet_io_core::IngressContent {
    let Some(command) = text.strip_prefix('/') else {
        return verlet_io_core::IngressContent::text(text);
    };

    let mut parts = command.splitn(2, char::is_whitespace);
    let name = parts.next().unwrap_or_default().to_string();
    let args = parts
        .next()
        .map(str::trim)
        .filter(|args| !args.is_empty())
        .map(ToOwned::to_owned);

    if name.is_empty() {
        verlet_io_core::IngressContent::text(text)
    } else {
        verlet_io_core::IngressContent::Command { name, args }
    }
}

fn message_attachments(message: &TelegramMessage) -> Vec<verlet_io_core::IoAttachment> {
    let mut attachments = Vec::new();

    if let Some(document) = message.document.as_ref() {
        let mut attachment = verlet_io_core::IoAttachment::new(
            format!("telegram:file:{}", document.file_id),
            document
                .mime_type
                .clone()
                .unwrap_or_else(|| "application/octet-stream".to_string()),
        );
        attachment.name = document.file_name.clone();
        attachment.uri = Some(format!("telegram:file:{}", document.file_id));
        attachment.size_bytes = document.file_size;
        if let Some(file_unique_id) = document.file_unique_id.as_ref() {
            attachment.metadata.insert(
                "telegram_file_unique_id".to_string(),
                file_unique_id.clone(),
            );
        }
        attachments.push(attachment);
    }

    if let Some(photo) = message.photo.last() {
        let mut attachment = verlet_io_core::IoAttachment::new(
            format!("telegram:file:{}", photo.file_id),
            "image/jpeg",
        );
        attachment.uri = Some(format!("telegram:file:{}", photo.file_id));
        attachment.size_bytes = photo.file_size;
        attachment
            .metadata
            .insert("telegram_width".to_string(), photo.width.to_string());
        attachment
            .metadata
            .insert("telegram_height".to_string(), photo.height.to_string());
        if let Some(file_unique_id) = photo.file_unique_id.as_ref() {
            attachment.metadata.insert(
                "telegram_file_unique_id".to_string(),
                file_unique_id.clone(),
            );
        }
        attachments.push(attachment);
    }

    attachments
}

fn parse_chat_id(value: &str) -> verlet_io_core::IoResult<TelegramChatId> {
    let raw = value.strip_prefix("telegram:chat:").unwrap_or(value).trim();
    if raw.is_empty() {
        return Err(TelegramError::MissingChatId.into());
    }
    if let Ok(id) = raw.parse::<i64>() {
        return Ok(TelegramChatId::Id(id));
    }
    if raw.starts_with('@') {
        return Ok(TelegramChatId::Username(raw.to_string()));
    }
    Err(TelegramError::InvalidChatId(value.to_string()).into())
}

fn parse_thread_id(value: &str) -> verlet_io_core::IoResult<i64> {
    let raw = value.strip_prefix("telegram:topic:").unwrap_or(value);
    parse_i64(raw, TelegramError::InvalidThreadId(value.to_string()))
}

fn parse_i64(value: &str, err: TelegramError) -> verlet_io_core::IoResult<i64> {
    value.parse::<i64>().map_err(|_| err.into())
}

fn payload_string(
    payload: &serde_json::Value,
    action: &str,
    field: &'static str,
) -> verlet_io_core::IoResult<String> {
    payload
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| invalid_platform_action_field(action, field))
}

fn payload_i64(
    payload: &serde_json::Value,
    action: &str,
    field: &'static str,
) -> verlet_io_core::IoResult<i64> {
    if let Some(value) = payload.get(field).and_then(serde_json::Value::as_i64) {
        return Ok(value);
    }
    if let Some(value) = payload.get(field).and_then(serde_json::Value::as_str) {
        return value
            .parse::<i64>()
            .map_err(|_| invalid_platform_action_field(action, field));
    }
    Err(invalid_platform_action_field(action, field))
}

fn metadata_i64(
    metadata: &std::collections::BTreeMap<String, String>,
    key: &'static str,
) -> verlet_io_core::IoResult<i64> {
    metadata
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| invalid_platform_action_field("reaction", "message_id"))?
        .parse::<i64>()
        .map_err(|_| invalid_platform_action_field("reaction", key))
}

fn invalid_platform_action_field(action: &str, field: &'static str) -> verlet_io_core::IoError {
    TelegramError::InvalidPlatformActionPayload {
        action: action.to_string(),
        field,
    }
    .into()
}

fn chat_title(chat: &TelegramChat) -> String {
    chat.title
        .clone()
        .or_else(|| {
            chat.username
                .as_ref()
                .map(|username| format!("@{username}"))
        })
        .or_else(|| match (&chat.first_name, &chat.last_name) {
            (Some(first), Some(last)) => Some(format!("{first} {last}")),
            (Some(first), None) => Some(first.clone()),
            (None, Some(last)) => Some(last.clone()),
            (None, None) => None,
        })
        .unwrap_or_else(|| format!("Telegram chat {}", chat.id))
}

fn user_display_name(user: &TelegramUser) -> String {
    match (&user.last_name, &user.username) {
        (Some(last), _) => format!("{} {}", user.first_name, last),
        (None, Some(username)) if user.first_name.is_empty() => format!("@{username}"),
        _ => user.first_name.clone(),
    }
}

fn suppressed_receipt(
    envelope: &verlet_io_core::EgressEnvelope,
    reason: &str,
) -> verlet_io_core::DeliveryReceipt {
    verlet_io_core::DeliveryReceipt {
        egress_id: envelope.id.clone(),
        delivered: true,
        external_message_id: None,
        error: None,
        metadata: std::collections::BTreeMap::from([(
            "telegram_suppressed".to_string(),
            reason.to_string(),
        )]),
    }
}

fn action_receipt(
    envelope: &verlet_io_core::EgressEnvelope,
    action: &str,
) -> verlet_io_core::DeliveryReceipt {
    verlet_io_core::DeliveryReceipt {
        egress_id: envelope.id.clone(),
        delivered: true,
        external_message_id: None,
        error: None,
        metadata: std::collections::BTreeMap::from([(
            "telegram_action".to_string(),
            action.to_string(),
        )]),
    }
}

fn sanitize_reqwest_error(err: reqwest::Error) -> String {
    if let Some(status) = err.status() {
        format!("status {status}")
    } else if err.is_timeout() {
        "timeout".to_string()
    } else if err.is_connect() {
        "connect error".to_string()
    } else {
        "transport error".to_string()
    }
}

fn truncate_body(body: &str) -> String {
    const MAX_BODY_CHARS: usize = 512;
    body.chars().take(MAX_BODY_CHARS).collect()
}

#[cfg(test)]
mod tests {
    use verlet_io_core::EgressAdapter as _;
    use verlet_io_core::IoProtocolAdapter as _;

    fn direct_update() -> crate::TelegramUpdate {
        serde_json::from_value(serde_json::json!({
            "update_id": 999,
            "message": {
                "message_id": 555,
                "from": {
                    "id": 42,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada"
                },
                "chat": {
                    "id": 123,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada"
                },
                "date": 1777000000,
                "text": "hello telegram"
            }
        }))
        .unwrap()
    }

    #[test]
    fn normalizes_direct_message_update_to_ingress_envelope() {
        let normalizer = crate::TelegramNormalizer::new("main");
        let envelope = normalizer
            .normalize_update(&direct_update(), 1_777_000_000_123)
            .unwrap()
            .unwrap();

        assert_eq!(envelope.source.protocol, crate::TELEGRAM_PROTOCOL);
        assert_eq!(envelope.source.instance_id, "main");
        assert_eq!(
            envelope.conversation.external_conversation_id,
            "telegram:chat:123"
        );
        assert_eq!(
            envelope.conversation.kind,
            verlet_io_core::ConversationKind::Direct
        );
        assert_eq!(
            envelope
                .actor
                .as_ref()
                .map(|actor| actor.external_actor_id.as_str()),
            Some("telegram:user:42")
        );
        assert_eq!(
            envelope
                .dedupe_key
                .as_ref()
                .map(verlet_io_core::IoDedupeKey::stable_key),
            Some("telegram.bot:main:update:999".to_string())
        );
        assert_eq!(
            envelope
                .effective_dedupe_key()
                .as_ref()
                .map(verlet_io_core::IoDedupeKey::stable_key),
            Some("telegram.bot:main:update:999".to_string()),
            "the adapter-envelope migration must preserve the literal pre-contract key"
        );
        assert_eq!(
            envelope
                .delivery
                .as_ref()
                .map(|delivery| delivery.delivery_id.as_str()),
            Some("update:999")
        );
        assert_eq!(envelope.principal, None);
        assert_eq!(envelope.content.text_projection(), "hello telegram");
        assert_eq!(
            envelope
                .metadata
                .get("telegram_message_id")
                .map(String::as_str),
            Some("555")
        );
    }

    #[test]
    fn normalizes_supergroup_topic_command() {
        let update: crate::TelegramUpdate = serde_json::from_value(serde_json::json!({
            "update_id": 1000,
            "message": {
                "message_id": 556,
                "message_thread_id": 777,
                "from": {
                    "id": 43,
                    "is_bot": false,
                    "first_name": "Grace",
                    "last_name": "Hopper"
                },
                "chat": {
                    "id": -10042,
                    "type": "supergroup",
                    "title": "Verlet HQ"
                },
                "date": 1777000001,
                "text": "/steer keep going"
            }
        }))
        .unwrap();

        let envelope = crate::TelegramNormalizer::new("main")
            .normalize_update(&update, 1)
            .unwrap()
            .unwrap();

        assert_eq!(
            envelope.conversation.kind,
            verlet_io_core::ConversationKind::Group
        );
        assert_eq!(
            envelope.conversation.external_thread_id.as_deref(),
            Some("777")
        );
        assert_eq!(envelope.content.text_projection(), "/steer keep going");
        assert!(matches!(
            envelope.content,
            verlet_io_core::IngressContent::Command {
                ref name,
                ref args
            } if name == "steer" && args.as_deref() == Some("keep going")
        ));
    }

    #[test]
    fn normalizes_callback_query_as_event() {
        let update: crate::TelegramUpdate = serde_json::from_value(serde_json::json!({
            "update_id": 1001,
            "callback_query": {
                "id": "cb-1",
                "from": {
                    "id": 44,
                    "is_bot": false,
                    "first_name": "Lin"
                },
                "message": {
                    "message_id": 557,
                    "chat": {
                        "id": 123,
                        "type": "private",
                        "first_name": "Ada"
                    },
                    "date": 1777000002,
                    "text": "button source"
                },
                "data": "approve"
            }
        }))
        .unwrap();

        let envelope = crate::TelegramNormalizer::new("main")
            .normalize_update(&update, 1)
            .unwrap()
            .unwrap();

        assert_eq!(
            envelope
                .actor
                .as_ref()
                .map(|actor| actor.external_actor_id.as_str()),
            Some("telegram:user:44")
        );
        assert!(matches!(
            envelope.content,
            verlet_io_core::IngressContent::Event { ref kind, .. } if kind == "telegram.callback_query"
        ));
        assert_eq!(
            envelope
                .metadata
                .get("telegram_update_kind")
                .map(String::as_str),
            Some("callback_query")
        );
    }

    #[test]
    fn normalizes_message_reaction_as_event() {
        let update: crate::TelegramUpdate = serde_json::from_value(serde_json::json!({
            "update_id": 1002,
            "message_reaction": {
                "chat": {
                    "id": 123,
                    "type": "private",
                    "first_name": "Ada"
                },
                "message_id": 555,
                "user": {
                    "id": 44,
                    "is_bot": false,
                    "first_name": "Lin",
                    "username": "lin"
                },
                "date": 1777000003,
                "old_reaction": [
                    { "type": "emoji", "emoji": "👍" }
                ],
                "new_reaction": [
                    { "type": "custom_emoji", "custom_emoji_id": "custom-1" },
                    { "type": "emoji", "emoji": "❤️" }
                ]
            }
        }))
        .unwrap();

        let envelope = crate::TelegramNormalizer::new("main")
            .normalize_update(&update, 1)
            .unwrap()
            .unwrap();

        assert_eq!(
            envelope
                .actor
                .as_ref()
                .map(|actor| actor.external_actor_id.as_str()),
            Some("telegram:user:44")
        );
        assert!(matches!(
            envelope.content,
            verlet_io_core::IngressContent::Event { ref kind, ref payload }
                if kind == "telegram.message_reaction"
                    && payload["message_id"] == 555
                    && payload["old_reaction"] == serde_json::json!([{ "type": "emoji", "emoji": "👍" }])
                    && payload["new_reaction"] == serde_json::json!([
                        { "type": "custom_emoji", "custom_emoji_id": "custom-1" },
                        { "type": "emoji", "emoji": "❤️" }
                    ])
        ));
        assert_eq!(
            envelope
                .metadata
                .get("telegram_update_kind")
                .map(String::as_str),
            Some("message_reaction")
        );
        assert_eq!(
            envelope
                .metadata
                .get("telegram_message_id")
                .map(String::as_str),
            Some("555")
        );
    }

    #[test]
    fn message_reaction_unknown_reaction_type_stays_opaque() {
        let update: crate::TelegramUpdate = serde_json::from_value(serde_json::json!({
            "update_id": 1003,
            "message_reaction": {
                "chat": { "id": 123, "type": "private" },
                "message_id": 556,
                "date": 1777000004,
                "old_reaction": [],
                "new_reaction": [
                    {
                        "type": "paid",
                        "emoji": "⭐",
                        "amount": 1
                    }
                ]
            }
        }))
        .unwrap();

        let envelope = crate::TelegramNormalizer::new("main")
            .normalize_update(&update, 1)
            .unwrap()
            .unwrap();

        assert!(matches!(
            envelope.content,
            verlet_io_core::IngressContent::Event { ref kind, ref payload }
                if kind == "telegram.message_reaction"
                    && payload["new_reaction"] == serde_json::json!([
                        {
                            "type": "paid",
                            "emoji": "⭐",
                            "amount": 1
                        }
            ])
        ));
    }

    #[test]
    fn message_reaction_missing_actor_and_unknown_chat_type_still_normalizes() {
        let update: crate::TelegramUpdate = serde_json::from_value(serde_json::json!({
            "update_id": 1004,
            "message_reaction": {
                "chat": { "id": 123, "type": "forumish" },
                "message_id": 557,
                "date": 1777000005,
                "old_reaction": [],
                "new_reaction": []
            }
        }))
        .unwrap();

        let envelope = crate::TelegramNormalizer::new("main")
            .normalize_update(&update, 1)
            .unwrap()
            .unwrap();

        assert!(envelope.actor.is_none());
        assert_eq!(
            envelope.conversation.kind,
            verlet_io_core::ConversationKind::Group
        );
        assert_eq!(
            envelope
                .conversation
                .metadata
                .get("telegram_chat_type")
                .map(String::as_str),
            Some("forumish")
        );
        assert!(matches!(
            envelope.content,
            verlet_io_core::IngressContent::Event { ref kind, ref payload }
                if kind == "telegram.message_reaction"
                    && payload["old_reaction"] == serde_json::json!([])
                    && payload["new_reaction"] == serde_json::json!([])
        ));
    }

    #[test]
    fn message_reaction_emoji_egress_serializes_in_derived_field_order() {
        let request = crate::TelegramSetMessageReactionRequest {
            chat_id: crate::TelegramChatId::Id(123),
            message_id: 555,
            reaction: vec![crate::TelegramReactionType::Emoji {
                emoji: "👍".to_string(),
            }],
        };

        assert_eq!(
            serde_json::to_string(&request).unwrap(),
            r#"{"chat_id":123,"message_id":555,"reaction":[{"type":"emoji","emoji":"👍"}]}"#
        );
    }

    #[test]
    fn message_reaction_other_round_trips_opaque_json() {
        let source = serde_json::json!({
            "type": "paid",
            "emoji": "⭐",
            "amount": 1,
            "nested": { "ok": true }
        });

        let reaction: crate::TelegramReactionType = serde_json::from_value(source.clone()).unwrap();

        assert!(matches!(reaction, crate::TelegramReactionType::Other(_)));
        assert_eq!(serde_json::to_value(&reaction).unwrap(), source);
    }

    #[test]
    fn message_reaction_known_type_with_extra_fields_stays_opaque() {
        let source = serde_json::json!({
            "type": "emoji",
            "emoji": "👍",
            "future_field": "kept"
        });

        let reaction: crate::TelegramReactionType = serde_json::from_value(source.clone()).unwrap();

        assert!(matches!(reaction, crate::TelegramReactionType::Other(_)));
        assert_eq!(serde_json::to_value(&reaction).unwrap(), source);
    }

    #[test]
    fn captures_basic_document_and_photo_attachments() {
        let update: crate::TelegramUpdate = serde_json::from_value(serde_json::json!({
            "update_id": 1002,
            "message": {
                "message_id": 558,
                "chat": { "id": 123, "type": "private" },
                "date": 1777000003,
                "caption": "see attached",
                "document": {
                    "file_id": "doc-file",
                    "file_unique_id": "doc-unique",
                    "file_name": "report.pdf",
                    "mime_type": "application/pdf",
                    "file_size": 1234
                },
                "photo": [
                    {
                        "file_id": "small-photo",
                        "file_unique_id": "small",
                        "width": 64,
                        "height": 64,
                        "file_size": 512
                    },
                    {
                        "file_id": "large-photo",
                        "file_unique_id": "large",
                        "width": 1024,
                        "height": 768,
                        "file_size": 4096
                    }
                ]
            }
        }))
        .unwrap();

        let envelope = crate::TelegramNormalizer::new("main")
            .normalize_update(&update, 1)
            .unwrap()
            .unwrap();

        assert_eq!(envelope.content.text_projection(), "see attached");
        assert_eq!(envelope.attachments.len(), 2);
        assert_eq!(envelope.attachments[0].name.as_deref(), Some("report.pdf"));
        assert_eq!(envelope.attachments[1].id, "telegram:file:large-photo");
    }

    #[test]
    fn builds_send_message_request_from_egress() {
        let conversation = verlet_io_core::IoConversation::new(
            "telegram:chat:-10042",
            verlet_io_core::ConversationKind::Group,
        )
        .with_external_thread_id("777");
        let mut target = verlet_io_core::IoTarget {
            source: verlet_io_core::IoSource::new(crate::TELEGRAM_PROTOCOL, "main"),
            conversation,
            actor: None,
            metadata: std::collections::BTreeMap::new(),
        };
        target.metadata.insert(
            "telegram_reply_to_message_id".to_string(),
            "555".to_string(),
        );
        let egress = verlet_io_core::EgressEnvelope::new(
            target,
            verlet_io_core::EgressKind::AssistantMessage {
                text: "hello back".to_string(),
            },
            1,
        );

        let request = crate::build_send_message_request(&egress).unwrap().unwrap();

        assert_eq!(request.chat_id, crate::TelegramChatId::Id(-10042));
        assert_eq!(request.message_thread_id, Some(777));
        assert_eq!(request.reply_to_message_id, Some(555));
        assert_eq!(request.text, "hello back");
    }

    #[test]
    fn builds_typing_platform_action_request() {
        let egress = verlet_io_core::EgressEnvelope::new(
            verlet_io_core::IoTarget {
                source: verlet_io_core::IoSource::new(crate::TELEGRAM_PROTOCOL, "main"),
                conversation: verlet_io_core::IoConversation::new(
                    "telegram:chat:123",
                    verlet_io_core::ConversationKind::Direct,
                ),
                actor: None,
                metadata: std::collections::BTreeMap::new(),
            },
            verlet_io_core::EgressKind::PlatformAction {
                action: "typing".to_string(),
                payload: serde_json::json!({}),
            },
            1,
        );

        let request = crate::build_platform_action_request(&egress)
            .unwrap()
            .unwrap();

        assert!(matches!(
            request,
            crate::TelegramPlatformActionRequest::SendChatAction(crate::TelegramSendChatActionRequest {
                chat_id: crate::TelegramChatId::Id(123),
                message_thread_id: None,
                ref action,
            }) if action == "typing"
        ));
    }

    #[test]
    fn builds_reaction_platform_action_request() {
        let egress = verlet_io_core::EgressEnvelope::new(
            verlet_io_core::IoTarget {
                source: verlet_io_core::IoSource::new(crate::TELEGRAM_PROTOCOL, "main"),
                conversation: verlet_io_core::IoConversation::new(
                    "telegram:chat:123",
                    verlet_io_core::ConversationKind::Direct,
                ),
                actor: None,
                metadata: std::collections::BTreeMap::new(),
            },
            verlet_io_core::EgressKind::PlatformAction {
                action: "reaction".to_string(),
                payload: serde_json::json!({
                    "message_id": 555,
                    "emoji": "👍",
                }),
            },
            1,
        );

        let request = crate::build_platform_action_request(&egress)
            .unwrap()
            .unwrap();

        assert!(matches!(
            request,
            crate::TelegramPlatformActionRequest::SetMessageReaction(crate::TelegramSetMessageReactionRequest {
                chat_id: crate::TelegramChatId::Id(123),
                message_id: 555,
                reaction,
            }) if reaction == vec![crate::TelegramReactionType::Emoji { emoji: "👍".to_string() }]
        ));
    }

    #[test]
    fn reaction_platform_action_can_use_egress_metadata_message_id() {
        let mut egress = verlet_io_core::EgressEnvelope::new(
            verlet_io_core::IoTarget {
                source: verlet_io_core::IoSource::new(crate::TELEGRAM_PROTOCOL, "main"),
                conversation: verlet_io_core::IoConversation::new(
                    "telegram:chat:123",
                    verlet_io_core::ConversationKind::Direct,
                ),
                actor: None,
                metadata: std::collections::BTreeMap::new(),
            },
            verlet_io_core::EgressKind::PlatformAction {
                action: "reaction".to_string(),
                payload: serde_json::json!({
                    "emoji": "👍",
                }),
            },
            1,
        );
        egress
            .metadata
            .insert("telegram_message_id".to_string(), "555".to_string());

        let request = crate::build_platform_action_request(&egress)
            .unwrap()
            .unwrap();

        assert!(matches!(
            request,
            crate::TelegramPlatformActionRequest::SetMessageReaction(
                crate::TelegramSetMessageReactionRequest {
                    message_id: 555,
                    ..
                }
            )
        ));
    }

    #[test]
    fn builds_sticker_platform_action_request() {
        let conversation = verlet_io_core::IoConversation::new(
            "telegram:chat:-10042",
            verlet_io_core::ConversationKind::Group,
        )
        .with_external_thread_id("777");
        let egress = verlet_io_core::EgressEnvelope::new(
            verlet_io_core::IoTarget {
                source: verlet_io_core::IoSource::new(crate::TELEGRAM_PROTOCOL, "main"),
                conversation,
                actor: None,
                metadata: std::collections::BTreeMap::new(),
            },
            verlet_io_core::EgressKind::PlatformAction {
                action: "sticker".to_string(),
                payload: serde_json::json!({
                    "file_id": "sticker-file",
                }),
            },
            1,
        );

        let request = crate::build_platform_action_request(&egress)
            .unwrap()
            .unwrap();

        assert!(matches!(
            request,
            crate::TelegramPlatformActionRequest::SendSticker(crate::TelegramSendStickerRequest {
                chat_id: crate::TelegramChatId::Id(-10042),
                message_thread_id: Some(777),
                ref sticker,
                ..
            }) if sticker == "sticker-file"
        ));
    }

    #[test]
    fn rejects_unknown_platform_action() {
        let egress = verlet_io_core::EgressEnvelope::new(
            verlet_io_core::IoTarget {
                source: verlet_io_core::IoSource::new(crate::TELEGRAM_PROTOCOL, "main"),
                conversation: verlet_io_core::IoConversation::new(
                    "telegram:chat:123",
                    verlet_io_core::ConversationKind::Direct,
                ),
                actor: None,
                metadata: std::collections::BTreeMap::new(),
            },
            verlet_io_core::EgressKind::PlatformAction {
                action: "wave".to_string(),
                payload: serde_json::json!({}),
            },
            1,
        );

        let err = crate::build_platform_action_request(&egress).unwrap_err();
        assert!(err.to_string().contains("unknown Telegram platform action"));
    }

    #[tokio::test]
    async fn silence_egress_is_suppressed_without_wire_request() {
        let egress = verlet_io_core::EgressEnvelope::new(
            verlet_io_core::IoTarget {
                source: verlet_io_core::IoSource::new(crate::TELEGRAM_PROTOCOL, "main"),
                conversation: verlet_io_core::IoConversation::new(
                    "telegram:chat:123",
                    verlet_io_core::ConversationKind::Direct,
                ),
                actor: None,
                metadata: std::collections::BTreeMap::new(),
            },
            verlet_io_core::EgressKind::Silence {
                reason: Some("agent_declined".to_string()),
            },
            1,
        );

        let receipt = crate::TelegramEgressAdapter::new("main", "token")
            .deliver(egress)
            .await
            .unwrap();

        assert!(receipt.delivered);
        assert_eq!(receipt.external_message_id, None);
        assert_eq!(
            receipt
                .metadata
                .get("telegram_suppressed")
                .map(String::as_str),
            Some("silence")
        );
    }

    #[test]
    fn skips_non_visible_egress_events() {
        let egress = verlet_io_core::EgressEnvelope::new(
            verlet_io_core::IoTarget {
                source: verlet_io_core::IoSource::new(crate::TELEGRAM_PROTOCOL, "main"),
                conversation: verlet_io_core::IoConversation::new(
                    "telegram:chat:123",
                    verlet_io_core::ConversationKind::Direct,
                ),
                actor: None,
                metadata: std::collections::BTreeMap::new(),
            },
            verlet_io_core::EgressKind::ToolStarted {
                name: "bash".to_string(),
            },
            1,
        );

        assert_eq!(crate::build_send_message_request(&egress).unwrap(), None);
    }

    #[test]
    fn egress_adapter_rejects_wrong_bot_instance() {
        let egress = verlet_io_core::EgressEnvelope::new(
            verlet_io_core::IoTarget {
                source: verlet_io_core::IoSource::new(crate::TELEGRAM_PROTOCOL, "other"),
                conversation: verlet_io_core::IoConversation::new(
                    "telegram:chat:123",
                    verlet_io_core::ConversationKind::Direct,
                ),
                actor: None,
                metadata: std::collections::BTreeMap::new(),
            },
            verlet_io_core::EgressKind::AssistantMessage {
                text: "hello".to_string(),
            },
            1,
        );

        let err = crate::TelegramEgressAdapter::new("main", "token")
            .build_send_message(&egress)
            .unwrap_err();
        assert!(err.to_string().contains("expected"));
    }

    #[test]
    fn protocol_capabilities_are_explicit() {
        let ingress = crate::TelegramWebhookAdapter::new("main").capabilities();
        assert!(ingress.ingress);
        assert!(!ingress.egress);
        assert!(ingress.durable_offsets);

        let egress = crate::TelegramEgressAdapter::new("main", "token").capabilities();
        assert!(!egress.ingress);
        assert!(egress.egress);
    }
}

#[cfg(test)]
mod retry_classification_tests {
    #[test]
    fn a_rate_limit_carries_telegrams_own_retry_after() {
        let error = crate::TelegramError::Api {
            status: 429,
            body: "Too Many Requests: retry after 30".to_string(),
            retry_after_secs: Some(30),
        };

        let io_error: verlet_io_core::IoError = error.into();

        assert!(matches!(
            io_error,
            verlet_io_core::IoError::RateLimited {
                retry_after_ms: Some(30_000),
                ..
            }
        ));
    }

    #[test]
    fn a_rate_limit_without_a_hint_is_still_a_rate_limit() {
        let error = crate::TelegramError::Api {
            status: 429,
            body: "Too Many Requests".to_string(),
            retry_after_secs: None,
        };

        let io_error: verlet_io_core::IoError = error.into();

        assert!(matches!(
            io_error,
            verlet_io_core::IoError::RateLimited {
                retry_after_ms: None,
                ..
            }
        ));
    }

    #[test]
    fn blocked_bot_and_missing_chat_are_permanent_not_retryable() {
        for status in [400, 401, 403] {
            let error = crate::TelegramError::Api {
                status,
                body: "bot was blocked by the user".to_string(),
                retry_after_secs: None,
            };

            let io_error: verlet_io_core::IoError = error.into();

            assert!(
                matches!(io_error, verlet_io_core::IoError::PermanentDelivery(_)),
                "status {status} should be permanent, got {io_error:?}"
            );
        }
    }

    #[test]
    fn a_server_side_fault_stays_an_ordinary_retryable_delivery_failure() {
        for status in [500, 502, 503] {
            let error = crate::TelegramError::Api {
                status,
                body: "Bad Gateway".to_string(),
                retry_after_secs: None,
            };

            let io_error: verlet_io_core::IoError = error.into();

            assert!(
                matches!(io_error, verlet_io_core::IoError::Delivery(_)),
                "status {status} should stay retryable, got {io_error:?}"
            );
        }
    }

    #[test]
    fn a_429_body_yields_its_retry_after_through_the_response_parameters() {
        // Pins the deserialization, which is where the hint was being dropped:
        // TelegramApiResponse did not model `parameters` at all.
        let decoded: crate::TelegramApiResponse<serde_json::Value> = serde_json::from_str(
            r#"{"ok":false,"error_code":429,"description":"Too Many Requests: retry after 17","parameters":{"retry_after":17}}"#,
        )
        .unwrap();

        assert_eq!(
            decoded
                .parameters
                .and_then(|parameters| parameters.retry_after),
            Some(17)
        );
    }
}
