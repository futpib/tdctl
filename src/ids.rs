use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TdlibMessageId(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TdlibDialogId(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MtpMessageId(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MtpUserId(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MtpBasicGroupId(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MtpChannelId(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MtpSecretChatId(pub i64);

pub enum MtpPeerId {
    User(MtpUserId),
    BasicGroup(MtpBasicGroupId),
    Channel(MtpChannelId),
    SecretChat(MtpSecretChatId),
}

impl fmt::Display for MtpMessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for MtpUserId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for MtpBasicGroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for MtpChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for MtpSecretChatId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for MtpPeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MtpPeerId::User(id) => write!(f, "{id}"),
            MtpPeerId::BasicGroup(id) => write!(f, "{id}"),
            MtpPeerId::Channel(id) => write!(f, "{id}"),
            MtpPeerId::SecretChat(id) => write!(f, "{id}"),
        }
    }
}

impl From<TdlibMessageId> for MtpMessageId {
    fn from(id: TdlibMessageId) -> Self {
        MtpMessageId(id.0 >> 20)
    }
}

const ZERO_CHANNEL_ID: i64 = -1_000_000_000_000;
const ZERO_SECRET_CHAT_ID: i64 = -2_000_000_000_000;

impl From<TdlibDialogId> for MtpPeerId {
    fn from(id: TdlibDialogId) -> Self {
        if id.0 > 0 {
            MtpPeerId::User(MtpUserId(id.0))
        } else if id.0 >= -999_999_999_999 {
            MtpPeerId::BasicGroup(MtpBasicGroupId(-id.0))
        } else {
            let secret_chat_id = id.0 - ZERO_SECRET_CHAT_ID;
            if secret_chat_id >= i32::MIN as i64
                && secret_chat_id <= i32::MAX as i64
                && secret_chat_id != 0
            {
                MtpPeerId::SecretChat(MtpSecretChatId(secret_chat_id))
            } else {
                MtpPeerId::Channel(MtpChannelId(ZERO_CHANNEL_ID - id.0))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_id_conversion() {
        let tdlib = TdlibMessageId(123 << 20);
        let mtp: MtpMessageId = tdlib.into();
        assert_eq!(mtp.0, 123);
    }

    #[test]
    fn user_id_conversion() {
        let tdlib = TdlibDialogId(12345);
        let mtp: MtpPeerId = tdlib.into();
        assert!(matches!(mtp, MtpPeerId::User(MtpUserId(12345))));
    }

    #[test]
    fn basic_group_id_conversion() {
        let tdlib = TdlibDialogId(-12345);
        let mtp: MtpPeerId = tdlib.into();
        assert!(matches!(mtp, MtpPeerId::BasicGroup(MtpBasicGroupId(12345))));
    }

    #[test]
    fn channel_id_conversion() {
        let tdlib = TdlibDialogId(-1_000_000_012_345);
        let mtp: MtpPeerId = tdlib.into();
        assert!(matches!(mtp, MtpPeerId::Channel(MtpChannelId(12345))));
    }

    #[test]
    fn secret_chat_id_conversion() {
        let tdlib = TdlibDialogId(-2_000_000_000_000 + 42);
        let mtp: MtpPeerId = tdlib.into();
        assert!(matches!(mtp, MtpPeerId::SecretChat(MtpSecretChatId(42))));
    }

    #[test]
    fn display_formatting() {
        assert_eq!(format!("{}", MtpMessageId(42)), "42");
        assert_eq!(format!("{}", MtpPeerId::User(MtpUserId(123))), "123");
        assert_eq!(format!("{}", MtpPeerId::Channel(MtpChannelId(456))), "456");
    }
}
