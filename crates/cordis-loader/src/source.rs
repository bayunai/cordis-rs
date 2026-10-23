//! 可选条目树来源契约。文件、网络或其他存储由提供方实现，Loader 不解析来源。
use crate::ExtensionsConfig;
use cordis_core::Context;
use std::fmt;

/// 来源自己的版本载荷。仅在受信任的 Loader 与来源之间往返，不能发送给客户端。
#[derive(Clone, PartialEq, Eq)]
pub struct SourceRevision(Vec<u8>);
impl SourceRevision {
    pub fn new(value: impl Into<Vec<u8>>) -> Self {
        Self(value.into())
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
impl fmt::Debug for SourceRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SourceRevision(..)")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceErrorKind {
    Io,
    InvalidData,
    Conflict,
    ReadOnly,
    Unavailable,
}

/// message 只能包含来源位置与错误分类，不得包含文件正文、解析输入或凭据。
#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct SourceError {
    pub kind: SourceErrorKind,
    pub message: String,
}
impl SourceError {
    pub fn new(kind: SourceErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

pub struct SourceDocument {
    pub config: ExtensionsConfig,
    pub revision: SourceRevision,
}

/// 所有方法在所属树的提交串行边界内调用；write 必须再次检查 expected。
/// validate_write 必须拒绝无法往返到目标声明的写入，且不得产生持久化副作用。
pub trait EntryTreeSource: Send + Sync + 'static {
    fn identity(&self) -> String;
    /// 建立来源元数据；定义原地更新成功后也会调用，以提交提供方的共享上下文策略。
    fn context(&self, owner: &Context) -> Result<Context, SourceError>;
    fn read(&self) -> Result<SourceDocument, SourceError>;
    fn check_revision(&self, expected: &SourceRevision) -> Result<(), SourceError>;
    fn validate_write(&self, config: &ExtensionsConfig) -> Result<(), SourceError>;
    fn write(
        &self,
        config: &ExtensionsConfig,
        expected: &SourceRevision,
    ) -> Result<SourceRevision, SourceError>;
}
