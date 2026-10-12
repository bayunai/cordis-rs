//! 类型化只读热配置。写入端仅由 Loader 持有。

use std::sync::Arc;
use tokio::sync::watch;

/// 同一版本的完整不可变配置快照。
pub struct ConfigSnapshot<T> {
    pub revision: u64,
    pub value: Arc<T>,
}

impl<T> Clone for ConfigSnapshot<T> {
    fn clone(&self) -> Self {
        Self {
            revision: self.revision,
            value: self.value.clone(),
        }
    }
}

/// 插件持有的只读配置引用；不拥有写入端。
pub struct ConfigRef<T> {
    receiver: watch::Receiver<ConfigSnapshot<T>>,
}

impl<T> Clone for ConfigRef<T> {
    fn clone(&self) -> Self {
        Self {
            receiver: self.receiver.clone(),
        }
    }
}

impl<T> ConfigRef<T> {
    pub fn get(&self) -> ConfigSnapshot<T> {
        self.receiver.borrow().clone()
    }

    /// 从订阅时的当前版本开始观察，后续更新允许合并。
    pub fn subscribe(&self) -> ConfigSubscription<T> {
        let mut receiver = self.receiver.clone();
        receiver.borrow_and_update();
        ConfigSubscription { receiver }
    }
}

/// 应在调用方 Effect 的受管任务中等待；卸载会取消该任务。
pub struct ConfigSubscription<T> {
    receiver: watch::Receiver<ConfigSnapshot<T>>,
}

impl<T> ConfigSubscription<T> {
    pub fn current(&self) -> ConfigSnapshot<T> {
        self.receiver.borrow().clone()
    }

    /// 返回最新快照；Entry 写入端释放且没有未读更新时报告关闭。
    pub async fn changed(&mut self) -> Result<ConfigSnapshot<T>, ConfigClosed> {
        self.receiver.changed().await.map_err(|_| ConfigClosed)?;
        Ok(self.receiver.borrow_and_update().clone())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("热配置所属 Entry 已释放")]
pub struct ConfigClosed;

pub(crate) fn channel<T>(value: Arc<T>) -> (watch::Sender<ConfigSnapshot<T>>, ConfigRef<T>) {
    let (sender, receiver) = watch::channel(ConfigSnapshot { revision: 0, value });
    (sender, ConfigRef { receiver })
}
