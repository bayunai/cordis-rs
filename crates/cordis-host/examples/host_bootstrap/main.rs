//! 可复制的单进程 Host 模板。
//!
//! ```bash
//! cargo run -p cordis-host --example host_bootstrap
//! # 或指定自己的启动锚点：
//! cargo run -p cordis-host --example host_bootstrap -- /path/to/bootstrap.toml
//! ```

mod host;
mod plugins;

use cordis_host::CordisHost;
use cordis_loader::LoaderPlugin;
use plugins::greeting::GREETING;
use std::{path::PathBuf, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap_path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/host_bootstrap/bootstrap.toml")
        });

    let host = CordisHost::new()?;
    let loader = LoaderPlugin::bootstrap(host::build_catalog()?, bootstrap_path)?;
    let _loader_fiber = host.root().plugin(Arc::new(loader)).await?;

    // 真实应用在此运行 HTTP、窗口事件循环或后台任务。
    let loader = host.root().get(cordis_loader::LOADER)?;
    let greeting = loader.entry_context("default-greeting")?.get(GREETING)?;
    println!("{}", greeting.0);
    println!(
        "mounted entries: {}",
        loader.await_idle().await?.entries.len()
    );

    host.shutdown().await?;
    Ok(())
}
