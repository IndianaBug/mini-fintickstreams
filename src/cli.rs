use clap::{Parser, ValueEnum};

#[derive(Parser, Debug, Clone)]
#[command(name = "mini-fintickstreams", about)]
pub struct Cli {
    /// Where to load config from
    #[arg(long, value_enum, default_value_t = ConfigSource::Env)]
    pub config: ConfigSource,

    /// What to do during graceful shutdown
    #[arg(long, value_enum, default_value_t = ShutdownAction::None)]
    pub shutdown_action: ShutdownAction,

    /// Tokio worker threads
    #[arg(long, default_value_t = default_workers())]
    pub workers: usize,

    /// Passed into AppRuntime::new(from_env, version)
    /// Rename this later if it isn't actually a "version".
    #[arg(long, default_value_t = 1)]
    pub stream_version: u32,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum ConfigSource {
    Env,
    File,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum ShutdownAction {
    None,
    RestoreStreams,
}

fn default_workers() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
}
