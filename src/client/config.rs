use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::Parser;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct ClientConfig {
    pub server_addr: SocketAddr,
    pub udp_port: u16,
    pub tickers_file: PathBuf,
    pub tickers: Vec<String>,
}

#[derive(Debug, Parser)]
#[command(name = "quote-client")]
struct CliArgs {
    #[arg(long = "server-addr")]
    server_addr: SocketAddr,
    #[arg(long = "udp-port")]
    udp_port: u16,
    #[arg(long = "tickers-file")]
    tickers_file: PathBuf,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read tickers file {path}: {source}")]
    ReadTickersFile {
        path: String,
        source: std::io::Error,
    },
    #[error("tickers file {0} does not contain any valid tickers")]
    EmptyTickersFile(String),
}

impl ClientConfig {
    pub fn parse() -> Result<Self, ConfigError> {
        let args = CliArgs::parse();
        Self::from_cli_args(args)
    }

    pub fn from_iter<I, T>(iter: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let args = CliArgs::parse_from(iter);
        Self::from_cli_args(args)
    }

    fn from_cli_args(args: CliArgs) -> Result<Self, ConfigError> {
        let tickers = load_tickers(&args.tickers_file)?;

        Ok(Self {
            server_addr: args.server_addr,
            udp_port: args.udp_port,
            tickers_file: args.tickers_file,
            tickers,
        })
    }
}

pub fn load_tickers(path: &Path) -> Result<Vec<String>, ConfigError> {
    let content = fs::read_to_string(path).map_err(|source| ConfigError::ReadTickersFile {
        path: path.display().to_string(),
        source,
    })?;

    let tickers: Vec<String> = content
        .lines()
        .map(|s| s.trim())
        .filter(|line| !line.is_empty())
        .map(|s| s.to_string())
        .collect();

    if tickers.is_empty() {
        return Err(ConfigError::EmptyTickersFile(path.display().to_string()));
    }

    Ok(tickers)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        env::temp_dir,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn parses_cli_and_loads_tickers() {
        let path = temp_tickers_file("AAPL\n\n TSLA \n");

        let args = vec![
            "quote-client",
            "--server-addr",
            "127.0.0.1:9000",
            "--udp-port",
            "34254",
            "--tickers-file",
            &path,
        ];

        let config = ClientConfig::from_iter(args).unwrap();

        assert_eq!(config.server_addr, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(config.udp_port, 34254);
        assert_eq!(config.tickers, vec!["AAPL", "TSLA"]);
    }

    #[test]
    fn fails_when_tickers_file_missing() {
        let err = load_tickers(Path::new("/tmp/non-existent-tickers-file-xyz.txt")).unwrap_err();
        assert!(matches!(err, ConfigError::ReadTickersFile { .. }));
    }

    #[test]
    fn fails_when_tickers_file_has_no_valid_lines() {
        let path = temp_tickers_file("  \n  \n");
        let err = load_tickers(Path::new(&path)).unwrap_err();
        assert!(matches!(err, ConfigError::EmptyTickersFile(_)));
    }

    fn temp_tickers_file(content: &str) -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = temp_dir().join(format!("tickers-{ts}.txt"));
        fs::write(&path, content).unwrap();
        path.display().to_string()
    }
}
