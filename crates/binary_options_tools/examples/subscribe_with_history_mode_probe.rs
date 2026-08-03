use std::{env, sync::Arc, time::Duration};

use async_trait::async_trait;
use binary_options_tools::{
    config::Config,
    pocketoption::{
        connect::PocketConnect,
        error::{PocketError, PocketResult},
        modules::{
            assets::AssetsModule,
            balance::BalanceModule,
            chart_stream::{ChartStreamApiModule, HistoryStreamEvent, HistoryStreamMode},
            deals::DealsApiModule,
            get_candles::GetCandlesApiModule,
            historical_data::HistoricalDataApiModule,
            keep_alive::{InitModule, KeepAliveModule},
            pending_trades::PendingTradesApiModule,
            raw::RawApiModule,
            server_time::ServerTimeModule,
            subscriptions::SubscriptionsApiModule,
            trades::TradesApiModule,
        },
        ssid::Ssid,
        state::{State, StateBuilder},
    },
};
use binary_options_tools_core::{
    builder::ClientBuilder,
    error::CoreResult,
    middleware::{MiddlewareContext, WebSocketMiddleware},
};
use futures_util::StreamExt;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

const DEFAULT_ASSET: &str = "EURUSD_otc";
const DEFAULT_PERIOD: u32 = 5;
const DEFAULT_LIVE_SECONDS: u64 = 20;
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Default)]
struct OutboundTap {
    frames: Arc<Mutex<Vec<String>>>,
}

impl OutboundTap {
    async fn checkpoint(&self) -> usize {
        self.frames.lock().await.len()
    }

    async fn frames_since(&self, checkpoint: usize) -> Vec<String> {
        self.frames.lock().await[checkpoint..].to_vec()
    }
}

#[async_trait]
impl WebSocketMiddleware<State> for OutboundTap {
    async fn on_send(&self, message: &Message, _: &MiddlewareContext<State>) -> CoreResult<()> {
        let frame = match message {
            Message::Text(text) => text.to_string(),
            Message::Binary(data) => format!("<binary:{} bytes>", data.len()),
            other => format!("{other:?}"),
        };
        println!("[socket-out] {frame}");
        self.frames.lock().await.push(frame);
        Ok(())
    }
}

#[derive(Debug)]
struct Args {
    ssid: String,
    asset: String,
    period: u32,
    mode: HistoryStreamMode,
    live_seconds: u64,
}

impl Args {
    fn parse() -> PocketResult<Self> {
        let mut ssid = env::var("POCKET_OPTION_SSID").ok();
        let mut asset = DEFAULT_ASSET.to_string();
        let mut period = DEFAULT_PERIOD;
        let mut mode = HistoryStreamMode::Points;
        let mut live_seconds = DEFAULT_LIVE_SECONDS;

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--ssid" => ssid = args.next(),
                "--asset" => asset = required_value(&mut args, "--asset")?,
                "--period" => period = parse_value(&mut args, "--period")?,
                "--mode" => {
                    let value = required_value(&mut args, "--mode")?.to_lowercase();
                    mode = match value.as_str() {
                        "points" => HistoryStreamMode::Points,
                        "ohlc" => HistoryStreamMode::Ohlc,
                        _ => {
                            return Err(PocketError::General(
                                "--mode must be 'points' or 'ohlc'".to_string(),
                            ))
                        }
                    };
                }
                "--seconds" => live_seconds = parse_value(&mut args, "--seconds")?,
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => {
                    return Err(PocketError::General(format!(
                        "Unknown argument '{other}'. Use --help for usage."
                    )))
                }
            }
        }

        Ok(Self {
            ssid: ssid.ok_or_else(|| {
                PocketError::General(
                    "Missing SSID. Pass --ssid or set POCKET_OPTION_SSID.".to_string(),
                )
            })?,
            asset,
            period,
            mode,
            live_seconds,
        })
    }
}

fn required_value(args: &mut impl Iterator<Item = String>, flag: &str) -> PocketResult<String> {
    args.next()
        .ok_or_else(|| PocketError::General(format!("{flag} requires a value")))
}

fn parse_value<T>(args: &mut impl Iterator<Item = String>, flag: &str) -> PocketResult<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let raw = required_value(args, flag)?;
    raw.parse::<T>()
        .map_err(|err| PocketError::General(format!("Invalid {flag} value '{raw}': {err}")))
}

fn print_help() {
    println!(
        r#"Live probe for subscribe_with_history_mode.

Usage:
  cargo run -p binary_options_tools --example subscribe_with_history_mode_probe -- \
    --ssid '<DEMO_SSID>' --asset EURUSD_otc --period 5 --mode points --seconds 20

Options:
  --ssid <ssid>       Pocket Option SSID. Can also use POCKET_OPTION_SSID.
  --asset <asset>     Asset symbol. Default: EURUSD_otc.
  --period <seconds>  Chart/history period. Default: 5.
  --mode <mode>       points or ohlc. Default: points.
  --seconds <n>       Live subscription duration after bootstrap. Default: 20.

The probe prints outbound socket frames after connection init is complete. For this method
we expect exactly one changeSymbol frame and no subscribeSymbol/subfor frames.
"#
    );
}

#[tokio::main]
async fn main() -> PocketResult<()> {
    let args = Args::parse()?;
    println!("Probe args: {args:?}");

    let tap = OutboundTap::default();
    let client = build_client(&args.ssid, tap.clone()).await?;
    let mut runner = client.1;
    let client = client.0;

    let runner_task = tokio::spawn(async move { runner.run().await });
    client.wait_connected().await;

    println!("Connected. Waiting 3 seconds before API calls.");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let expected_bootstrap_count = print_reference_bootstrap(&client, &args).await?;
    let checkpoint = tap.checkpoint().await;
    println!("Cleared init/reference outbound frame baseline at {checkpoint} frames.");

    let handle = client
        .get_handle::<ChartStreamApiModule>()
        .await
        .ok_or_else(|| PocketError::ModuleNotFound("ChartStreamApiModule".to_string()))?;

    let mut stream = handle
        .subscribe_with_history_mode(args.asset.clone(), args.period, args.mode)
        .await?
        .to_stream()
        .boxed();

    println!("\n== Warm stream bootstrap output ==");
    let bootstrap_count = drain_bootstrap(&mut stream, expected_bootstrap_count).await?;
    println!("Warm bootstrap events printed: {bootstrap_count}/{expected_bootstrap_count}");

    println!(
        "\n== Live updateStream output for {} seconds ==",
        args.live_seconds
    );
    let live_count = drain_live(&mut stream, Duration::from_secs(args.live_seconds)).await?;
    println!("Live events printed: {live_count}");

    println!("\n== Outbound frames sent by subscribe_with_history_mode ==");
    let frames = tap.frames_since(checkpoint).await;
    for (index, frame) in frames.iter().enumerate() {
        println!("[{index}] {frame}");
    }
    assert_only_change_symbol(&frames)?;

    client.shutdown_ref().await.map_err(PocketError::from)?;
    runner_task.abort();
    println!("\nDisconnected.");
    Ok(())
}

async fn build_client(
    ssid: &str,
    tap: OutboundTap,
) -> PocketResult<(
    binary_options_tools_core::client::Client<State>,
    binary_options_tools_core::client::ClientRunner<State>,
)> {
    let parsed_ssid = Ssid::parse(ssid)?;
    let mut builder = StateBuilder::default().ssid(parsed_ssid.clone());
    if let Some(url) = parsed_ssid.current_url() {
        builder = builder.default_connection_url(url);
    } else if let Some(url) = Config::default().urls.first() {
        builder = builder.default_connection_url(url.to_string());
    }

    let state = builder.build()?;
    ClientBuilder::new(PocketConnect, state)
        .with_lightweight_module::<KeepAliveModule>()
        .with_lightweight_module::<InitModule>()
        .with_lightweight_module::<BalanceModule>()
        .with_lightweight_module::<ServerTimeModule>()
        .with_lightweight_module::<AssetsModule>()
        .with_module::<TradesApiModule>()
        .with_module::<DealsApiModule>()
        .with_module::<SubscriptionsApiModule>()
        .with_module::<GetCandlesApiModule>()
        .with_module::<PendingTradesApiModule>()
        .with_module::<HistoricalDataApiModule>()
        .with_module::<ChartStreamApiModule>()
        .with_module::<RawApiModule>()
        .with_middleware(Box::new(tap))
        .build()
        .await
        .map_err(PocketError::from)
}

async fn print_reference_bootstrap(
    client: &binary_options_tools_core::client::Client<State>,
    args: &Args,
) -> PocketResult<usize> {
    let handle = client
        .get_handle::<HistoricalDataApiModule>()
        .await
        .ok_or_else(|| PocketError::ModuleNotFound("HistoricalDataApiModule".to_string()))?;

    println!("\n== Reference merged bootstrap from history API ==");
    match args.mode {
        HistoryStreamMode::Points => {
            let points = handle
                .history_points(args.asset.clone(), args.period)
                .await?;
            for (index, point) in points.iter().enumerate() {
                println!(
                    "[reference #{}] point asset={} time={} price={} json={}",
                    index + 1,
                    point.asset,
                    point.time,
                    point.price,
                    serde_json::to_string(point)
                        .map_err(|err| PocketError::General(err.to_string()))?
                );
            }
            println!("Reference bootstrap point count: {}", points.len());
            Ok(points.len())
        }
        HistoryStreamMode::Ohlc => {
            let candles = handle.history_ohlc(args.asset.clone(), args.period).await?;
            for (index, candle) in candles.iter().enumerate() {
                println!(
                    "[reference #{}] candle symbol={} ts={} open={} high={} low={} close={} json={}",
                    index + 1,
                    candle.symbol,
                    candle.timestamp,
                    candle.open,
                    candle.high,
                    candle.low,
                    candle.close,
                    serde_json::to_string(candle)
                        .map_err(|err| PocketError::General(err.to_string()))?
                );
            }
            println!("Reference bootstrap candle count: {}", candles.len());
            Ok(candles.len())
        }
    }
}

async fn drain_bootstrap(
    stream: &mut futures_util::stream::BoxStream<'_, PocketResult<HistoryStreamEvent>>,
    expected_count: usize,
) -> PocketResult<usize> {
    let mut count = 0;
    while count < expected_count {
        match tokio::time::timeout(BOOTSTRAP_TIMEOUT, stream.next()).await {
            Ok(Some(Ok(event))) => {
                count += 1;
                print_event("bootstrap", count, &event)?;
            }
            Ok(Some(Err(err))) => return Err(err),
            Ok(None) => return Err(PocketError::General("stream ended during bootstrap".into())),
            Err(_) => {
                return Err(PocketError::General(format!(
                    "timed out waiting for warm bootstrap event {}/{}",
                    count + 1,
                    expected_count
                )))
            }
        }
    }
    Ok(count)
}

async fn drain_live(
    stream: &mut futures_util::stream::BoxStream<'_, PocketResult<HistoryStreamEvent>>,
    duration: Duration,
) -> PocketResult<usize> {
    let deadline = tokio::time::Instant::now() + duration;
    let mut count = 0;
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(event))) => {
                count += 1;
                print_event("live", count, &event)?;
            }
            Ok(Some(Err(err))) => return Err(err),
            Ok(None) => return Ok(count),
            Err(_) => return Ok(count),
        }
    }
    Ok(count)
}

fn print_event(phase: &str, index: usize, event: &HistoryStreamEvent) -> PocketResult<()> {
    match event {
        HistoryStreamEvent::Point(point) => {
            println!(
                "[{phase} #{index}] point asset={} time={} price={} json={}",
                point.asset,
                point.time,
                point.price,
                serde_json::to_string(point)
                    .map_err(|err| PocketError::General(err.to_string()))?
            );
        }
        HistoryStreamEvent::Candle(candle) => {
            println!(
                "[{phase} #{index}] candle symbol={} ts={} open={} high={} low={} close={} json={}",
                candle.symbol,
                candle.timestamp,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                serde_json::to_string(candle)
                    .map_err(|err| PocketError::General(err.to_string()))?
            );
        }
    }
    Ok(())
}

fn assert_only_change_symbol(frames: &[String]) -> PocketResult<()> {
    let non_keepalive: Vec<_> = frames
        .iter()
        .filter(|frame| frame.as_str() != r#"42["ps"]"# && frame.as_str() != "3")
        .collect();
    let change_symbol: Vec<_> = frames
        .iter()
        .filter(|frame| {
            frame.contains(r#"[\"changeSymbol\""#) || frame.contains(r#"["changeSymbol""#)
        })
        .collect();
    let forbidden: Vec<_> = non_keepalive
        .iter()
        .filter(|frame| frame.contains("subscribeSymbol") || frame.contains("subfor"))
        .collect();

    println!("changeSymbol frames: {}", change_symbol.len());
    println!(
        "forbidden subscribeSymbol/subfor frames: {}",
        forbidden.len()
    );
    println!(
        "ignored keepalive frames: {}",
        frames.len() - non_keepalive.len()
    );

    if change_symbol.len() != 1 || !forbidden.is_empty() || non_keepalive.len() != 1 {
        return Err(PocketError::General(format!(
            "Expected exactly one outbound changeSymbol frame and no subscribeSymbol/subfor frames after ignoring keepalive, got {frames:?}"
        )));
    }
    println!("Outbound check passed: exactly one changeSymbol, no subscribeSymbol/subfor.");
    Ok(())
}
