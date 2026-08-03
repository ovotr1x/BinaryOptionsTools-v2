use async_trait::async_trait;
use binary_options_tools_core::{
    error::{CoreError, CoreResult},
    reimports::{bounded_async, AsyncReceiver, AsyncSender, Message},
    traits::{ApiModule, Rule, RunnerCommand},
};
use futures_util::stream::unfold;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::{select, sync::Mutex};
use uuid::Uuid;

use crate::pocketoption::{
    candle::{
        merge_history_ohlc, merge_history_points, BaseCandle, Candle, CandleItem, HistoryItem,
        HistoryPoint, SubscriptionType,
    },
    error::{PocketError, PocketResult},
    state::State,
    types::MultiPatternRule,
};

const STREAM_CHANNEL_CAPACITY: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryStreamMode {
    Points,
    Ohlc,
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum HistoryStreamEvent {
    Point(HistoryPoint),
    Candle(Candle),
}

#[derive(Debug)]
pub enum Command {
    SubscribeWithHistory {
        asset: String,
        period: u32,
        mode: HistoryStreamMode,
        command_id: Uuid,
    },
}

#[derive(Debug)]
pub enum CommandResponse {
    Subscribed {
        command_id: Uuid,
        stream_receiver: AsyncReceiver<PocketResult<HistoryStreamEvent>>,
    },
    Failed {
        command_id: Uuid,
        error: Box<PocketError>,
    },
}

#[derive(Clone, Debug)]
pub struct ChartStreamHandle {
    sender: AsyncSender<Command>,
    receiver: AsyncReceiver<CommandResponse>,
    call_lock: Arc<Mutex<()>>,
}

impl ChartStreamHandle {
    pub async fn subscribe_with_history_mode(
        &self,
        asset: String,
        period: u32,
        mode: HistoryStreamMode,
    ) -> PocketResult<HistoryStream> {
        let _guard = self.call_lock.lock().await;
        let command_id = Uuid::new_v4();
        self.sender
            .send(Command::SubscribeWithHistory {
                asset: asset.clone(),
                period,
                mode,
                command_id,
            })
            .await
            .map_err(CoreError::from)?;

        loop {
            match self.receiver.recv().await.map_err(CoreError::from)? {
                CommandResponse::Subscribed {
                    command_id: response_id,
                    stream_receiver,
                } if response_id == command_id => {
                    return Ok(HistoryStream {
                        receiver: stream_receiver,
                    });
                }
                CommandResponse::Failed {
                    command_id: response_id,
                    error,
                } if response_id == command_id => return Err(*error),
                _ => continue,
            }
        }
    }
}

pub struct HistoryStream {
    receiver: AsyncReceiver<PocketResult<HistoryStreamEvent>>,
}

impl HistoryStream {
    pub async fn receive(&mut self) -> PocketResult<HistoryStreamEvent> {
        self.receiver.recv().await.map_err(CoreError::from)?
    }

    pub fn to_stream(
        self,
    ) -> impl futures_util::Stream<Item = PocketResult<HistoryStreamEvent>> + 'static {
        Box::pin(unfold(self, |mut stream| async move {
            let result = stream.receive().await;
            Some((result, stream))
        }))
    }
}

#[derive(Clone)]
struct PendingStream {
    asset: String,
    period: u32,
    mode: HistoryStreamMode,
    sender: AsyncSender<PocketResult<HistoryStreamEvent>>,
}

struct ActiveStream {
    asset: String,
    mode: HistoryStreamMode,
    edge_time: f64,
    sender: AsyncSender<PocketResult<HistoryStreamEvent>>,
    live_ohlc: SubscriptionType,
}

#[derive(Deserialize)]
struct HistoryResponse {
    asset: String,
    period: u32,
    #[serde(default)]
    history: Option<Vec<HistoryItem>>,
    #[serde(default)]
    candles: Option<Vec<CandleItem>>,
}
#[derive(Clone)]
struct StreamRow {
    asset: String,
    timestamp: f64,
    price: Decimal,
}

pub struct ChartStreamApiModule {
    _state: Arc<State>,
    command_receiver: AsyncReceiver<Command>,
    command_responder: AsyncSender<CommandResponse>,
    message_receiver: AsyncReceiver<Arc<Message>>,
    to_ws_sender: AsyncSender<Message>,
    pending: Vec<PendingStream>,
    active: Vec<ActiveStream>,
}

#[async_trait]
impl ApiModule<State> for ChartStreamApiModule {
    type Command = Command;
    type CommandResponse = CommandResponse;
    type Handle = ChartStreamHandle;

    fn new(
        state: Arc<State>,
        command_receiver: AsyncReceiver<Self::Command>,
        command_responder: AsyncSender<Self::CommandResponse>,
        message_receiver: AsyncReceiver<Arc<Message>>,
        to_ws_sender: AsyncSender<Message>,
        _: AsyncSender<RunnerCommand>,
    ) -> Self {
        Self {
            _state: state,
            command_receiver,
            command_responder,
            message_receiver,
            to_ws_sender,
            pending: Vec::new(),
            active: Vec::new(),
        }
    }

    fn create_handle(
        sender: AsyncSender<Self::Command>,
        receiver: AsyncReceiver<Self::CommandResponse>,
    ) -> Self::Handle {
        ChartStreamHandle {
            sender,
            receiver,
            call_lock: Arc::new(Mutex::new(())),
        }
    }

    async fn run(&mut self) -> CoreResult<()> {
        loop {
            select! {
                cmd = self.command_receiver.recv() => {
                    let cmd = match cmd {
                        Ok(cmd) => cmd,
                        Err(_) => return Ok(()),
                    };
                    match cmd {
                        Command::SubscribeWithHistory { asset, period, mode, command_id } => {
                            self.start_stream(asset, period, mode, command_id).await?;
                        }
                    }
                }
                msg = self.message_receiver.recv() => {
                    let msg = match msg {
                        Ok(msg) => msg,
                        Err(_) => return Ok(()),
                    };
                    self.handle_message(msg.as_ref()).await;
                }
            }
        }
    }

    fn rule(_: Arc<State>) -> Box<dyn Rule + Send + Sync> {
        Box::new(MultiPatternRule::new(vec![
            "updateHistory",
            "updateHistoryNewFast",
            "updateHistoryNew",
            "updateStream",
        ]))
    }
}

impl ChartStreamApiModule {
    async fn start_stream(
        &mut self,
        asset: String,
        period: u32,
        mode: HistoryStreamMode,
        command_id: Uuid,
    ) -> CoreResult<()> {
        let (stream_sender, stream_receiver) = bounded_async(STREAM_CHANNEL_CAPACITY);

        if let Err(error) = self.send_change_symbol(&asset, period).await {
            self.command_responder
                .send(CommandResponse::Failed {
                    command_id,
                    error: Box::new(error.into()),
                })
                .await?;
            return Ok(());
        }

        self.pending.push(PendingStream {
            asset,
            period,
            mode,
            sender: stream_sender,
        });
        self.command_responder
            .send(CommandResponse::Subscribed {
                command_id,
                stream_receiver,
            })
            .await?;
        Ok(())
    }

    async fn send_change_symbol(&self, asset: &str, period: u32) -> CoreResult<()> {
        let payload = serde_json::json!([
            "changeSymbol",
            {
                "asset": asset,
                "period": period,
            }
        ]);
        self.to_ws_sender
            .send(Message::text(format!(
                "42{}",
                serde_json::to_string(&payload)?
            )))
            .await
            .map_err(CoreError::from)
    }

    async fn handle_message(&mut self, msg: &Message) {
        if let Some(history) = parse_history_response(msg) {
            self.handle_history(history).await;
            return;
        }

        for row in parse_stream_rows(msg) {
            self.handle_stream_row(row).await;
        }
    }

    async fn handle_history(&mut self, history: HistoryResponse) {
        let Some(index) = self
            .pending
            .iter()
            .position(|pending| pending.asset == history.asset && pending.period == history.period)
        else {
            return;
        };
        let pending = self.pending.remove(index);
        let points = merge_history_points(
            &history.asset,
            history.period,
            history.history.as_deref(),
            history.candles.as_deref(),
        );
        let edge_time = points
            .iter()
            .map(|point| point.time)
            .filter(|time| time.is_finite())
            .reduce(f64::max)
            .unwrap_or(f64::NEG_INFINITY);

        match pending.mode {
            HistoryStreamMode::Points => {
                for point in points {
                    if pending
                        .sender
                        .send(Ok(HistoryStreamEvent::Point(point)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            HistoryStreamMode::Ohlc => {
                let candles = merge_history_ohlc(
                    &history.asset,
                    history.period,
                    history.history.as_deref(),
                    history.candles.as_deref(),
                );
                for candle in candles {
                    if pending
                        .sender
                        .send(Ok(HistoryStreamEvent::Candle(candle)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }

        self.active.push(ActiveStream {
            asset: pending.asset,
            mode: pending.mode,
            edge_time,
            sender: pending.sender,
            live_ohlc: SubscriptionType::time(Duration::from_secs(history.period as u64)),
        });
    }

    async fn handle_stream_row(&mut self, row: StreamRow) {
        for active in &mut self.active {
            if row.asset != active.asset || row.timestamp <= active.edge_time {
                continue;
            }

            let event = match active.mode {
                HistoryStreamMode::Points => match row.price.to_f64() {
                    Some(price) => Ok(Some(HistoryStreamEvent::Point(HistoryPoint {
                        asset: active.asset.clone(),
                        time: row.timestamp,
                        price,
                    }))),
                    None => Err(PocketError::General(format!(
                        "Failed to convert live price {} for {} at {}",
                        row.price, row.asset, row.timestamp
                    ))),
                },
                HistoryStreamMode::Ohlc => match row.price.to_f64() {
                    Some(price) => active
                        .live_ohlc
                        .update(&BaseCandle::from((row.timestamp.floor() as i64, price)))
                        .and_then(|maybe_base| {
                            maybe_base
                                .map(|base| {
                                    Candle::try_from((base, active.asset.clone()))
                                        .map(HistoryStreamEvent::Candle)
                                        .map_err(|err| PocketError::General(err.to_string()))
                                })
                                .transpose()
                        }),
                    None => Err(PocketError::General(format!(
                        "Failed to convert live price {} for {} at {}",
                        row.price, row.asset, row.timestamp
                    ))),
                },
            };

            active.edge_time = row.timestamp;
            match event {
                Ok(Some(event)) => {
                    let _ = active.sender.send(Ok(event)).await;
                }
                Ok(None) => {}
                Err(err) => {
                    let _ = active.sender.send(Err(err)).await;
                }
            }
        }
    }
}

fn parse_history_response(msg: &Message) -> Option<HistoryResponse> {
    let value = message_json_value(msg)?;
    if value.get("asset").is_some() && value.get("period").is_some() {
        return serde_json::from_value(value).ok();
    }
    let arr = value.as_array()?;
    let event = arr.first()?.as_str()?;
    if !event.starts_with("updateHistory") {
        return None;
    }
    serde_json::from_value(arr.get(1)?.clone()).ok()
}

fn parse_stream_rows(msg: &Message) -> Vec<StreamRow> {
    let Some(value) = message_json_value(msg) else {
        return Vec::new();
    };

    let rows = if let Some(arr) = value.as_array() {
        if arr.first().and_then(|item| item.as_str()) == Some("updateStream") {
            arr.get(1).and_then(|item| item.as_array()).cloned()
        } else {
            Some(arr.clone())
        }
    } else {
        None
    };

    rows.unwrap_or_default()
        .into_iter()
        .filter_map(parse_stream_row)
        .collect()
}

fn parse_stream_row(value: serde_json::Value) -> Option<StreamRow> {
    let row = value.as_array()?;
    let asset = row.first()?.as_str()?.to_string();
    let timestamp = row.get(1)?.as_f64()?;
    let price = Decimal::from_f64_retain(row.get(2)?.as_f64()?)?;
    Some(StreamRow {
        asset,
        timestamp,
        price,
    })
}

fn message_json_value(msg: &Message) -> Option<serde_json::Value> {
    match msg {
        Message::Text(text) => {
            let trimmed = text.trim();
            if trimmed.starts_with('{') || trimmed.starts_with('[') {
                serde_json::from_str(trimmed).ok()
            } else {
                text.find('[')
                    .and_then(|start| serde_json::from_str(&text[start..]).ok())
            }
        }
        Message::Binary(data) => serde_json::from_slice(data).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pocketoption::ssid::Ssid;
    use crate::pocketoption::state::StateBuilder;
    use binary_options_tools_core::reimports::{bounded_async, Message};
    use binary_options_tools_core::traits::ApiModule;
    use futures_util::StreamExt;
    use rust_decimal::prelude::ToPrimitive;
    use std::{sync::Arc, time::Duration};
    use tokio::time::timeout;

    fn test_state() -> Arc<crate::pocketoption::state::State> {
        let dummy_ssid =
            r#"42["auth",{"session":"dummy_session","isDemo":1,"uid":123,"platform":2}]"#;
        let ssid = Ssid::parse(dummy_ssid).expect("dummy SSID parses");
        Arc::new(
            StateBuilder::default()
                .ssid(ssid)
                .build()
                .expect("state builds"),
        )
    }

    async fn spawn_module() -> (
        ChartStreamHandle,
        binary_options_tools_core::reimports::AsyncSender<Arc<Message>>,
        binary_options_tools_core::reimports::AsyncReceiver<Message>,
    ) {
        let (cmd_tx, cmd_rx) = bounded_async(10);
        let (resp_tx, resp_rx) = bounded_async(10);
        let (msg_tx, msg_rx) = bounded_async(10);
        let (ws_tx, ws_rx) = bounded_async(10);
        let (runner_tx, _runner_rx) = bounded_async(1);
        let mut module =
            ChartStreamApiModule::new(test_state(), cmd_rx, resp_tx, msg_rx, ws_tx, runner_tx);
        tokio::spawn(async move {
            let _ = module.run().await;
        });
        (
            ChartStreamApiModule::create_handle(cmd_tx, resp_rx),
            msg_tx,
            ws_rx,
        )
    }

    async fn assert_only_change_symbol(
        ws_rx: &binary_options_tools_core::reimports::AsyncReceiver<Message>,
        asset: &str,
        period: u32,
    ) {
        let ws_msg = ws_rx.recv().await.expect("changeSymbol is sent");
        match ws_msg {
            Message::Text(text) => assert_eq!(
                text,
                format!(r#"42["changeSymbol",{{"asset":"{asset}","period":{period}}}]"#)
            ),
            _ => panic!("expected text websocket message"),
        }
        let extra = timeout(Duration::from_millis(50), ws_rx.recv()).await;
        assert!(
            extra.is_err(),
            "must not send subscribeSymbol, subfor, or any second subscribe frame"
        );
    }

    async fn assert_no_event(
        stream: &mut futures_util::stream::BoxStream<'_, PocketResult<HistoryStreamEvent>>,
    ) {
        let result = timeout(Duration::from_millis(50), stream.next()).await;
        assert!(
            result.is_err(),
            "stale or wrong-asset data must not produce an event"
        );
    }

    #[tokio::test]
    async fn subscribe_with_history_points_sends_one_change_symbol_and_filters_live_rows() {
        let (handle, msg_tx, ws_rx) = spawn_module().await;
        let asset = "EURUSD_otc";
        let period = 5;

        let mut stream = handle
            .subscribe_with_history_mode(asset.to_string(), period, HistoryStreamMode::Points)
            .await
            .expect("subscription starts")
            .to_stream()
            .boxed();

        assert_only_change_symbol(&ws_rx, asset, period).await;

        msg_tx
            .send(Arc::new(Message::Text(
                r#"42["updateHistoryNewFast",{"asset":"EURUSD_otc","period":5,"history":[[100,1.10],[105,1.20]]}]"#
                    .to_string()
                    .into(),
            )))
            .await
            .expect("history delivered");

        match stream.next().await.expect("first event").expect("ok event") {
            HistoryStreamEvent::Point(point) => {
                assert_eq!(point.asset, asset);
                assert_eq!(point.time, 100.0);
                assert_eq!(point.price, 1.10);
            }
            event => panic!("expected point, got {event:?}"),
        }
        match stream
            .next()
            .await
            .expect("second event")
            .expect("ok event")
        {
            HistoryStreamEvent::Point(point) => {
                assert_eq!(point.asset, asset);
                assert_eq!(point.time, 105.0);
                assert_eq!(point.price, 1.20);
            }
            event => panic!("expected point, got {event:?}"),
        }

        msg_tx
            .send(Arc::new(Message::Text(
                r#"42["updateStream",[["EURUSD_otc",105,1.30]]]"#.to_string().into(),
            )))
            .await
            .expect("stale live delivered");
        assert_no_event(&mut stream).await;

        msg_tx
            .send(Arc::new(Message::Text(
                r#"42["updateStream",[["GBPUSD_otc",106,1.40]]]"#.to_string().into(),
            )))
            .await
            .expect("wrong asset live delivered");
        assert_no_event(&mut stream).await;

        msg_tx
            .send(Arc::new(Message::Text(
                r#"42["updateStream",[["EURUSD_otc",106,1.50]]]"#.to_string().into(),
            )))
            .await
            .expect("matching live delivered");
        match stream.next().await.expect("live event").expect("ok event") {
            HistoryStreamEvent::Point(point) => {
                assert_eq!(point.asset, asset);
                assert_eq!(point.time, 106.0);
                assert_eq!(point.price, 1.50);
            }
            event => panic!("expected live point, got {event:?}"),
        }

        msg_tx
            .send(Arc::new(Message::Text(
                r#"42["updateStream",[["EURUSD_otc",106.5,1.60]]]"#.to_string().into(),
            )))
            .await
            .expect("fractional live delivered");
        match timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("fractional live event")
            .expect("stream item")
            .expect("ok event")
        {
            HistoryStreamEvent::Point(point) => {
                assert_eq!(point.asset, asset);
                assert_eq!(point.time, 106.5);
                assert_eq!(point.price, 1.60);
            }
            event => panic!("expected fractional live point, got {event:?}"),
        }
    }

    #[tokio::test]
    async fn subscribe_with_history_ohlc_bootstraps_candles_and_uses_subscription_time_update_for_live_rows(
    ) {
        let (handle, msg_tx, ws_rx) = spawn_module().await;
        let asset = "EURUSD_otc";
        let period = 2;

        let mut stream = handle
            .subscribe_with_history_mode(asset.to_string(), period, HistoryStreamMode::Ohlc)
            .await
            .expect("subscription starts")
            .to_stream()
            .boxed();

        assert_only_change_symbol(&ws_rx, asset, period).await;

        msg_tx
            .send(Arc::new(Message::Text(
                r#"42["updateHistoryNew",{"asset":"EURUSD_otc","period":2,"history":[[100,1.0],[101,1.2],[102,1.3],[103,1.1],[104,1.4]]}]"#
                    .to_string()
                    .into(),
            )))
            .await
            .expect("history delivered");

        match stream
            .next()
            .await
            .expect("first candle")
            .expect("ok event")
        {
            HistoryStreamEvent::Candle(candle) => {
                assert_eq!(candle.symbol, asset);
                assert_eq!(candle.timestamp, 100);
                assert_eq!(candle.open.to_f64().unwrap(), 1.0);
                assert_eq!(candle.close.to_f64().unwrap(), 1.2);
            }
            event => panic!("expected candle, got {event:?}"),
        }
        match stream
            .next()
            .await
            .expect("second candle")
            .expect("ok event")
        {
            HistoryStreamEvent::Candle(candle) => {
                assert_eq!(candle.symbol, asset);
                assert_eq!(candle.timestamp, 102);
                assert_eq!(candle.open.to_f64().unwrap(), 1.3);
                assert_eq!(candle.close.to_f64().unwrap(), 1.1);
            }
            event => panic!("expected candle, got {event:?}"),
        }

        msg_tx
            .send(Arc::new(Message::Text(
                r#"42["updateStream",[["EURUSD_otc",104,1.8],["GBPUSD_otc",105,1.9]]]"#
                    .to_string()
                    .into(),
            )))
            .await
            .expect("ignored live delivered");
        assert_no_event(&mut stream).await;

        for (timestamp, price) in [(105, 2.0), (106, 2.2), (107, 1.9)] {
            msg_tx
                .send(Arc::new(Message::Text(
                    format!(r#"42["updateStream",[["EURUSD_otc",{timestamp},{price}]]]"#).into(),
                )))
                .await
                .expect("matching live delivered");
        }

        match stream.next().await.expect("live candle").expect("ok event") {
            HistoryStreamEvent::Candle(candle) => {
                assert_eq!(candle.symbol, asset);
                assert_eq!(candle.timestamp, 107);
                assert_eq!(candle.open.to_f64().unwrap(), 2.0);
                assert_eq!(candle.high.to_f64().unwrap(), 2.2);
                assert_eq!(candle.low.to_f64().unwrap(), 1.9);
                assert_eq!(candle.close.to_f64().unwrap(), 1.9);
            }
            event => panic!("expected live candle, got {event:?}"),
        }
    }
}
