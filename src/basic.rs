use std::{collections::BTreeMap, fmt::Display, path::PathBuf, sync::Arc};

use anyhow::Result;
use csv::{ByteRecord, ReaderBuilder};
use csv_async::{AsyncDeserializer, AsyncReaderBuilder};
use nohash_hasher::IntMap;
use serde::Deserialize;
use tokio::{
    fs::File,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Notify,
    },
    task::{spawn_blocking, JoinSet},
};
use tokio_stream::StreamExt;
use ustr::{Ustr, UstrMap};

type Ticket = Arc<Notify>;

#[derive(Debug, Deserialize)]
struct WeatherCSVRecord {
    station: Ustr,
    measure: f32,
}

struct WeatherRecord {
    min: f32,
    max: f32,
    sum: f64,
    count: u32,
}

impl Display for WeatherRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mean = self.sum / self.count as f64;
        write!(f, "{:.1}/{:.1}/{:.1}", self.min, mean, self.max)
    }
}

pub async fn async_process(path: PathBuf) -> Result<()> {
    let mut reader = AsyncReaderBuilder::new()
        .has_headers(false)
        .delimiter(b';')
        .create_deserializer(File::open(path).await?);

    let mut iter = reader.deserialize::<WeatherCSVRecord>();
    while let Some(Ok(record)) = iter.next().await {}
    // for result in reader.deserialize() {
    //     let record: WeatherCSVRecord = result?;
    //     count += 1;
    //     println!("{count}");
    //     match map.get_mut(&record.station) {
    //         Some(val) => {
    //             if record.measure > val.max {
    //                 val.max = record.measure;
    //             } else {
    //                 val.min = record.measure;
    //             }
    //             val.mean = (val.mean + record.measure) / 2.;
    //         }
    //         None => {
    //             let _ = map.insert(
    //                 record.station,
    //                 WeatherRecord {
    //                     min: record.measure,
    //                     max: record.measure,
    //                     mean: record.measure,
    //                 },
    //             );
    //         }
    //     }
    // }
    Ok(())
}

pub async fn process(path: PathBuf) -> Result<()> {
    let mut set = JoinSet::new();
    let mut channels: IntMap<u16, UnboundedSender<WeatherCSVRecord>> = IntMap::default();
    let mut ticket = Ticket::default();
    ticket.notify_one();
    // Assuming each city name starts with uppercase letter
    for index in 65..91_u16 {
        ticket = spawn_task(&mut set, &mut channels, ticket, index);
    }

    //Handling non english letters
    ticket = spawn_task(&mut set, &mut channels, ticket, 195);
    let _ = spawn_task(&mut set, &mut channels, ticket, 196);

    let _ = spawn_blocking(move || reader_sender(path, channels)).await;
    while (set.join_next().await).is_some() {}
    Ok(())
}

fn spawn_task(
    set: &mut JoinSet<()>,
    channels: &mut IntMap<u16, UnboundedSender<WeatherCSVRecord>>,
    prev_ticket: Ticket,
    index: u16,
) -> Ticket {
    let (sender, receiver) = unbounded_channel();
    let ticket = Ticket::default();
    let ticket_cloned = ticket.clone();
    let _ = channels.insert(index, sender);
    set.spawn(async move {
        single_task(receiver, prev_ticket, ticket).await;
    });
    ticket_cloned
}

async fn single_task(
    mut receiver: UnboundedReceiver<WeatherCSVRecord>,
    prev_ticket: Ticket,
    next_ticket: Ticket,
) {
    //let mut map: UstrMap<WeatherRecord> = UstrMap::default();
    let mut map: BTreeMap<Ustr, WeatherRecord> = BTreeMap::new();
    // let limit = 500;
    // let mut received = 1;
    // let mut buffer: Vec<WeatherCSVRecord> = Vec::with_capacity(limit);
    // while received > 0 {
    //     received = receiver.recv_many(&mut buffer, limit).await;
    //     buffer.iter().take(received).for_each(|record| {
    //         let WeatherCSVRecord { station, measure } = record;
    //         match map.get_mut(&station) {
    //             Some(elem) => {
    //                 elem.min = measure.min(elem.min);
    //                 elem.max = measure.max(elem.max);
    //                 elem.sum += *measure as f64;
    //                 elem.count += 1;
    //             }
    //             None => {
    //                 map.insert(
    //                     *station,
    //                     WeatherRecord {
    //                         min: *measure,
    //                         max: *measure,
    //                         sum: *measure as f64,
    //                         count: 1,
    //                     },
    //                 );
    //             }
    //         }
    //     });
    // }
    while let Some(record) = receiver.recv().await {
        let WeatherCSVRecord { station, measure } = record;
        match map.get_mut(&station) {
            Some(elem) => {
                elem.min = measure.min(elem.min);
                elem.max = measure.max(elem.max);
                elem.sum += measure as f64;
                elem.count += 1;
            }
            None => {
                map.insert(
                    station,
                    WeatherRecord {
                        min: measure,
                        max: measure,
                        sum: measure as f64,
                        count: 1,
                    },
                );
            }
        }
    }

    prev_ticket.notified().await;
    map.iter()
        .for_each(|(key, value)| println!("{key}={value}"));
    next_ticket.notify_one();
}

fn reader_sender(
    path: PathBuf,
    channels: IntMap<u16, UnboundedSender<WeatherCSVRecord>>,
) -> Result<()> {
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b';')
        .from_path(path)?;

    let mut iter = reader.deserialize::<WeatherCSVRecord>();

    while let Some(Ok(record)) = iter.next() {
        let index = record.station.as_bytes()[0] as u16;
        let channel = channels.get(&index).unwrap();
        let _ = channel.send(record);
    }
    Ok(())
}
