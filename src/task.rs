use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use anyhow::Result;
use csv::ReaderBuilder;
use nohash_hasher::IntMap;
use serde::Deserialize;
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Notify,
    },
    task::{spawn_blocking, JoinSet},
};
use ustr::Ustr;

use crate::WeatherRecord;

type Ticket = Arc<Notify>;

#[derive(Debug, Deserialize)]
struct WeatherCSVRecord {
    station: Ustr,
    measure: f32,
}

pub async fn process(path: PathBuf) -> Result<()> {
    let mut set = JoinSet::new();
    let mut channels: IntMap<u16, UnboundedSender<WeatherCSVRecord>> = IntMap::default();
    let mut ticket = Ticket::default();
    ticket.notify_one();

    // Assuming each city name starts with uppercase letter
    for index in 65..91_u16 {
        let (sender, ticket_task) = spawn_processing_task(&mut set, ticket);
        ticket = ticket_task;
        let _ = channels.insert(index, sender);
    }

    //Handling non english letters
    for index in 195..197_u16 {
        let (sender, ticket_task) = spawn_processing_task(&mut set, ticket);
        ticket = ticket_task;
        let _ = channels.insert(index, sender);
    }

    let _ = spawn_blocking(move || reader_sender(path, channels)).await;
    while (set.join_next().await).is_some() {}
    Ok(())
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

fn spawn_processing_task(
    set: &mut JoinSet<()>,
    prev_ticket: Ticket,
) -> (UnboundedSender<WeatherCSVRecord>, Ticket) {
    let (sender, receiver) = unbounded_channel();
    let ticket = Ticket::default();
    let ticket_cloned = ticket.clone();

    set.spawn(async move {
        task(receiver, prev_ticket, ticket).await;
    });
    (sender, ticket_cloned)
}

async fn task(
    mut receiver: UnboundedReceiver<WeatherCSVRecord>,
    prev_ticket: Ticket,
    next_ticket: Ticket,
) {
    let mut map: BTreeMap<Ustr, WeatherRecord> = BTreeMap::new();
    while let Some(record) = receiver.recv().await {
        let WeatherCSVRecord { station, measure } = record;
        match map.get_mut(&station) {
            Some(elem) => {
                elem.update(measure);
            }
            None => {
                map.insert(station, WeatherRecord::new(measure));
            }
        }
    }

    prev_ticket.notified().await;
    map.iter()
        .for_each(|(key, value)| println!("{key}={value}"));
    next_ticket.notify_one();
}
