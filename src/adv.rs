use std::{
    fs::File,
    os::unix::fs::{FileExt, MetadataExt},
    path::PathBuf,
    str::from_utf8_unchecked,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
};

use anyhow::Result;
use ustr::{ustr, UstrMap};

use crate::WeatherRecord;

//Based on https://github.com/coriolinus/1brc

/// Size of chunk that each thread will process at a time
const CHUNK_SIZE: u64 = 4 * 1024 * 1024;
/// How much extra space we back the chunk start up by, to ensure we capture the full initial record
///
/// Must be greater than the longest line in the table
const CHUNK_EXCESS: u64 = 64;

/// Get an aligned buffer from the given file.
///
/// "Aligned" in this case means that the first byte of the returned buffer is the
/// first byte of a record, and if `offset != 0` then the previous byte of the source file is `\n`,
/// and the final byte of the returned buffer is `\n`.
fn get_aligned_buffer<'a>(
    file: &File,
    offset: u64,
    mut buffer: &'a mut [u8],
    file_size: u64,
) -> Result<&'a [u8]> {
    let buffer_size = buffer.len().min((file_size - offset) as usize);
    buffer = &mut buffer[..buffer_size];

    let mut head;
    let read_from;

    if offset == 0 {
        head = 0;
        read_from = 0;
    } else {
        head = CHUNK_EXCESS as usize;
        read_from = offset - CHUNK_EXCESS;
    };

    file.read_exact_at(buffer, read_from)?;

    // step backwards until we find the end of the previous record
    // then drop all elements before that
    while head > 0 {
        if buffer[head - 1] == b'\n' {
            break;
        }
        head -= 1;
    }

    // find the end of the final valid record
    let mut tail = buffer.len() - 1;
    while buffer[tail] != b'\n' {
        tail -= 1;
    }

    Ok(&buffer[head..=tail])
}

pub fn process(path: PathBuf) -> Result<()> {
    // let mut set = JoinSet::new();
    // let mut channels: IntMap<u16, UnboundedSender<WeatherCSVRecord>> = IntMap::default();
    // let mut ticket = Ticket::default();
    // ticket.notify_one();

    // // Assuming each city name starts with uppercase letter
    // for index in 65..91_u16 {
    //     let (sender, ticket_task) = spawn_processing_task(&mut set, ticket);
    //     ticket = ticket_task;
    //     let _ = channels.insert(index, sender);
    // }

    // //Handling non english letters
    // for index in 195..197_u16 {
    //     let (sender, ticket_task) = spawn_processing_task(&mut set, ticket);
    //     ticket = ticket_task;
    //     let _ = channels.insert(index, sender);
    // }
    let file = std::fs::File::open(path)?;
    let x = &file;
    let file_size = file.metadata()?.size();
    let offset = Arc::new(AtomicU64::new(0));
    let map = Arc::new(Mutex::new(UstrMap::default()));
    thread::scope(|scope| {
        for _ in 0..thread::available_parallelism().map(Into::into).unwrap_or(1) {
            let offset = offset.clone();
            let mut map = map.clone();
            scope.spawn(move || {
                let _ = reader_sender(x, offset, file_size, &mut map);
            });
        }
    });
    let map = Arc::into_inner(map).unwrap().into_inner().unwrap();
    let mut keys = map.keys().collect::<Vec<_>>();
    keys.sort_unstable();

    for key in keys {
        let record = map[key];

        println!("{key}: {record}");
    }

    // for _ in 0..16 {
    //     let path = path.clone();
    //     let offset = offset.clone();
    //     //let channels = channels.clone();
    //     let _ = spawn_blocking(move || reader_sender(path, channels, offset, file_size)).await;
    // }
    // drop(channels);

    // while (set.join_next().await).is_some() {}
    // Ok(())
    Ok(())
}

fn reader_sender(
    file: &File,
    offset: Arc<AtomicU64>,
    file_size: u64,
    outer_map: &mut Arc<Mutex<UstrMap<WeatherRecord>>>,
) -> Result<()> {
    let mut buffer = vec![0; (CHUNK_SIZE + CHUNK_EXCESS) as usize];
    let mut map: UstrMap<WeatherRecord> = UstrMap::default();
    loop {
        let offset = offset.fetch_add(CHUNK_SIZE, Ordering::SeqCst);
        if offset > file_size {
            break;
        }
        // totally safe by FAQ assumptions
        unsafe {
            let buf = get_aligned_buffer(file, offset, &mut buffer, file_size)?;
            for line in from_utf8_unchecked(buf).lines() {
                let (station, temp) = line.split_once(';').unwrap();
                let measure: f32 = temp.parse().unwrap();
                let temp = ustr(station);

                match map.get_mut(&temp) {
                    Some(elem) => {
                        elem.update(measure);
                    }
                    None => {
                        map.insert(temp, WeatherRecord::new(measure));
                    }
                }
            }
        }

        // for line in buf.split(|&b| b == b'\n').filter(|line| !line.is_empty()) {
        //     // let idx = line
        //     //     .iter()
        //     //     .enumerate()
        //     //     .find_map(|(idx, &b)| (b == b';').then_some(idx))
        //     //     .unwrap();

        //     let (station, temp) = from_utf8(line)?.split_once(';').unwrap();
        // //    / let measure: f32 = from_utf8(&line[idx + 1..]).unwrap().parse()?;
        //     //let temp = ustr(from_utf8(&line[..idx])?);
        //     let measure: f32 = temp.parse().unwrap();
        //     let temp = ustr(station);

        //     match map.get_mut(&temp) {
        //         Some(elem) => {
        //             elem.min = measure.min(elem.min);
        //             elem.max = measure.max(elem.max);
        //             elem.sum += measure as f64;
        //             elem.count += 1;
        //         }
        //         None => {
        //             map.insert(
        //                 temp,
        //                 WeatherRecord {
        //                     min: measure,
        //                     max: measure,
        //                     sum: measure as f64,
        //                     count: 1,
        //                 },
        //             );
        //         }
        //     }
        // }
    }

    let mut outer = outer_map.lock().expect("non-poisoned mutex");
    for (city, records) in map.into_iter() {
        outer
            .entry(city)
            .and_modify(|outer_records| *outer_records += records)
            .or_insert(records);
    }
    // println!("exit");
    Ok(())
}
