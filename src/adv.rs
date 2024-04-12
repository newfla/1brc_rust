use std::{
    fs::File,
    os::unix::fs::{FileExt, MetadataExt},
    path::PathBuf,
    str::from_utf8_unchecked,
    sync::{Arc, Mutex},
    thread,
};

use ahash::AHashMap;
use anyhow::Result;
use ustr::UstrMap;

use crate::WeatherRecord;

//Based on https://github.com/coriolinus/1brc

/// Size of chunk that each thread will process at a time
const CHUNK_SIZE: u64 = 3 * 1024 * 1024;
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
) -> &'a [u8] {
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

    file.read_exact_at(buffer, read_from).unwrap();

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

    &buffer[head..=tail]
}

pub fn process(path: PathBuf) -> Result<()> {
    let file = std::fs::File::open(path)?;
    let x = &file;
    let file_size = file.metadata()?.size();
    let mut offset = 0u64;
    let map = Arc::new(Mutex::new(UstrMap::default()));
    let num_thread = thread::available_parallelism().map(Into::into).unwrap_or(1);
    thread::scope(|scope| {
        for _ in 0..num_thread {
            let mut map = map.clone();
            scope.spawn(move || {
                reader_sender(x, offset, num_thread as u64, file_size, &mut map);
            });
            offset += CHUNK_SIZE;
        }
    });
    let map = Arc::into_inner(map).unwrap().into_inner().unwrap();
    let mut keys = map.keys().collect::<Vec<_>>();
    keys.sort_unstable();

    for key in keys {
        let record = map[key];
        println!("{key}: {record}");
    }
    Ok(())
}

fn reader_sender(
    file: &File,
    mut offset: u64,
    num_thread: u64,
    file_size: u64,
    outer_map: &mut Arc<Mutex<UstrMap<WeatherRecord>>>,
) {
    let mut buffer = vec![0; (CHUNK_SIZE + CHUNK_EXCESS) as usize];
    let mut map: UstrMap<WeatherRecord> = UstrMap::default();
    let jump = CHUNK_SIZE * num_thread;

    while offset < file_size {
        // totally safe by FAQ assumptions
        unsafe {
            let buf = get_aligned_buffer(file, offset, &mut buffer, file_size);
            let mut loop_map: AHashMap<&str, WeatherRecord> = AHashMap::default();
            for line in from_utf8_unchecked(buf).lines() {
                let (station, temp) = line.split_once(';').unwrap();
                let measure = temp.parse().unwrap();

                match loop_map.get_mut(station) {
                    Some(elem) => {
                        elem.update(measure);
                    }
                    None => {
                        loop_map.insert(station, WeatherRecord::new(measure));
                    }
                }
            }
            for (city, records) in loop_map.into_iter() {
                map.entry(city.into())
                    .and_modify(|outer_records| *outer_records += records)
                    .or_insert(records);
            }
        }
        offset += jump;
    }

    let mut outer = outer_map.lock().unwrap();
    for (city, records) in map.into_iter() {
        outer
            .entry(city)
            .and_modify(|outer_records| *outer_records += records)
            .or_insert(records);
    }
}
