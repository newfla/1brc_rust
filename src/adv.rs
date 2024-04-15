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
//Based on https://github.com/thebracket/one_billion_rows

const MINUS: u8 = b'-';
const SEMICOLON: u8 = b';';
const NEWLINE: u8 = b'\n';

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

    // for some reason escluding tail byte to avoid !line.is_empty() causes a performance degradation
    &buffer[head..=tail]
}

fn parse_ascii_digits(buffer: &[u8]) -> i32 {
    let size = buffer.len();
    let mut negative_mul = 1;
    let mut accumulator = 0;
    let mut positional_mul = 10_i32.pow(size as u32 - 2);

    if MINUS == buffer[0] {
        negative_mul = -1;
    } else {
        let digit = buffer[0] as i32 - 48;
        accumulator += digit * positional_mul;
    }

    positional_mul /= 10;

    for item in buffer.iter().take(size - 2).skip(1) {
        let digit = *item as i32 - 48;
        accumulator += digit * positional_mul;
        positional_mul /= 10;
    }

    let digit = buffer[size - 1] as i32 - 48;
    accumulator += digit;

    accumulator *= negative_mul;
    accumulator
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
                reader(x, offset, num_thread as u64, file_size, &mut map);
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

fn reader(
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
        let mut loop_map: AHashMap<&[u8], WeatherRecord> = AHashMap::default();
        let buf = get_aligned_buffer(file, offset, &mut buffer, file_size);

        for line in buf
            .split(|val| val == &NEWLINE)
            .filter(|line| !line.is_empty())
        {
            let split_point = line
                .iter()
                .enumerate()
                .find_map(|(id, val)| (val == &SEMICOLON).then_some(id))
                .unwrap();

            let measure = parse_ascii_digits(&line[split_point + 1..]);
            let station = &line[..split_point];

            match loop_map.get_mut(station) {
                Some(elem) => {
                    elem.update(measure);
                }
                None => {
                    loop_map.insert(station, WeatherRecord::new(measure));
                }
            }
        }
        unsafe {
            for (city, records) in loop_map.into_iter() {
                map.entry(from_utf8_unchecked(city).into())
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
