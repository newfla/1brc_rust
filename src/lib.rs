use std::{
    fmt::Display,
    ops::{Add, AddAssign},
};

pub mod adv;

#[derive(Debug, Clone, Copy)]
struct WeatherRecord {
    min: i32,
    max: i32,
    sum: i64,
    count: u32,
}
impl WeatherRecord {
    fn update(&mut self, item: i32) {
        self.count += 1;
        self.min = self.min.min(item);
        self.max = self.max.max(item);
        self.sum += item as i64;
    }

    fn new(item: i32) -> Self {
        Self {
            min: item,
            max: item,
            sum: item as i64,
            count: 1,
        }
    }
}

impl Add for WeatherRecord {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            count: self.count + rhs.count,
            min: self.min.min(rhs.min),
            max: self.max.max(rhs.max),
            sum: self.sum + rhs.sum,
        }
    }
}

impl AddAssign for WeatherRecord {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
    }
}
impl Display for WeatherRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mean = self.sum as f32 / self.count as f32;
        write!(
            f,
            "{:.1}/{:.1}/{:.1}",
            self.min as f32 / 10.,
            mean / 10.,
            self.max as f32 / 10.
        )
    }
}
