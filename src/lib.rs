use std::fmt::Display;

pub mod adv;

#[derive(Debug, Clone)]
struct WeatherRecord {
    city: String,
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

    fn new(city: &str, item: i32) -> Self {
        Self {
            city: city.to_owned(),
            min: item,
            max: item,
            sum: item as i64,
            count: 1,
        }
    }

    fn merge(&mut self, rhs: &Self) {
        self.count += rhs.count;
        self.min = self.min.min(rhs.min);
        self.max = self.max.max(rhs.max);
        self.sum += rhs.sum;
    }
}

impl Display for WeatherRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mean = self.sum as f32 / self.count as f32;
        write!(
            f,
            "{}: {:.1}/{:.1}/{:.1}",
            self.city,
            self.min as f32 / 10.,
            mean / 10.,
            self.max as f32 / 10.
        )
    }
}
