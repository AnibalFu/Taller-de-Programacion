use chrono::{DateTime, Duration, Utc};

pub struct SaveTimer {
    next_save: DateTime<Utc>,
    save_interval: Duration,
}

impl SaveTimer {
    pub fn new(save_interval_secs: i64) -> Self {
        SaveTimer {
            save_interval: Duration::milliseconds(save_interval_secs),
            next_save: Utc::now() + Duration::milliseconds(save_interval_secs),
        }
    }

    pub fn set_next_save(&mut self) {
        self.next_save = Utc::now() + self.save_interval;
    }

    pub fn should_save(&self) -> bool {
        Utc::now() >= self.next_save
    }
}
