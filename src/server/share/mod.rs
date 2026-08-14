pub mod manager;
pub mod partition;

pub use manager::{ShareGroupManager, SHARE_GROUP_STATE_MAGIC};
pub use partition::{InFlightBatch, InFlightBatch as InFlightRecord, SharePartition, ShareRecordState};
