#![forbid(unsafe_code)]

pub const API_MAJOR: u32 = 1;
pub const API_MINOR: u32 = 0;

pub mod v1 {
    tonic::include_proto!("erps.v1");
}
