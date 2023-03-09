pub mod core_proto {
    tonic::include_proto!("core");
}

pub use core_proto::*;

pub type HostAddr = String;

pub const HOST_ADDR_HEADER_KEY: &str = "x-host-addr";
