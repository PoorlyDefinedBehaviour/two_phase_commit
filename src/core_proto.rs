pub mod core_proto {
    tonic::include_proto!("core");
}

pub use core_proto::*;

pub type HostAddr = String;
