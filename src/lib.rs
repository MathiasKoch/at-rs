#![cfg_attr(not(test), no_std)]

#[macro_use]
extern crate nb;

mod buffer;
pub mod client;
mod error;
mod parser;
mod traits;
pub mod utils;

pub type MaxCommandLen = heapless::consts::U64;
pub type MaxResponseLen = heapless::consts::U64;
pub type MaxResponseLines = heapless::consts::U50;

pub use self::buffer::Buffer;
pub use self::error::Error;
pub use self::parser::ATParser;
pub use self::traits::{ATCommandInterface, ATInterface, ATRequestType};

#[cfg(test)]
mod tests;

use embedded_hal::{serial, timer::CountDown};
use heapless::{spsc::Queue, ArrayLength};

type ReqQueue<Req, N> = Queue<Req, N, u8>;
type ResQueue<Res, N> = Queue<Result<Res, error::Error>, N, u8>;
type ClientParser<Serial, T, Req, RxBufferLen, ReqQueueLen, ResQueueLen> = (
    client::ATClient<T, Req, ReqQueueLen, ResQueueLen>,
    parser::ATParser<Serial, Req, RxBufferLen, ReqQueueLen, ResQueueLen>,
);
pub type Response<Req> = <<Req as ATRequestType>::Command as ATCommandInterface>::Response;

pub fn new<Serial, Req, T, RxBufferLen, ReqQueueLen, ResQueueLen>(
    queues: (
        &'static mut ReqQueue<Req, ReqQueueLen>,
        &'static mut ResQueue<Response<Req>, ResQueueLen>,
    ),
    serial: Serial,
    timer: T,
    default_timeout: T::Time,
) -> ClientParser<Serial, T, Req, RxBufferLen, ReqQueueLen, ResQueueLen>
where
    Serial: serial::Write<u8> + serial::Read<u8>,
    RxBufferLen: ArrayLength<u8>,
    ReqQueueLen: ArrayLength<Req>,
    ResQueueLen: ArrayLength<Result<Response<Req>, error::Error>>,
    Req: ATRequestType,
    Req::Command: ATCommandInterface + PartialEq,
    Response<Req>: core::fmt::Debug,
    T: CountDown,
{
    let (wifi_req_p, wifi_req_c) = queues.0.split();
    let (wifi_res_p, wifi_res_c) = queues.1.split();

    let client = client::ATClient::new((wifi_req_p, wifi_res_c), default_timeout, timer);
    let parser = ATParser::new(serial, (wifi_req_c, wifi_res_p));

    (client, parser)
}
