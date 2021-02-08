use crate::error::Error;
use crate::Mode;
use heapless::{ArrayLength, Vec};

pub trait AtatErr {}

/// This trait needs to be implemented for every response type.
///
/// Example:
/// ```
/// use atat::AtatResp;
///
/// pub struct GreetingText {
///     pub text: heapless::String<heapless::consts::U64>,
/// }
///
/// impl AtatResp for GreetingText {}
/// ```
pub trait AtatResp {}

pub trait AtatUrc {
    /// The type of the response. Usually the enum this trait is implemented on.
    type Response;

    /// Parse the response into a `Self::Response` instance.
    fn parse(resp: &[u8]) -> Result<Self::Response, Error>;
}

/// This trait needs to be implemented for every command type.
///
/// It can also be derived by the [`atat_derive`] crate.
///
/// [`atat_derive`]: https://crates.io/crates/atat_derive
///
/// Example:
/// ```
/// use atat::{AtatCmd, AtatResp, Error};
/// use core::fmt::Write;
/// use heapless::Vec;
///
/// pub struct SetGreetingText<'a> {
///     pub text: &'a str,
/// }
///
/// pub struct NoResponse;
///
/// impl AtatResp for NoResponse {};
///
/// impl<'a> AtatCmd for SetGreetingText<'a> {
///     type CommandLen = heapless::consts::U64;
///     type Response = NoResponse;
///
///     fn as_bytes(&self) -> Vec<u8, Self::CommandLen> {
///         let mut buf: Vec<u8, Self::CommandLen> = Vec::new();
///         write!(buf, "AT+CSGT={}", self.text);
///         buf
///     }
///
///     fn parse(&self, resp: &[u8]) -> Result<Self::Response, Error> {
///         Ok(NoResponse)
///     }
/// }
/// ```
pub trait AtatCmd {
    /// The max length of the command.
    ///
    /// Example: For the command "AT+RST" you would specify
    ///
    /// ```
    /// type CommandLen = heapless::consts::U6;
    /// ```
    type CommandLen: ArrayLength<u8>;

    /// The type of the response. Must implement the `AtatResp` trait.
    type Response: AtatResp;

    /// Return the command as a heapless `Vec` of bytes.
    fn as_bytes(&self) -> Vec<u8, Self::CommandLen>;

    /// Parse the response into a `Self::Response` instance.
    fn parse(&self, resp: &[u8]) -> Result<Self::Response, Error>;

    /// Whether or not this command can be aborted.
    fn can_abort(&self) -> bool {
        false
    }

    /// The max timeout in milliseconds.
    fn max_timeout_ms(&self) -> u32 {
        1000
    }

    /// Force the ingress manager into receive state immediately after sending
    /// the command.
    fn force_receive_state(&self) -> bool {
        false
    }
}

pub trait AtatClient {
    /// Send an AT command.
    ///
    /// `cmd` must implement [`AtatCmd`].
    ///
    /// This function will block until a response is received, if in Timeout or
    /// Blocking mode. In Nonblocking mode, the send can be called until it no
    /// longer returns `nb::Error::WouldBlock`, or `self.check_response(cmd)` can
    /// be called, with the same result.
    ///
    /// This function will also make sure that atleast `self.config.cmd_cooldown`
    /// has passed since the last response or URC has been received, to allow
    /// the slave AT device time to deliver URC's.
    fn send<A: AtatCmd>(&mut self, cmd: &A) -> nb::Result<A::Response, Error>;

    /// Checks if there are any URC's (Unsolicited Response Code) in
    /// queue from the ingress manager.
    ///
    /// Example:
    /// ```
    /// use atat::atat_derive::{AtatResp, AtatUrc};
    ///
    /// #[derive(Clone, AtatResp)]
    /// pub struct MessageWaitingIndication {
    ///     #[at_arg(position = 0)]
    ///     pub status: u8,
    ///     #[at_arg(position = 1)]
    ///     pub code: u8,
    /// }
    ///
    /// #[derive(Clone, AtatUrc)]
    /// pub enum Urc {
    ///     #[at_urc("+UMWI")]
    ///     MessageWaitingIndication(MessageWaitingIndication),
    /// }
    ///
    /// // match client.check_urc::<Urc>() {
    /// //     Some(Urc::MessageWaitingIndication(MessageWaitingIndication { status, code })) => {
    /// //         // Do something to act on `+UMWI` URC
    /// //     }
    /// // }
    /// ```
    fn check_urc<URC: AtatUrc>(&mut self) -> Option<URC::Response> {
        let mut return_urc = None;
        self.peek_urc_with::<URC, _>(|urc| {
            return_urc = Some(urc);
            true
        });
        return_urc
    }

    fn peek_urc_with<URC: AtatUrc, F: FnOnce(URC::Response) -> bool>(&mut self, f: F);

    /// Check if there are any responses enqueued from the ingress manager.
    ///
    /// The function will return `nb::Error::WouldBlock` until a response or an
    /// error is available, or a timeout occurs and `config.mode` is Timeout.
    ///
    /// This function is usually only called through [`send`].
    ///
    /// [`send`]: #method.send
    fn check_response<A: AtatCmd>(&mut self, cmd: &A) -> nb::Result<A::Response, Error>;

    /// Get the configured mode of the client.
    ///
    /// Options are:
    /// - `NonBlocking`
    /// - `Blocking`
    /// - `Timeout`
    fn get_mode(&self) -> Mode;
}

impl<T, L> AtatResp for heapless::Vec<T, L>
where
    T: AtatResp,
    L: ArrayLength<T>,
{
}

impl<L> AtatResp for heapless::String<L> where L: ArrayLength<u8> {}

impl<L> AtatCmd for heapless::String<L>
where
    L: ArrayLength<u8>,
{
    type CommandLen = L;

    type Response = heapless::String<heapless::consts::U256>;

    fn as_bytes(&self) -> Vec<u8, Self::CommandLen> {
        self.clone().into_bytes()
    }

    fn parse(&self, resp: &[u8]) -> Result<Self::Response, Error> {
        heapless::String::from_utf8(
            Vec::from_slice(resp).map_err(|_| Error::ParseString)?,
        ).map_err(|_| Error::ParseString)
    }
}

#[cfg(all(test, feature = "derive"))]
mod test {
    use super::*;
    use crate as atat;
    use atat_derive::{AtatEnum, AtatResp};
    use heapless::{consts, String};

    #[derive(Debug, Clone, PartialEq, AtatEnum)]
    pub enum PDPContextStatus {
        /// 0: deactivated
        Deactivated = 0,
        /// 1: activated
        Activated = 1,
    }

    #[derive(Debug, Clone, AtatResp, PartialEq)]
    pub struct PDPContextState {
        #[at_arg(position = 0)]
        pub cid: u8,
        #[at_arg(position = 1)]
        pub status: PDPContextStatus,
    }

    #[derive(Debug, Clone, AtatResp, PartialEq)]
    pub struct PDPContextDefinition {
        #[at_arg(position = 0)]
        pub cid: u8,
        #[at_arg(position = 1)]
        pub pdp_type: String<consts::U6>,
        #[at_arg(position = 2)]
        pub apn: String<consts::U99>,
        #[at_arg(position = 3)]
        pub pdp_addr: String<consts::U99>,
        #[at_arg(position = 4)]
        pub d_comp: u8,
        #[at_arg(position = 5)]
        pub h_comp: u8,
        #[at_arg(position = 6)]
        pub ipv4_addr_alloc: Option<u8>,
        #[at_arg(position = 7)]
        pub emergency_indication: Option<u8>,
        #[at_arg(position = 8)]
        pub p_cscf_discovery: Option<u8>,
        #[at_arg(position = 9)]
        pub im_cn_signalling_flag_ind: Option<u8>,
        /* #[at_arg(position = 10)]
         * pub nslpi: Option<u8>, */
    }

    #[test]
    fn single_multi_response() {
        let mut v = Vec::<_, heapless::consts::U5>::from_slice(&[PDPContextState {
            cid: 1,
            status: PDPContextStatus::Deactivated,
        }])
        .unwrap();

        let mut resp: heapless::Vec<PDPContextState, heapless::consts::U5> =
            serde_at::from_slice(b"+CGACT: 1,0\r\n").unwrap();

        assert_eq!(resp.pop(), v.pop());
        assert_eq!(resp.pop(), None);
    }

    #[test]
    fn multi_response() {
        let mut v = Vec::<_, heapless::consts::U3>::from_slice(&[
            PDPContextState {
                cid: 1,
                status: PDPContextStatus::Deactivated,
            },
            PDPContextState {
                cid: 2,
                status: PDPContextStatus::Activated,
            },
            PDPContextState {
                cid: 3,
                status: PDPContextStatus::Deactivated,
            },
        ])
        .unwrap();

        let mut resp: heapless::Vec<PDPContextState, heapless::consts::U3> =
            serde_at::from_slice(b"+CGACT: 1,0\r\n+CGACT: 2,1\r\n+CGACT: 3,0").unwrap();

        assert_eq!(resp.pop(), v.pop());
        assert_eq!(resp.pop(), v.pop());
        assert_eq!(resp.pop(), v.pop());
        assert_eq!(resp.pop(), None);
    }

    #[test]
    fn multi_response_advanced() {
        let mut v = Vec::<_, heapless::consts::U3>::from_slice(&[
            PDPContextDefinition {
                cid: 2,
                pdp_type: String::from("IP"),
                apn: String::from("em"),
                pdp_addr: String::from("100.92.188.66"),
                d_comp: 0,
                h_comp: 0,
                ipv4_addr_alloc: Some(0),
                emergency_indication: Some(0),
                p_cscf_discovery: Some(0),
                im_cn_signalling_flag_ind: Some(0),
            },
            PDPContextDefinition {
                cid: 1,
                pdp_type: String::from("IP"),
                apn: String::from("STATREAL"),
                pdp_addr: String::from("0.0.0.0"),
                d_comp: 0,
                h_comp: 0,
                ipv4_addr_alloc: None,
                emergency_indication: None,
                p_cscf_discovery: None,
                im_cn_signalling_flag_ind: None,
            },
            PDPContextDefinition {
                cid: 3,
                pdp_type: String::from("IP"),
                apn: String::from("tim.ibox.it"),
                pdp_addr: String::from("0.0.0.0"),
                d_comp: 0,
                h_comp: 0,
                ipv4_addr_alloc: None,
                emergency_indication: None,
                p_cscf_discovery: None,
                im_cn_signalling_flag_ind: None,
            },
        ])
        .unwrap();

        let mut resp: heapless::Vec<PDPContextDefinition, heapless::consts::U3> =
            serde_at::from_slice(b"+CGDCONT: 2,\"IP\",\"em\",\"100.92.188.66\",0,0,0,0,0,0\r\n+CGDCONT: 1,\"IP\",\"STATREAL\",\"0.0.0.0\",0,0\r\n+CGDCONT: 3,\"IP\",\"tim.ibox.it\",\"0.0.0.0\",0,0").unwrap();

        assert_eq!(resp.pop(), v.pop());
        assert_eq!(resp.pop(), v.pop());
        assert_eq!(resp.pop(), v.pop());
        assert_eq!(resp.pop(), None);
    }
}
