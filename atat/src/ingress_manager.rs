use heapless::{consts, ArrayLength, Vec};

use crate::atat_log;
use crate::error::Error;
use crate::queues::{ComConsumer, ComItem, ResItem, ResProducer, UrcItem, UrcProducer};
use crate::{Command, Config};

type ByteVec<L> = Vec<u8, L>;

trait SliceExt {
    fn trim(&self, whitespaces: &[u8]) -> &Self;
    fn trim_start(&self, whitespaces: &[u8]) -> &Self;
}

impl SliceExt for [u8] {
    fn trim(&self, whitespaces: &[u8]) -> &[u8] {
        let is_not_whitespace = |c| !whitespaces.contains(c);

        if let Some(first) = self.iter().position(is_not_whitespace) {
            if let Some(last) = self.iter().rposition(is_not_whitespace) {
                &self[first..=last]
            } else {
                unreachable!();
            }
        } else {
            &[]
        }
    }

    fn trim_start(&self, whitespaces: &[u8]) -> &[u8] {
        let is_not_whitespace = |c| !whitespaces.contains(c);
        self.iter()
            .position(is_not_whitespace)
            .map_or(&[], |first| &self[first..])
    }
}

/// Helper function to take a subsection from `buf`.
///
/// It searches for `needle`, either from the beginning of buf, or the end,
/// depending on `reverse`. If the search finds a match, it continues forward as
/// long as the next characters matches `line_term_char` or `format_char`. It
/// then returns a substring, trimming it for whitespaces if `trim_response` is
/// true, and leaves the remainder in `buf`.
///
/// Example:
/// ```
/// use atat::get_line;
/// use heapless::{consts, Vec};
///
/// let mut buf: Vec<u8, consts::U128> =
///     Vec::from_slice(b"+USORD: 3,16,\"16 bytes of data\"\r\nOK\r\nAT+GMR\r\r\n").unwrap();
/// let response: Option<Vec<u8, consts::U64>> =
///     get_line(&mut buf, b"OK", b'\r', b'\n', false, false);
/// assert_eq!(
///     response,
///     Some(Vec::from_slice(b"+USORD: 3,16,\"16 bytes of data\"\r\nOK\r\n").unwrap())
/// );
/// assert_eq!(
///     buf,
///     Vec::<u8, consts::U128>::from_slice(b"AT+GMR\r\r\n").unwrap()
/// );
/// ```
pub fn get_line<L: ArrayLength<u8>, I: ArrayLength<u8>>(
    buf: &mut Vec<u8, I>,
    needle: &[u8],
    line_term_char: u8,
    format_char: u8,
    trim_response: bool,
    reverse: bool,
) -> Option<Vec<u8, L>> {
    if buf.len() == 0 {
        return None;
    }

    let ind = if reverse {
        buf.windows(needle.len())
            .rposition(|window| window == needle)
    } else {
        buf.windows(needle.len())
            .position(|window| window == needle)
    };

    match ind {
        Some(index) => {
            let white_space = buf
                .iter()
                .skip(index + needle.len())
                .position(|c| ![format_char, line_term_char].contains(c))
                .unwrap_or(buf.len() - index - needle.len());

            let (left, right) = buf.split_at(index + needle.len() + white_space);

            let return_buf = if trim_response {
                left.trim(&[b'\t', b' ', format_char, line_term_char])
            } else {
                left
            }
            .iter()
            .cloned()
            .collect();

            *buf = right.iter().cloned().collect();
            Some(return_buf)
        }
        None => None,
    }
}

/// State of the `IngressManager`, used to distiguish URCs from solicited
/// responses
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, defmt::Format)]
pub enum State {
    Idle,
    ReceivingResponse,
}

/// The type returned from a custom URC matcher.
pub enum UrcMatcherResult<L: ArrayLength<u8>> {
    NotHandled,
    Incomplete,
    Complete(Vec<u8, L>),
}

/// A user-defined URC matcher
///
/// This is used to detect and consume URCs that are not terminated with
/// standard response codes like "OK". An example could be an URC that returns
/// length-value (LV) encoded data without a terminator.
///
/// Note that you should only detect and consume but not process URCs.
/// Processing should be done by an [`AtatUrc`](trait.AtatUrc.html)
/// implementation.
///
/// A very simplistic example that can only handle the URC `+FOO,xx` (with
/// `xx` being two arbitrary characters) followed by CRLF:
///
/// ```
/// use atat::{UrcMatcher, UrcMatcherResult};
/// use heapless::{consts, ArrayLength, Vec};
///
/// struct FooUrcMatcher {}
///
/// impl<BufLen: ArrayLength<u8>> UrcMatcher<BufLen> for FooUrcMatcher {
///     fn process(&mut self, buf: &mut Vec<u8, BufLen>) -> UrcMatcherResult<BufLen> {
///         if buf.starts_with(b"+FOO,") {
///             if buf.len() >= 9 {
///                 if &buf[7..9] == b"\r\n" {
///                     // URC is complete
///                     let data = Vec::from_slice(&buf[..9]).unwrap();
///                     *buf = Vec::from_slice(&buf[9..]).unwrap();
///                     UrcMatcherResult::Complete(data)
///                 } else {
///                     // Invalid, reject
///                     UrcMatcherResult::NotHandled
///                 }
///             } else {
///                 // Insufficient data
///                 UrcMatcherResult::Incomplete
///             }
///         } else {
///             UrcMatcherResult::NotHandled
///         }
///     }
/// }
/// ```
pub trait UrcMatcher<BufLen: ArrayLength<u8>> {
    /// Take a look at `buf`. Then:
    ///
    /// - If the buffer contains a full URC, remove these bytes from the buffer
    ///   and return [`Complete`] with the matched data.
    /// - If it contains an incomplete URC, return [`Incomplete`].
    /// - Otherwise, return [`NotHandled`].
    ///
    /// [`Complete`]: enum.UrcMatcherResult.html#variant.Complete
    /// [`Incomplete`]: enum.UrcMatcherResult.html#variant.Incomplete
    /// [`NotHandled`]: enum.UrcMatcherResult.html#variant.NotHandled
    fn process(&mut self, buf: &mut ByteVec<BufLen>) -> UrcMatcherResult<BufLen>;
}

/// A URC matcher that does nothing (it always returns [`NotHandled`][nothandled]).
///
/// [nothandled]: enum.UrcMatcherResult.html#variant.NotHandled
pub struct NoopUrcMatcher {}

impl<BufLen: ArrayLength<u8>> UrcMatcher<BufLen> for NoopUrcMatcher {
    fn process(&mut self, _: &mut ByteVec<BufLen>) -> UrcMatcherResult<BufLen> {
        UrcMatcherResult::NotHandled
    }
}

pub struct IngressManager<
    BufLen = consts::U256,
    U = NoopUrcMatcher,
    ComCapacity = consts::U3,
    ResCapacity = consts::U5,
    UrcCapacity = consts::U10,
> where
    BufLen: ArrayLength<u8>,
    U: UrcMatcher<BufLen>,
    ComCapacity: ArrayLength<ComItem>,
    ResCapacity: ArrayLength<ResItem<BufLen>>,
    UrcCapacity: ArrayLength<UrcItem<BufLen>>,
{
    /// Buffer holding incoming bytes.
    buf: ByteVec<BufLen>,
    /// A flag that is set to `true` when the buffer is cleared
    /// with an incomplete response.
    buf_incomplete: bool,

    /// The response producer sends responses to the client
    res_p: ResProducer<BufLen, ResCapacity>,
    /// The URC producer sends URCs to the client
    urc_p: UrcProducer<BufLen, UrcCapacity>,
    /// The command consumer receives commands from the client
    com_c: ComConsumer<ComCapacity>,

    /// Current processing state.
    state: State,
    /// Command line termination character S3 (Default = b'\r' ASCII: \[013\])
    line_term_char: u8,
    /// Response formatting character S4 (Default = b'\n' ASCII: \[010\])
    format_char: u8,
    echo_enabled: bool,

    /// Custom URC matcher.
    custom_urc_matcher: Option<U>,
}

impl<BufLen, ComCapacity, ResCapacity, UrcCapacity>
    IngressManager<BufLen, NoopUrcMatcher, ComCapacity, ResCapacity, UrcCapacity>
where
    BufLen: ArrayLength<u8>,
    ComCapacity: ArrayLength<ComItem>,
    ResCapacity: ArrayLength<ResItem<BufLen>>,
    UrcCapacity: ArrayLength<UrcItem<BufLen>>,
{
    #[must_use]
    pub fn new(
        res_p: ResProducer<BufLen, ResCapacity>,
        urc_p: UrcProducer<BufLen, UrcCapacity>,
        com_c: ComConsumer<ComCapacity>,
        config: Config,
    ) -> Self {
        Self::with_custom_urc_matcher(res_p, urc_p, com_c, config, None)
    }
}

impl<BufLen, U, ComCapacity, ResCapacity, UrcCapacity>
    IngressManager<BufLen, U, ComCapacity, ResCapacity, UrcCapacity>
where
    U: UrcMatcher<BufLen>,
    BufLen: ArrayLength<u8>,
    ComCapacity: ArrayLength<ComItem>,
    ResCapacity: ArrayLength<ResItem<BufLen>>,
    UrcCapacity: ArrayLength<UrcItem<BufLen>>,
{
    pub fn with_custom_urc_matcher(
        res_p: ResProducer<BufLen, ResCapacity>,
        urc_p: UrcProducer<BufLen, UrcCapacity>,
        com_c: ComConsumer<ComCapacity>,
        config: Config,
        custom_urc_matcher: Option<U>,
    ) -> Self {
        Self {
            state: State::Idle,
            buf: Vec::new(),
            buf_incomplete: false,
            res_p,
            urc_p,
            com_c,
            line_term_char: config.line_term_char,
            format_char: config.format_char,
            echo_enabled: config.at_echo_enabled,
            custom_urc_matcher,
        }
    }

    /// Write data into the internal buffer raw bytes being the core type allows
    /// the ingress manager to be abstracted over the communication medium.
    ///
    /// This function should be called by the UART Rx, either in a receive
    /// interrupt, or a DMA interrupt, to move data from the peripheral into the
    /// ingress manager receive buffer.
    pub fn write(&mut self, data: &[u8]) {
        // atat_log!(debug, "Received response: \"{:?}\"", data);

        if self.buf.extend_from_slice(data).is_err() {
            self.notify_response(Err(Error::Overflow));
        }
    }

    /// Notify the client that an appropriate response code, or error has been
    /// received
    fn notify_response(&mut self, resp: Result<ByteVec<BufLen>, Error>) {
        match &resp {
            Ok(_r) => {
                if _r.is_empty() {
                    atat_log!(debug, "Received OK")
                } else {
                    #[allow(clippy::single_match)]
                    match core::str::from_utf8(_r) {
                        Ok(_s) => {
                            #[cfg(not(feature = "log-logging"))]
                            atat_log!(debug, "Received response: \"{:str}\"", _s);
                            #[cfg(feature = "log-logging")]
                            atat_log!(debug, "Received response \"{:?}\"", _s)
                        }
                        Err(_) => atat_log!(
                            debug,
                            "Received response: {:?}",
                            core::convert::AsRef::<[u8]>::as_ref(&_r)
                        ),
                    };
                }
            }
            Err(_e) => atat_log!(error, "Received error response: {:?}", _e),
        }
        if self.res_p.ready() {
            unsafe { self.res_p.enqueue_unchecked(resp) };
        } else {
            // FIXME: Handle queue not being ready
            atat_log!(error, "Response queue full!");
        }
    }

    /// Notify the client that an unsolicited response code (URC) has been
    /// received
    fn notify_urc(&mut self, resp: ByteVec<BufLen>) {
        #[allow(clippy::single_match)]
        match core::str::from_utf8(&resp) {
            Ok(_s) => {
                #[cfg(not(feature = "log-logging"))]
                atat_log!(debug, "Received URC: {:str}", _s);
                #[cfg(feature = "log-logging")]
                atat_log!(debug, "Received URC: {:?}", _s);
            }
            Err(_) => atat_log!(
                debug,
                "Received URC: {:?}",
                core::convert::AsRef::<[u8]>::as_ref(&resp)
            ),
        };

        if self.urc_p.ready() {
            unsafe { self.urc_p.enqueue_unchecked(resp) };
        } else {
            // FIXME: Handle queue not being ready
            atat_log!(error, "URC queue full!");
        }
    }

    /// Handle receiving internal config commands from the client.
    fn handle_com(&mut self) {
        if let Some(com) = self.com_c.dequeue() {
            match com {
                Command::ClearBuffer => {
                    self.state = State::Idle;
                    self.buf_incomplete = false;
                    // #[allow(clippy::single_match)]
                    // match core::str::from_utf8(&self.buf) {
                    //     Ok(_s) => {
                    //         #[cfg(not(feature = "log-logging"))]
                    //         atat_log!(debug, "Clearing buffer on timeout / {:str}", _s);
                    //         #[cfg(feature = "log-logging")]
                    //         atat_log!(debug, "Clearing buffer on timeout / {:?}", _s);
                    //     }
                    //     Err(_) => atat_log!(
                    //         debug,
                    //         "Clearing buffer on timeout / {:?}",
                    //         core::convert::AsRef::<[u8]>::as_ref(&self.buf)
                    //     ),
                    // };

                    self.clear_buf(true);
                }
                Command::ForceState(state) => {
                    atat_log!(trace, "Switching to state {:?}", state);
                    self.buf_incomplete = false;
                    self.state = state;
                }
                Command::SetEcho(e) => {
                    self.echo_enabled = e;
                }
                Command::SetFormat(c) => {
                    self.format_char = c;
                }
                Command::SetLineTerm(c) => {
                    self.line_term_char = c;
                }
            }
        }
    }

    /// Clear the buffer.
    ///
    /// If `complete` is `true`, clear the entire buffer. Otherwise, only
    /// remove data until (and including) the first newline (or the entire
    /// buffer if no newline is present).
    fn clear_buf(&mut self, complete: bool) {
        if complete {
            self.buf.clear();
            atat_log!(trace, "Cleared complete buffer");
        } else {
            let removed = get_line::<BufLen, _>(
                &mut self.buf,
                &[self.line_term_char],
                self.line_term_char,
                self.format_char,
                false,
                false,
            );
            #[allow(unused_variables)]
            if let Some(r) = removed {
                #[allow(clippy::single_match)]
                match core::str::from_utf8(&r) {
                    Ok(_s) => {
                        #[cfg(not(feature = "log-logging"))]
                        atat_log!(trace, "Cleared partial buffer, removed {:str}", _s);
                        #[cfg(feature = "log-logging")]
                        atat_log!(trace, "Cleared partial buffer, removed {:?}", _s);
                    }
                    Err(_) => atat_log!(
                        trace,
                        "Cleared partial buffer, removed {:?}",
                        core::convert::AsRef::<[u8]>::as_ref(&r)
                    ),
                };
            } else {
                self.buf.clear();
                atat_log!(trace, "Cleared partial buffer, removed everything");
            }
        }
    }

    /// Process the receive buffer, checking for AT responses, URC's or errors
    ///
    /// This function should be called regularly for the ingress manager to work
    pub fn digest(&mut self) {
        // Handle commands

        // Trim leading whitespace
        if self.buf.starts_with(&[self.line_term_char]) || self.buf.starts_with(&[self.format_char])
        {
            self.buf = Vec::from_slice(self.buf.trim_start(&[
                b'\t',
                b' ',
                self.format_char,
                self.line_term_char,
            ]))
            .unwrap();
        }

        #[allow(clippy::single_match)]
        match core::str::from_utf8(&self.buf) {
            Ok(_s) => {
                #[cfg(not(feature = "log-logging"))]
                atat_log!(trace, "Digest / {:str}", _s);
                #[cfg(feature = "log-logging")]
                atat_log!(trace, "Digest / {:?}", _s);
            }
            Err(_) => atat_log!(
                trace,
                "Digest / {:?}",
                core::convert::AsRef::<[u8]>::as_ref(&self.buf)
            ),
        };

        match self.state {
            State::Idle => {
                // The minimal buffer length that is required to identify all
                // types of responses (e.g. `AT` and `+`).
                let min_length = 2;

                // Echo is currently required
                if !self.echo_enabled {
                    unimplemented!("Disabling AT echo is currently unsupported");
                }

                // Handle AT echo responses
                if !self.buf_incomplete && self.buf.get(0..2) == Some(b"AT") {
                    if get_line::<BufLen, _>(
                        &mut self.buf,
                        &[self.line_term_char],
                        self.line_term_char,
                        self.format_char,
                        false,
                        false,
                    )
                    .is_some()
                    {
                        self.state = State::ReceivingResponse;
                        self.buf_incomplete = false;
                        atat_log!(trace, "Switching to state ReceivingResponse");
                    }

                // Handle URCs
                } else if !self.buf_incomplete && self.buf.get(0) == Some(&b'+') {
                    // Try to apply the custom URC matcher
                    let handled = match self.custom_urc_matcher {
                        Some(ref mut matcher) => match matcher.process(&mut self.buf) {
                            UrcMatcherResult::NotHandled => false,
                            UrcMatcherResult::Incomplete => true,
                            UrcMatcherResult::Complete(urc) => {
                                self.notify_urc(urc);
                                true
                            }
                        },
                        None => false,
                    };
                    if !handled {
                        if let Some(line) = get_line(
                            &mut self.buf,
                            &[self.line_term_char],
                            self.line_term_char,
                            self.format_char,
                            true,
                            false,
                        ) {
                            self.buf_incomplete = false;
                            self.notify_urc(line);
                        }
                    }

                // Text sent by the device that is not a valid response type (e.g. starting
                // with "AT" or "+") can be ignored. Clear the buffer, but only if we can
                // ensure that we don't accidentally break a valid response.
                } else if self.buf_incomplete || self.buf.len() > min_length {
                    atat_log!(
                        trace,
                        "Clearing buffer with invalid response (incomplete: {:?}, buflen: {:?})",
                        self.buf_incomplete,
                        self.buf.len()
                    );
                    self.buf_incomplete = self.buf.is_empty()
                        || (self.buf.len() > 0
                            && self.buf.get(self.buf.len() - 1) != Some(&self.line_term_char)
                            && self.buf.get(self.buf.len() - 1) != Some(&self.format_char));

                    self.clear_buf(false);

                    // If the buffer wasn't cleared completely, that means that
                    // a newline was found. In that case, the buffer cannot be
                    // in an incomplete state.
                    if !self.buf.is_empty() {
                        self.buf_incomplete = false;
                    }
                }
            }
            State::ReceivingResponse => {
                let resp = if let Some(mut line) = get_line::<BufLen, _>(
                    &mut self.buf,
                    b"OK",
                    self.line_term_char,
                    self.format_char,
                    true,
                    false,
                ) {
                    Ok(get_line(
                        &mut line,
                        &[self.line_term_char],
                        self.line_term_char,
                        self.format_char,
                        true,
                        true,
                    )
                    .unwrap_or_else(Vec::new))
                } else if let Some(_bytes) = get_line::<BufLen, _>(
                    &mut self.buf,
                    b"ERROR",
                    self.line_term_char,
                    self.format_char,
                    true,
                    false,
                ) {
                    #[allow(clippy::single_match)]
                    match core::str::from_utf8(&_bytes) {
                        Ok(_s) => {
                            #[cfg(not(feature = "log-logging"))]
                            atat_log!(error, "Received error response: {:str}", _s);
                            #[cfg(feature = "log-logging")]
                            atat_log!(error, "Received error response {:?}", _s)
                        }
                        Err(_) => atat_log!(
                            error,
                            "Received error response: {:?}",
                            core::convert::AsRef::<[u8]>::as_ref(&_bytes)
                        ),
                    };
                    Err(Error::InvalidResponse)
                } else if get_line::<BufLen, _>(
                    &mut self.buf,
                    b">",
                    self.line_term_char,
                    self.format_char,
                    false,
                    false,
                )
                .is_some()
                    || get_line::<BufLen, _>(
                        &mut self.buf,
                        b"@",
                        self.line_term_char,
                        self.format_char,
                        false,
                        false,
                    )
                    .is_some()
                {
                    Ok(Vec::new())
                } else {
                    return;
                };

                self.notify_response(resp);
                atat_log!(trace, "Switching to state Idle");
                self.state = State::Idle;
            }
        }
        self.handle_com();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate as atat;
    use crate::queues::{ComQueue, ResQueue, UrcQueue};
    use atat::Mode;
    use heapless::{consts, spsc::Queue};

    type TestRxBufLen = consts::U256;
    type TestComCapacity = consts::U3;
    type TestResCapacity = consts::U5;
    type TestUrcCapacity = consts::U10;

    macro_rules! setup {
        ($config:expr, $urch:expr) => {{
            static mut RES_Q: ResQueue<TestRxBufLen, TestResCapacity> =
                Queue(heapless::i::Queue::u8());
            let (res_p, res_c) = unsafe { RES_Q.split() };
            static mut URC_Q: UrcQueue<TestRxBufLen, TestUrcCapacity> =
                Queue(heapless::i::Queue::u8());
            let (urc_p, urc_c) = unsafe { URC_Q.split() };
            static mut COM_Q: ComQueue<TestComCapacity> = Queue(heapless::i::Queue::u8());
            let (_com_p, com_c) = unsafe { COM_Q.split() };
            (
                IngressManager::with_custom_urc_matcher(res_p, urc_p, com_c, $config, $urch),
                res_c,
                urc_c,
            )
        }};
        ($config:expr) => {{
            let val: (
                IngressManager<
                    TestRxBufLen,
                    NoopUrcMatcher,
                    TestComCapacity,
                    TestResCapacity,
                    TestUrcCapacity,
                >,
                _,
                _,
            ) = setup!($config, None);
            val
        }};
    }

    #[test]
    fn no_response() {
        let conf = Config::new(Mode::Timeout);
        let (mut at_pars, mut res_c, _urc_c) = setup!(conf);

        assert_eq!(at_pars.state, State::Idle);
        at_pars.write(b"AT\r\r\n\r\n");
        at_pars.digest();

        assert_eq!(at_pars.state, State::ReceivingResponse);

        at_pars.write(b"OK\r\n");
        at_pars.digest();
        assert_eq!(at_pars.state, State::Idle);
        assert_eq!(res_c.dequeue().unwrap(), Ok(Vec::new()));
    }

    #[test]
    fn response() {
        let conf = Config::new(Mode::Timeout);
        let (mut at_pars, mut res_c, _urc_c) = setup!(conf);

        assert_eq!(at_pars.state, State::Idle);
        at_pars.write(b"AT+USORD=3,16\r\n");
        at_pars.digest();
        assert_eq!(at_pars.state, State::ReceivingResponse);

        at_pars.write(b"+USORD: 3,16,\"16 bytes of data\"\r\n");
        at_pars.digest();

        {
            let expectation =
                Vec::<_, TestRxBufLen>::from_slice(b"+USORD: 3,16,\"16 bytes of data\"\r\n")
                    .unwrap();
            assert_eq!(at_pars.buf, expectation);
        }

        at_pars.write(b"OK\r\n");
        {
            let expectation =
                Vec::<_, TestRxBufLen>::from_slice(b"+USORD: 3,16,\"16 bytes of data\"\r\nOK\r\n")
                    .unwrap();
            assert_eq!(at_pars.buf, expectation);
        }
        at_pars.digest();
        assert_eq!(at_pars.buf, Vec::<_, TestRxBufLen>::new());
        assert_eq!(at_pars.state, State::Idle);
        {
            let expectation =
                Vec::<_, TestRxBufLen>::from_slice(b"+USORD: 3,16,\"16 bytes of data\"").unwrap();
            assert_eq!(res_c.dequeue().unwrap(), Ok(expectation));
        }
    }

    #[test]
    fn multi_line_response() {
        let conf = Config::new(Mode::Timeout);
        let (mut at_pars, mut res_c, _urc_c) = setup!(conf);

        assert_eq!(at_pars.state, State::Idle);
        at_pars.write(b"AT+GMR\r\r\n");
        at_pars.digest();
        assert_eq!(at_pars.state, State::ReceivingResponse);

        at_pars.write(b"AT version:1.1.0.0(May 11 2016 18:09:56)\r\nSDK version:1.5.4(baaeaebb)\r\ncompile time:May 20 2016 15:08:19\r\nOK\r\n");
        at_pars.digest();

        assert_eq!(at_pars.buf, Vec::<_, TestRxBufLen>::new());
        assert_eq!(at_pars.state, State::Idle);
        {
            let expectation = Vec::<_, TestRxBufLen>::from_slice(b"AT version:1.1.0.0(May 11 2016 18:09:56)\r\nSDK version:1.5.4(baaeaebb)\r\ncompile time:May 20 2016 15:08:19").unwrap();
            assert_eq!(res_c.dequeue().unwrap(), Ok(expectation));
        }
    }

    #[test]
    fn urc() {
        let conf = Config::new(Mode::Timeout);
        let (mut at_pars, _res_c, _urc_c) = setup!(conf);

        assert_eq!(at_pars.state, State::Idle);

        at_pars.write(b"+UUSORD: 3,16,\"16 bytes of data\"\r\n");
        at_pars.digest();
        assert_eq!(at_pars.buf, Vec::<_, TestRxBufLen>::new());
        assert_eq!(at_pars.state, State::Idle);
    }

    #[test]
    fn overflow() {
        let conf = Config::new(Mode::Timeout);
        let (mut at_pars, mut res_c, _urc_c) = setup!(conf);

        at_pars.write(b"+USORD: 3,266,\"");
        for _ in 0..266 {
            at_pars.write(b"s");
        }
        at_pars.write(b"\"\r\n");
        at_pars.digest();
        assert_eq!(res_c.dequeue().unwrap(), Err(Error::Overflow));
        assert_eq!(at_pars.state, State::Idle);
    }

    #[test]
    fn read_error() {
        let conf = Config::new(Mode::Timeout);
        let (mut at_pars, _res_c, _urc_c) = setup!(conf);

        assert_eq!(at_pars.state, State::Idle);

        assert_eq!(at_pars.buf, Vec::<_, TestRxBufLen>::new());
        at_pars.write(b"OK\r\n");
        at_pars.digest();

        assert_eq!(at_pars.state, State::Idle);
    }

    #[test]
    fn error_response() {
        let conf = Config::new(Mode::Timeout);
        let (mut at_pars, mut res_c, _urc_c) = setup!(conf);

        assert_eq!(at_pars.state, State::Idle);
        at_pars.write(b"AT+USORD=3,16\r\n");
        at_pars.digest();
        assert_eq!(at_pars.state, State::ReceivingResponse);

        at_pars.write(b"+USORD: 3,16,\"16 bytes of data\"\r\n");
        at_pars.digest();
        assert_eq!(at_pars.state, State::ReceivingResponse);

        at_pars.write(b"ERROR\r\n");
        at_pars.digest();

        assert_eq!(at_pars.state, State::Idle);
        assert_eq!(at_pars.buf, Vec::<_, TestRxBufLen>::new());
        assert_eq!(res_c.dequeue().unwrap(), Err(Error::InvalidResponse));
    }

    /// By breaking up non-AT-commands into chunks, it's possible that
    /// they're mistaken for AT commands due to buffer clearing.
    ///
    /// Regression test for #27.
    #[test]
    fn chunkwise_digest() {
        let conf = Config::new(Mode::Timeout);
        let (mut at_pars, _res_c, _urc_c) = setup!(conf);

        assert_eq!(at_pars.state, State::Idle);

        at_pars.write(b"THIS FORM");
        at_pars.digest();
        assert_eq!(at_pars.state, State::Idle);
        at_pars.write(b"AT SUCKS\r\n");
        at_pars.digest();
        assert_eq!(at_pars.state, State::Idle);
    }

    /// By sending AT-commands byte-by-byte, it's possible that
    /// the command is incorrectly ignored due to buffer clearing.
    ///
    /// Regression test for #27.
    #[test]
    fn bytewise_digest() {
        let conf = Config::new(Mode::Timeout);
        let (mut at_pars, _res_c, _urc_c) = setup!(conf);

        assert_eq!(at_pars.state, State::Idle);

        for byte in b"AT\r\n" {
            at_pars.write(&[*byte]);
            at_pars.digest();
        }
        assert_eq!(at_pars.state, State::ReceivingResponse);
    }

    /// If an invalid response ends with a line terminator, the incomplete flag
    /// should be cleared.
    #[test]
    fn invalid_line_with_termination() {
        let conf = Config::new(Mode::Timeout);
        let (mut at_pars, _res_c, _urc_c) = setup!(conf);

        assert_eq!(at_pars.state, State::Idle);

        at_pars.write(b"some status msg\r\n");
        at_pars.digest();
        assert_eq!(at_pars.state, State::Idle);

        at_pars.write(b"AT+GMR\r\r\n");
        at_pars.digest();
        assert_eq!(at_pars.state, State::ReceivingResponse);
    }

    /// If a valid response follows an invalid response, the buffer should not
    /// be cleared in between.
    #[test]
    fn mixed_response() {
        let conf = Config::new(Mode::Timeout);
        let (mut at_pars, _res_c, _urc_c) = setup!(conf);

        assert_eq!(at_pars.state, State::Idle);

        at_pars.write(b"some status msg\r\nAT+GMR\r\r\n");
        at_pars.digest();
        at_pars.digest();
        assert_eq!(at_pars.state, State::ReceivingResponse);
    }

    #[test]
    fn clear_buf_complete() {
        let conf = Config::new(Mode::Timeout);
        let (mut ingress, _res_c, _urc_c) = setup!(conf);

        ingress.write(b"hello\r\ngoodbye\r\n");
        assert_eq!(ingress.buf, b"hello\r\ngoodbye\r\n");

        ingress.clear_buf(true);
        assert_eq!(ingress.buf, b"");
    }

    #[test]
    fn clear_buf_partial() {
        let conf = Config::new(Mode::Timeout);
        let (mut ingress, _res_c, _urc_c) = setup!(conf);

        ingress.write(b"hello\r\nthere\r\ngoodbye\r\n");
        assert_eq!(ingress.buf, b"hello\r\nthere\r\ngoodbye\r\n");

        ingress.clear_buf(false);
        assert_eq!(ingress.buf, b"there\r\ngoodbye\r\n");

        ingress.clear_buf(false);
        assert_eq!(ingress.buf, b"goodbye\r\n");

        ingress.clear_buf(false);
        assert_eq!(ingress.buf, b"");
    }

    #[test]
    fn clear_buf_partial_no_newlines() {
        let conf = Config::new(Mode::Timeout);
        let (mut ingress, _res_c, _urc_c) = setup!(conf);

        ingress.write(b"no newlines anywhere");
        assert_eq!(ingress.buf, b"no newlines anywhere");
        ingress.clear_buf(false);
        assert_eq!(ingress.buf, b"");
    }

    #[test]
    fn custom_urc_matcher() {
        let conf = Config::new(Mode::Timeout);

        struct MyUrcMatcher {}
        impl UrcMatcher<TestRxBufLen> for MyUrcMatcher {
            fn process(
                &mut self,
                buf: &mut ByteVec<TestRxBufLen>,
            ) -> UrcMatcherResult<TestRxBufLen> {
                if buf.len() >= 6 && buf.get(0..6) == Some(b"+match") {
                    let data = buf.clone();
                    buf.truncate(0);
                    UrcMatcherResult::Complete(data)
                } else if buf.len() >= 4 && buf.get(0..4) == Some(b"+mat") {
                    UrcMatcherResult::Incomplete
                } else {
                    UrcMatcherResult::NotHandled
                }
            }
        }

        let (mut ingress, _res_c, mut urc_c) = setup!(conf, Some(MyUrcMatcher {}));

        // Initial state
        assert_eq!(ingress.state, State::Idle);
        assert_eq!(urc_c.dequeue(), None);

        // Check an URC that is not handled by MyUrcMatcher (fall back to default behavior)
        // Note that this requires the trailing newlines to be present!
        ingress.write(b"+default-behavior\r\n");
        ingress.digest();
        assert_eq!(ingress.state, State::Idle);
        assert_eq!(urc_c.dequeue().unwrap(), b"+default-behavior");

        // Check an URC that is generally handled by MyUrcMatcher but
        // considered incomplete (not enough data). This will not yet result in
        // an URC being dispatched.
        ingress.write(b"+mat");
        ingress.digest();
        assert_eq!(ingress.state, State::Idle);
        assert_eq!(urc_c.dequeue(), None);

        // Make it complete!
        ingress.write(b"ch"); // Still no newlines, but this will still be picked up!
        ingress.digest();
        assert_eq!(ingress.state, State::Idle);
        assert_eq!(urc_c.dequeue().unwrap(), b"+match");
    }

    #[test]
    fn trim() {
        assert_eq!(
            b"  hello  whatup  ".trim(&[b' ', b'\t', b'\r', b'\n']),
            b"hello  whatup"
        );
        assert_eq!(
            b"  hello  whatup  ".trim_start(&[b' ', b'\t', b'\r', b'\n']),
            b"hello  whatup  "
        );
        assert_eq!(
            b"  \r\n \thello  whatup  ".trim_start(&[b' ', b'\t', b'\r', b'\n']),
            b"hello  whatup  "
        );
        assert_eq!(
            b"  \r\n \thello  whatup  \n \t".trim(&[b' ', b'\t', b'\r', b'\n']),
            b"hello  whatup"
        );
    }
}
