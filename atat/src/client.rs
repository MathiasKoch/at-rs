use heapless::{consts, spsc::Consumer, String};

use embedded_hal::{serial, timer::CountDown};

use crate::error::{Error, NBResult, Result};
use crate::traits::{ATATCmd, ATATInterface};
use crate::{Config, Mode};

// use dynstack::{DynStack, dyn_push};

use log::{info, error};

type ResConsumer = Consumer<'static, Result<String<consts::U256>>, consts::U10, u8>;

#[derive(Debug, PartialEq)]
enum ClientState {
    Idle,
    AwaitingResponse,
}

pub struct ATClient<Tx, T>
where
    Tx: serial::Write<u8>,
    T: CountDown,
{
    tx: Tx,
    res_c: ResConsumer,
    // last_response_time: T::Time,
    state: ClientState,
    mode: Mode<T>,
    // handlers: DynStack<dyn Fn(&str)>,
}

impl<Tx, T> ATClient<Tx, T>
where
    Tx: serial::Write<u8>,
    T: CountDown,
{
    pub fn new(tx: Tx, queue: ResConsumer, config: Config<T>) -> Self {
        Self {
            tx,
            res_c: queue,
            state: ClientState::Idle,
            mode: config.mode,
            // handlers: DynStack::<dyn Fn(&str)>::new(),
        }
    }
}
impl<Tx, T> ATClient<Tx, T>
where
    Tx: serial::Write<u8>,
    T: CountDown,
{
    pub fn register_urc_handler<F>(&mut self, handler: &'static F) -> core::result::Result<(), ()>
    where
        F: for<'a> Fn(&'a str)
    {
        // dyn_push!(self.handlers, handler);
        Ok(())
    }
}

impl<Tx, T> ATATInterface for ATClient<Tx, T>
where
    Tx: serial::Write<u8>,
    T: CountDown,
    T::Time: From<u32>,
{
    fn send<A: ATATCmd>(&mut self, cmd: &A) -> NBResult<A::Response> {
        if let ClientState::Idle = self.state {
            for c in cmd.as_str().as_bytes() {
                block!(self.tx.write(*c)).ok();
            }
            block!(self.tx.flush()).ok();
            self.state = ClientState::AwaitingResponse;
        }

        let res = match self.mode {
            Mode::Blocking => block!(self.check_response(cmd)).map_err(nb::Error::Other)?,
            Mode::NonBlocking => self.check_response(cmd)?,
            Mode::Timeout(ref mut timer) => {
                timer.start(cmd.max_timeout_ms());
                block!(self.check_response(cmd)).map_err(nb::Error::Other)?
            }
        };

        match res {
            Some(r) => Ok(r),
            None => Err(nb::Error::WouldBlock),
        }
    }

    fn check_response<A: ATATCmd>(&mut self, cmd: &A) -> NBResult<Option<A::Response>> {
        if let Some(result) = self.res_c.dequeue() {
            return match result {
                Ok(resp) => {
                    if let ClientState::AwaitingResponse = self.state {
                        self.state = ClientState::Idle;
                        info!("{:?}\r", resp);
                        Ok(Some(cmd.parse(&resp).map_err(|e| {
                            error!("{:?}", e);
                            nb::Error::Other(e)
                        })?))
                    } else {
                        // URC
                        // for handler in self.handlers.iter() {
                        //     handler(&resp);
                        // };
                        Ok(None)
                    }
                }
                Err(e) => Err(nb::Error::Other(e)),
            };
        } else if let Mode::Timeout(ref mut timer) = self.mode {
            if timer.wait().is_ok() {
                self.state = ClientState::Idle;
                return Err(nb::Error::Other(Error::Timeout));
            }
        }
        Err(nb::Error::WouldBlock)
    }
}
#[cfg(test)]
#[cfg_attr(tarpaulin, skip)]
mod test {
    use super::*;
    use nb;
    use void::Void;
    use heapless::{consts, spsc::Queue, String, Vec};
    use crate::atat_derive::{ATATCmd, ATATResp};
    use crate as atat;
    use atat::{traits::ATATInterface};
    use serde;
    use serde_repr::{Serialize_repr, Deserialize_repr};

    struct CdMock{
        time : u32,
    }

    impl CountDown for CdMock{
        type Time = u32;
        fn start<T>(&mut self, count : T)
        where T: Into<Self::Time> {
            self.time = count.into();
        }
        fn wait(&mut self) -> nb::Result<(), Void>{
            Ok(())
        }
    }

    struct TxMock {
        s: String<consts::U64>,
    }

    impl TxMock {
        fn new(s: String<consts::U64>) -> Self {
            TxMock { s }
        }
    }

    impl serial::Write<u8> for TxMock {
        type Error = ();

        fn write(&mut self, c: u8) -> nb::Result<(), Self::Error> {
            //TODO: this just feels wrong..
            match self.s.push(c as char) {
                Ok(_) => Ok(()),
                Err(_) => Err(nb::Error::Other(())),
            }
        }

        fn flush(&mut self) -> nb::Result<(), Self::Error> {
            Ok(())
        }
    }

    #[derive(Clone, ATATCmd)]
    #[at_cmd("+CFUN", NoResonse, timeout_ms = 180000)]
    pub struct SetModuleFunctionality {
        #[at_arg(position = 0)]
        pub fun: Functionality,
        #[at_arg(position = 1)]
        pub rst: Option<ResetMode>,
    }

    #[derive(Clone, ATATCmd)]
    #[at_cmd("+FUN", NoResonse, timeout_ms = 180000)]
    pub struct Test2Cmd {
        #[at_arg(position = 1)]
        pub fun: Functionality,
        #[at_arg(position = 0)]
        pub rst: Option<ResetMode>,
    }
    #[derive(Clone, ATATCmd)]
    #[at_cmd("+CUN", TestResponseVec, timeout_ms = 180000)]
    pub struct TestRespVecCmd {
        #[at_arg(position = 0)]
        pub fun: Functionality,
        #[at_arg(position = 1)]
        pub rst: Option<ResetMode>,
    }
    #[derive(Clone, ATATCmd)]
    #[at_cmd("+CUN", TestResponseString, timeout_ms = 180000)]
    pub struct TestRespStringCmd {
        #[at_arg(position = 0)]
        pub fun: Functionality,
        #[at_arg(position = 1)]
        pub rst: Option<ResetMode>,
    }
    #[derive(Clone, ATATCmd)]
    #[at_cmd("+CUN", TestResponseStringMixed, timeout_ms = 180000)]
    pub struct TestRespStringMixCmd {
        #[at_arg(position = 1)]
        pub fun: Functionality,
        #[at_arg(position = 0)]
        pub rst: Option<ResetMode>,
    }


    #[derive(Clone, PartialEq, Serialize_repr, Deserialize_repr)]
    #[repr(u8)]
    pub enum Functionality {
        Min = 0,
        Full = 1,
        APM = 4,
        DM = 6,
    }
    #[derive(Clone, PartialEq, Serialize_repr, Deserialize_repr)]
    #[repr(u8)]
    pub enum ResetMode {
        DontReset = 0,
        Reset = 1,
    }
    #[derive(Clone, ATATResp, PartialEq, Debug)]
    pub struct NoResonse;
    #[derive(Clone, ATATResp, PartialEq, Debug)]
    pub struct TestResponseVec {
        #[at_arg(position = 0)]
        pub socket: u8,
        #[at_arg(position = 1)]
        pub length: usize,
        #[at_arg(position = 2)]
        pub data: Vec<u8, consts::U256>
    }

    #[derive(Clone, ATATResp, PartialEq, Debug)]
    pub struct TestResponseString {
        #[at_arg(position = 0)]
        pub socket: u8,
        #[at_arg(position = 1)]
        pub length: usize,
        #[at_arg(position = 2)]
        pub data: String<consts::U64>
    }

    #[derive(Clone, ATATResp, PartialEq, Debug)]
    pub struct TestResponseStringMixed {
        #[at_arg(position = 1)]
        pub socket: u8,
        #[at_arg(position = 2)]
        pub length: usize,
        #[at_arg(position = 0)]
        pub data: String<consts::U64>
    }

    #[test]
    fn string_sent() {
        static mut REQ_Q: Queue<Result<String<consts::U256>>, consts::U10, u8> = Queue(heapless::i::Queue::u8());
        let (mut p, c) = unsafe {REQ_Q.split()};
        let tx_mock = TxMock::new(String::new());
        let mut at_cli : ATClient<TxMock, CdMock> = ATClient::new(tx_mock, c, Config::new(Mode::Blocking));

        let cmd = SetModuleFunctionality{fun : Functionality::APM, rst : Some(ResetMode::DontReset)};
        
        let resp : Result<String::<consts::U256>> = Ok(String::<consts::U256>::from(""));
        p.enqueue(resp).unwrap();

        assert_eq!(at_cli.state, ClientState::Idle);

        match at_cli.send(&cmd){
            Ok(response) => {
                assert_eq!(response, NoResonse);
            },
            _ => panic!("Panic send error in test.")
        }
        assert_eq!(at_cli.state, ClientState::Idle);

        assert_eq!(at_cli.tx.s, String::<consts::U32>::from("AT+CFUN=4,0\r\n"),"Wrong encoding of string");

        let resp : Result<String::<consts::U256>> = Ok(String::<consts::U256>::from(""));
        p.enqueue(resp).unwrap();
        
        let cmd = Test2Cmd{fun : Functionality::DM, rst : Some(ResetMode::Reset)};
        match at_cli.send(&cmd){
            Ok(response) => {
                assert_eq!(response, NoResonse);
            },
            _ => panic!("Panic send error in test.")
        }

        assert_eq!(at_cli.tx.s, String::<consts::U32>::from("AT+CFUN=4,0\r\nAT+FUN=1,6\r\n"), "Reverse order string did not match");

    }


    #[test]
    //#[ignore]
    fn countdown() {
        static mut REQ_Q: Queue<Result<String<consts::U256>>, consts::U10, u8> = Queue(heapless::i::Queue::u8());
        let c = unsafe {REQ_Q.split().1};
        let tx_mock = TxMock::new(String::new());
        let mut at_cli : ATClient<TxMock, CdMock> = ATClient::new(tx_mock, c, Config::new(Mode::Timeout(CdMock{time : 0})));

        assert_eq!(at_cli.state, ClientState::Idle);

        let cmd = Test2Cmd{fun : Functionality::DM, rst : Some(ResetMode::Reset)};
        match at_cli.send(&cmd){
            Err(nb::Error::Other(error)) => {assert_eq!(error, Error::Timeout)},
            _ => panic!("Panic send error in test.")
        }
        //Todo: Test countdown is recived corretly
        match at_cli.mode{
            Mode::Timeout(cd_mock) => {} // assert_eq!(cd_mock.time, 180000),
            _ =>    panic!("Wrong AT mode")
        }
        assert_eq!(at_cli.state, ClientState::Idle);

    }

    #[test]
    fn blocking() {
        static mut REQ_Q: Queue<Result<String<consts::U256>>, consts::U10, u8> = Queue(heapless::i::Queue::u8());
        let (mut p, c) = unsafe {REQ_Q.split()};
        let tx_mock = TxMock::new(String::new());
        let mut at_cli : ATClient<TxMock, CdMock> = ATClient::new(tx_mock, c, Config::new(Mode::Blocking));

        let cmd = SetModuleFunctionality{fun : Functionality::APM, rst : Some(ResetMode::DontReset)};
        
        let resp : Result<String::<consts::U256>> = Ok(String::<consts::U256>::from(""));
        p.enqueue(resp).unwrap();

        assert_eq!(at_cli.state, ClientState::Idle);

        match at_cli.send(&cmd){
            Ok(response) => {
                assert_eq!(response, NoResonse);
            },
            _ => panic!("Panic send error in test.")
        }
        assert_eq!(at_cli.state, ClientState::Idle);
        assert_eq!(at_cli.tx.s, String::<consts::U32>::from("AT+CFUN=4,0\r\n"));
    }

    #[test]
    fn non_blocking() {
        static mut REQ_Q: Queue<Result<String<consts::U256>>, consts::U10, u8> = Queue(heapless::i::Queue::u8());
        let (mut p, c) = unsafe {REQ_Q.split()};
        let tx_mock = TxMock::new(String::new());
        let mut at_cli : ATClient<TxMock, CdMock> = ATClient::new(tx_mock, c, Config::new(Mode::NonBlocking));


        let cmd = SetModuleFunctionality{fun : Functionality::APM, rst : Some(ResetMode::DontReset)};
        
        assert_eq!(at_cli.state, ClientState::Idle);

        match at_cli.send(&cmd){
            Err(error) => assert_eq!(error, nb::Error::WouldBlock),
            _ => panic!("Panic send error in test"),
        }

        assert_eq!(at_cli.state, ClientState::AwaitingResponse);

        match at_cli.check_response(&cmd){
            Err(error) => assert_eq!(error, nb::Error::WouldBlock),
            _ => panic!("Send error in test"),
        }

        let resp : Result<String::<consts::U256>> = Ok(String::<consts::U256>::from(""));
        p.enqueue(resp).unwrap();

        assert_eq!(at_cli.state, ClientState::AwaitingResponse);

        match at_cli.check_response(&cmd){
            Ok(Some(response)) => {
                assert_eq!(response, NoResonse);
            },
            _ => panic!("Panic send error in test.")
        }
        assert_eq!(at_cli.state, ClientState::Idle);
        
    }


    //Testing unsupported frature in form of vec deserialization
    #[test]
    #[ignore]
    fn response_vec() {
        static mut REQ_Q: Queue<Result<String<consts::U256>>, consts::U10, u8> = Queue(heapless::i::Queue::u8());
        let (mut p, c) = unsafe {REQ_Q.split()};
        let tx_mock = TxMock::new(String::new());
        let mut at_cli : ATClient<TxMock, CdMock> = ATClient::new(tx_mock, c, Config::new(Mode::Blocking));


        let cmd = TestRespVecCmd{fun : Functionality::APM, rst : Some(ResetMode::DontReset)};
        
        let resp : Result<String::<consts::U256>> = Ok(String::<consts::U256>::from("+CUN: 22,16,\"0123456789012345\""));
        p.enqueue(resp).unwrap();

        let res_vec: Vec<u8, consts::U256> = "0123456789012345".as_bytes().iter().cloned().collect();

        assert_eq!(at_cli.state, ClientState::Idle);

        match at_cli.send(&cmd){
            Ok(response) => {
                assert_eq!(response, TestResponseVec{socket : 22, length : 16, data : res_vec});
            },
            Err(error) => panic!("Panic send error in test: {:?}", error)
        }
        assert_eq!(at_cli.state, ClientState::Idle);


        assert_eq!(at_cli.tx.s, String::<consts::U32>::from("AT+CFUN=4,0\r\n"));
    }
    //Test response containing string
    #[test]
    fn response_string() {
        static mut REQ_Q: Queue<Result<String<consts::U256>>, consts::U10, u8> = Queue(heapless::i::Queue::u8());
        let (mut p, c) = unsafe {REQ_Q.split()};
        let tx_mock = TxMock::new(String::new());
        let mut at_cli : ATClient<TxMock, CdMock> = ATClient::new(tx_mock, c, Config::new(Mode::Blocking));

        //String last
        let cmd = TestRespStringCmd{fun : Functionality::APM, rst : Some(ResetMode::DontReset)};
        
        let resp : Result<String::<consts::U256>> = Ok(String::<consts::U256>::from("+CUN: 22,16,\"0123456789012345\""));
        p.enqueue(resp).unwrap();

        assert_eq!(at_cli.state, ClientState::Idle);

        match at_cli.send(&cmd){
            Ok(response) => {
                assert_eq!(response, TestResponseString{socket : 22, length : 16, data : String::<consts::U64>::from("0123456789012345")});
            },
            Err(error) => panic!("Panic send error in test: {:?}", error)
        }
        assert_eq!(at_cli.state, ClientState::Idle);


        //Mixed order for string
        let cmd = TestRespStringMixCmd{fun : Functionality::APM, rst : Some(ResetMode::DontReset)};
        
        let resp : Result<String::<consts::U256>> = Ok(String::<consts::U256>::from("+CUN: \"0123456789012345\",22,16"));
        p.enqueue(resp).unwrap();

        match at_cli.send(&cmd){
            Ok(response) => {
                assert_eq!(response, TestResponseStringMixed{socket : 22, length : 16, data : String::<consts::U64>::from("0123456789012345")});
            },
            Err(error) => panic!("Panic send error in test: {:?}", error)
        }
        assert_eq!(at_cli.state, ClientState::Idle);
    }

    #[test]
    fn urc() {
        static mut REQ_Q: Queue<Result<String<consts::U256>>, consts::U10, u8> = Queue(heapless::i::Queue::u8());
        let (mut p, c) = unsafe {REQ_Q.split()};
        let tx_mock = TxMock::new(String::new());
        let mut at_cli : ATClient<TxMock, CdMock> = ATClient::new(tx_mock, c, Config::new(Mode::NonBlocking));

        let cmd = SetModuleFunctionality{fun : Functionality::APM, rst : Some(ResetMode::DontReset)};
        let resp : Result<String::<consts::U256>> = Ok(String::<consts::U256>::from(""));
        p.enqueue(resp).unwrap();

        assert_eq!(at_cli.state, ClientState::Idle);

        match at_cli.check_response(&cmd){
            Ok(None) => {},
            _ => panic!("Send error in test"),
        }

        assert_eq!(at_cli.state, ClientState::Idle);
                
    }

    #[test]
    fn invalid_response(){
        static mut REQ_Q: Queue<Result<String<consts::U256>>, consts::U10, u8> = Queue(heapless::i::Queue::u8());
        let (mut p, c) = unsafe {REQ_Q.split()};
        let tx_mock = TxMock::new(String::new());
        let mut at_cli : ATClient<TxMock, CdMock> = ATClient::new(tx_mock, c, Config::new(Mode::Blocking));

        //String last
        let cmd = TestRespStringCmd{fun : Functionality::APM, rst : Some(ResetMode::DontReset)};
        
        let resp : Result<String::<consts::U256>> = Ok(String::<consts::U256>::from("+CUN: 22,16,22"));
        p.enqueue(resp).unwrap();

        assert_eq!(at_cli.state, ClientState::Idle);

        match at_cli.send(&cmd){
            Err(error) => assert_eq!(error, nb::Error::Other(Error::InvalidResponse)),
            _ => panic!("Panic send error in test")
        }
        assert_eq!(at_cli.state, ClientState::Idle);

    }
}