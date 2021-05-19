use heapless::{String, Vec};
use serde_at::CharVec;

/// Trait used by [`atat_derive`] to estimate lengths of the serialized commands, at compile time.
///
/// [`atat_derive`]: https://crates.io/crates/atat_derive
pub trait AtatLen {
    const LEN: usize;
}

macro_rules! impl_length {
    ($type:ty, $len:expr) => {
        #[allow(clippy::use_self)]
        impl AtatLen for $type {
            const LEN: usize = $len;
        }
    };
}

impl_length!(char, 1);
impl_length!(bool, 5);
impl_length!(isize, 19);
impl_length!(usize, 20);
impl_length!(u8, 3);
impl_length!(u16, 5);
impl_length!(u32, 10);
impl_length!(u64, 20);
impl_length!(u128, 39);
impl_length!(i8, 4);
impl_length!(i16, 6);
impl_length!(i32, 11);
impl_length!(i64, 20);
impl_length!(i128, 40);
impl_length!(f32, 42);
impl_length!(f64, 312);

impl<const T: usize> AtatLen for String<T> {
    const LEN: usize = T;
}

impl<T: AtatLen> AtatLen for Option<T> {
    const LEN: usize = T::LEN;
}

impl<T: AtatLen> AtatLen for &T {
    const LEN: usize = T::LEN;
}

impl<T, const L: usize> AtatLen for Vec<T, L>
where
    T: AtatLen,
{
    const LEN: usize = L * <T as AtatLen>::LEN;
}

impl<const T: usize> AtatLen for CharVec<T> {
    const LEN: usize = T;
}

#[cfg(test)]
mod tests {
    use crate as atat;
    use atat::{derive::AtatLen, AtatCmd};
    use atat_derive::{AtatCmd, AtatEnum, AtatResp};
    use heapless::{String, Vec};
    use serde_at::{from_str, to_string, SerializeOptions};

    #[derive(Debug, PartialEq, AtatResp)]
    struct NoResponse {}

    #[derive(Debug, PartialEq, AtatEnum)]
    enum SimpleEnum {
        #[at_arg(default, value = 0)]
        A,
        #[at_arg(value = 1)]
        B,
        #[at_arg(value = 2)]
        C,
        #[at_arg(value = 3)]
        D,
    }
    #[derive(Debug, PartialEq, AtatEnum)]
    #[at_enum(u32)]
    enum SimpleEnumU32 {
        #[at_arg(default)]
        A,
        B,
        C,
        D,
    }

    #[derive(Debug, PartialEq, AtatEnum)]
    enum MixedEnum<'a> {
        #[at_arg(value = 0)]
        UnitVariant,
        #[at_arg(value = 1)]
        SingleSimpleTuple(u8),
        #[at_arg(default, value = 2)]
        AdvancedTuple(u8, String<10>, i64, SimpleEnumU32),
        #[at_arg(value = 3)]
        SingleSimpleStruct { x: u8 },
        #[at_arg(value = 4)]
        AdvancedStruct {
            a: u8,
            b: String<10>,
            c: i64,
            d: SimpleEnum,
        },
        #[at_arg(value = 6)]
        SingleSimpleTupleLifetime(#[at_arg(len = 10)] &'a str),
    }

    #[derive(Debug, PartialEq, AtatCmd)]
    #[at_cmd("+CFUN", NoResponse)]
    struct LengthTester<'a> {
        x: u8,
        y: String<128>,
        #[at_arg(len = 2)]
        z: u16,
        #[at_arg(len = 150)]
        w: &'a str,
        a: SimpleEnum,
        b: SimpleEnumU32,
        #[at_arg(len = 3)]
        c: SimpleEnumU32,
        // d: Vec<SimpleEnumU32, 5>,
    }

    #[test]
    fn test_atat_len() {
        assert_eq!(<char as AtatLen>::LEN, 1);
        assert_eq!(<bool as AtatLen>::LEN, 5);
        assert_eq!(<isize as AtatLen>::LEN, 19);
        assert_eq!(<usize as AtatLen>::LEN, 20);
        assert_eq!(<u8 as AtatLen>::LEN, 3);
        assert_eq!(<u16 as AtatLen>::LEN, 5);
        assert_eq!(<u32 as AtatLen>::LEN, 10);
        assert_eq!(<u64 as AtatLen>::LEN, 20);
        assert_eq!(<u128 as AtatLen>::LEN, 39);
        assert_eq!(<i8 as AtatLen>::LEN, 4);
        assert_eq!(<i16 as AtatLen>::LEN, 6);
        assert_eq!(<i32 as AtatLen>::LEN, 11);
        assert_eq!(<i64 as AtatLen>::LEN, 20);
        assert_eq!(<i128 as AtatLen>::LEN, 40);
        assert_eq!(<f32 as AtatLen>::LEN, 42);
        assert_eq!(<f64 as AtatLen>::LEN, 312);

        assert_eq!(<SimpleEnum as AtatLen>::LEN, 3);
        assert_eq!(<SimpleEnumU32 as AtatLen>::LEN, 10);
        // (fields) + (n_fields - 1)
        // (3 + 128 + 2 + 150 + 3 + 10 + 3 + (10*5)) + 7
        assert_eq!(
            <LengthTester<'_> as AtatLen>::LEN,
            (3 + 128 + 2 + 150 + 3 + 10 + 3) + 6
        );
        assert_eq!(<MixedEnum<'_> as AtatLen>::LEN, (3 + 3 + 10 + 20 + 10) + 4);
    }

    #[test]
    fn test_length_serialize() {
        assert_eq!(
            LengthTester {
                x: 8,
                y: String::from("SomeString"),
                z: 2,
                w: &"whatup",
                a: SimpleEnum::A,
                b: SimpleEnumU32::A,
                c: SimpleEnumU32::B,
                // d: Vec::new()
            }
            .as_bytes(),
            Vec::<u8, 360>::from_slice(b"AT+CFUN=8,\"SomeString\",2,\"whatup\",0,0,1\r\n").unwrap()
        );
    }

    #[test]
    fn test_mixed_enum() {
        assert_eq!(
            to_string::<_, 1, 3>(
                &MixedEnum::UnitVariant,
                String::from("CMD"),
                SerializeOptions::default()
            )
            .unwrap(),
            String::<1>::from("0")
        );
        assert_eq!(
            to_string::<_, 10, 3>(
                &MixedEnum::SingleSimpleTuple(15),
                String::from("CMD"),
                SerializeOptions::default()
            )
            .unwrap(),
            String::<10>::from("1,15")
        );
        assert_eq!(
            to_string::<_, 50, 3>(
                &MixedEnum::AdvancedTuple(25, String::from("testing"), -54, SimpleEnumU32::A),
                String::from("CMD"),
                SerializeOptions::default()
            )
            .unwrap(),
            String::<50>::from("2,25,\"testing\",-54,0")
        );
        assert_eq!(
            to_string::<_, 10, 3>(
                &MixedEnum::SingleSimpleStruct { x: 35 },
                String::from("CMD"),
                SerializeOptions::default()
            )
            .unwrap(),
            String::<10>::from("3,35")
        );

        assert_eq!(
            to_string::<_, 50, 3>(
                &MixedEnum::AdvancedStruct {
                    a: 77,
                    b: String::from("whaat"),
                    c: 88,
                    d: SimpleEnum::B
                },
                String::from("CMD"),
                SerializeOptions::default()
            )
            .unwrap(),
            String::<50>::from("4,77,\"whaat\",88,1")
        );

        assert_eq!(Ok(MixedEnum::UnitVariant), from_str::<MixedEnum<'_>>("0"));
        assert_eq!(
            Ok(MixedEnum::SingleSimpleTuple(67)),
            from_str::<MixedEnum<'_>>("1,67")
        );
        assert_eq!(
            Ok(MixedEnum::AdvancedTuple(
                251,
                String::from("deser"),
                -43,
                SimpleEnumU32::C
            )),
            from_str::<MixedEnum<'_>>("2,251,\"deser\",-43,2")
        );

        assert_eq!(
            Ok(MixedEnum::SingleSimpleTupleLifetime("abc")),
            from_str::<MixedEnum<'_>>("6,\"abc\"")
        );
    }
}
