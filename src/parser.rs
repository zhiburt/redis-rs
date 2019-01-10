use std::{
    io::{self, Read},
    str,
};

use crate::types::{make_extension_error, ErrorKind, RedisError, RedisResult, Value};

use combine::{
    any,
    error::{Commit, StreamError},
    parser,
    parser::{
        byte::{crlf, take_until_bytes},
        combinator::{any_send_sync_partial_state, AnySendSyncPartialState},
        function,
        range::{recognize, take},
    },
    stream::{
        state::Stream as StateStream, PointerOffset, RangeStream, StreamErrorFor, StreamOnce,
    },
    ParseError,
};

struct ResultExtend<T, E>(Result<T, E>);

impl<T, E> Default for ResultExtend<T, E>
where
    T: Default,
{
    fn default() -> Self {
        ResultExtend(Ok(T::default()))
    }
}

impl<T, U, E> Extend<Result<U, E>> for ResultExtend<T, E>
where
    T: Extend<U>,
{
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = Result<U, E>>,
    {
        let mut returned_err = None;
        if let Ok(ref mut elems) = self.0 {
            elems.extend(iter.into_iter().scan((), |_, item| match item {
                Ok(item) => Some(item),
                Err(err) => {
                    returned_err = Some(err);
                    None
                }
            }));
        }
        if let Some(err) = returned_err {
            self.0 = Err(err);
        }
    }
}

trait RedisParser: Send {
    fn nil(&mut self) -> RedisResult<()>;
    fn data(&mut self, data: &[u8]) -> RedisResult<()>;
    fn status(&mut self, msg: &str) -> RedisResult<()>;
    fn bulk_start(&mut self, size: usize);
    fn bulk_end(&mut self);
    fn int(&mut self, i: i64) -> RedisResult<()>;
}

enum ValueParser {
    Bulk(Vec<Vec<Value>>),
    Value(Value),
}

impl Default for ValueParser {
    fn default() -> Self {
        ValueParser::Value(Value::Nil)
    }
}

impl ValueParser {
    fn take(&mut self) -> Value {
        let value = ::std::mem::replace(self, ValueParser::default());
        match value {
            ValueParser::Value(v) => v,
            ValueParser::Bulk(_) => unreachable!(),
        }
    }

    fn value(&mut self, value: Value) -> RedisResult<()> {
        match self {
            ValueParser::Value(_) => *self = ValueParser::Value(value),
            ValueParser::Bulk(bulk) => bulk.last_mut().unwrap().push(value),
        }
        Ok(())
    }
}

impl RedisParser for ValueParser {
    fn nil(&mut self) -> RedisResult<()> {
        self.value(Value::Nil)
    }
    fn data(&mut self, data: &[u8]) -> RedisResult<()> {
        self.value(Value::Data(data.to_owned()))
    }
    fn status(&mut self, msg: &str) -> RedisResult<()> {
        self.value(if msg == "OK" {
            Value::Okay
        } else {
            Value::Status(msg.to_owned())
        })
    }
    fn bulk_start(&mut self, size: usize) {
        let bulks = match self {
            ValueParser::Value(_) => {
                *self = ValueParser::Bulk(vec![]);
                match self {
                    ValueParser::Value(_) => unreachable!(),
                    ValueParser::Bulk(bulks) => bulks,
                }
            }
            ValueParser::Bulk(bulks) => bulks,
        };
        bulks.push(Vec::with_capacity(size));
    }
    fn bulk_end(&mut self) {
        *self = match self {
            ValueParser::Value(_) => unreachable!(),
            ValueParser::Bulk(bulks) => {
                let done_bulk = bulks.pop().unwrap();
                match bulks.last_mut() {
                    Some(bulk) => {
                        bulk.push(Value::Bulk(done_bulk));
                        return;
                    }
                    None => ValueParser::Value(Value::Bulk(done_bulk)),
                }
            }
        }
    }
    fn int(&mut self, i: i64) -> RedisResult<()> {
        self.value(Value::Int(i))
    }
}

parser! {
fn with_state[S, R, I, F](f: F)(StateStream<I, S>) -> R
    where [
        F: FnMut(&mut S) -> R,
        I: combine::Stream
    ]
{
    function::parser(move |input: &mut StateStream<I, S>| -> Result<_, _> {
        Ok((f(&mut input.state), Commit::Peek(())))
    })
}
}

parser! {
type PartialState = AnySendSyncPartialState;
fn value['a, 'b, I]()(StateStream<I, &'b mut dyn RedisParser>) -> RedisResult<()>
    where [I: RangeStream<Token = u8, Range = &'a [u8]>,
           I::Error: combine::ParseError<u8, &'a [u8], I::Position>,
           StateStream<I, &'b mut dyn RedisParser>: RangeStream<Token = u8, Range = &'a [u8]>,
           <StateStream<I, &'b mut dyn RedisParser> as StreamOnce>::Token: PartialEq + Send,
           <StateStream<I, &'b mut dyn RedisParser> as StreamOnce>::Range: combine::stream::Range + PartialEq + Send,
           <StateStream<I, &'b mut dyn RedisParser> as StreamOnce>::Error: combine::ParseError<u8, &'a [u8], <StateStream<I, &'b mut dyn RedisParser> as StreamOnce>::Position>,
           <<StateStream<I, &'b mut dyn RedisParser> as StreamOnce>::Error as combine::ParseError<u8, &'a [u8], <StateStream<I, &'b mut dyn RedisParser> as StreamOnce>::Position>>::StreamError: StreamError<u8, &'a [u8]>,
          ]
{
        any_send_sync_partial_state(any().then_partial(move |&mut b| {
        let line = || recognize(take_until_bytes(&b"\r\n"[..]).with(take(2).map(|_| ())))
            .and_then(|line: &[u8]| {
                str::from_utf8(&line[..line.len() - 2])
                    .map_err(StreamErrorFor::<StateStream<I, _>>::other)
            });

        let status = || line().map_input(move |line, input: &mut StateStream<_, &mut dyn RedisParser>| {
            input.state.status(line)
        });

        let int = || line().and_then(move |line| {
            match line.trim().parse::<i64>() {
                Err(_) => Err(StreamErrorFor::<StateStream<I, _>>::message_static_message("Expected integer, got garbage")),
                Ok(value) => Ok(value),
            }
        });

        let data = || int().then_partial(move |&mut size| {
            if size < 0 {
                with_state(move |state: &mut &mut dyn RedisParser| state.nil())
                    .left()
            } else {
                take(size as usize)
                    .map_input(move |bs: &[u8], input: &mut StateStream<_, &mut dyn RedisParser>|
                        input.state.data(bs)
                    )
                    .skip(crlf())
                    .right()
            }
        });

        let bulk = || {
            int().then_partial(move |&mut length| {
                if length < 0 {
                    with_state(move |state: &mut &mut dyn RedisParser|
                        state.nil()
                    )
                        .left()
                } else {
                    let length = length as usize;
                    with_state(move |state: &mut &mut dyn RedisParser| state.bulk_start(length))
                        .with(combine::count_min_max(length, length, value()))
                        .skip(with_state(|state: &mut &mut dyn RedisParser| state.bulk_end()))
                        .map(|result: ResultExtend<(), _>| {
                            result.0
                        })
                        .right()
                }
            })
        };

        let error = || {
            line()
                .map(move |line: &str| {
                    let desc = "An error was signalled by the server";
                    let mut pieces = line.splitn(2, ' ');
                    let kind = match pieces.next().unwrap() {
                        "ERR" => ErrorKind::ResponseError,
                        "EXECABORT" => ErrorKind::ExecAbortError,
                        "LOADING" => ErrorKind::BusyLoadingError,
                        "NOSCRIPT" => ErrorKind::NoScriptError,
                        "MOVED" => ErrorKind::Moved,
                        "ASK" => ErrorKind::Ask,
                        "TRYAGAIN" => ErrorKind::TryAgain,
                        "CLUSTERDOWN" => ErrorKind::ClusterDown,
                        "CROSSSLOT" => ErrorKind::CrossSlot,
                        "MASTERDOWN" => ErrorKind::MasterDown,
                        "READONLY" => ErrorKind::ReadOnly,
                        code => return make_extension_error(code, pieces.next()),
                    };
                    match pieces.next() {
                        Some(detail) => RedisError::from((kind, desc, detail.to_string())),
                        None => RedisError::from((kind, desc)),
                    }
                })
            };

        combine::dispatch!(b;
            b'+' => status(),
            b':' => int().then_partial(move |&mut i| {
                with_state(move |state: &mut &mut dyn RedisParser|
                    state.int(i)
                )
            }),
            b'$' => data(),
            b'*' => bulk(),
            b'-' => error().map(Err),
            b => combine::unexpected_any(combine::error::Token(b))
        )
    }))
}
}

#[cfg(feature = "aio")]
mod aio_support {
    use super::*;

    use bytes::{Buf, BytesMut};
    use tokio::io::AsyncRead;
    use tokio_util::codec::{Decoder, Encoder};

    #[derive(Default)]
    pub struct ValueCodec {
        state: AnySendSyncPartialState,
        redis_state: ValueParser,
    }

    impl ValueCodec {
        fn decode_stream(&mut self, bytes: &mut BytesMut, eof: bool) -> RedisResult<Option<Value>> {
            let (opt, removed_len) = {
                let buffer = &bytes[..];
                let mut stream = combine::stream::state::Stream {
                    stream: combine::easy::Stream(combine::stream::MaybePartialStream(
                        buffer, !eof,
                    )),
                    state: &mut self.redis_state as &mut dyn RedisParser,
                };
                match combine::stream::decode_tokio(value(), &mut stream, &mut self.state) {
                    Ok(x) => x,
                    Err(err) => {
                        let err = err
                            .map_position(|pos| pos.translate_position(buffer))
                            .map_range(|range| format!("{:?}", range))
                            .to_string();
                        return Err(RedisError::from((
                            ErrorKind::ResponseError,
                            "parse error",
                            err,
                        )));
                    }
                }
            };

            bytes.advance(removed_len);
            match opt {
                Some(result) => Ok(Some({
                    result?;
                    self.redis_state.take()
                })),
                None => Ok(None),
            }
        }
    }

    impl Encoder<Vec<u8>> for ValueCodec {
        type Error = RedisError;
        fn encode(&mut self, item: Vec<u8>, dst: &mut BytesMut) -> Result<(), Self::Error> {
            dst.extend_from_slice(item.as_ref());
            Ok(())
        }
    }

    impl Decoder for ValueCodec {
        type Item = Value;
        type Error = RedisError;

        fn decode(&mut self, bytes: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            self.decode_stream(bytes, false)
        }

        fn decode_eof(&mut self, bytes: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            self.decode_stream(bytes, true)
        }
    }

    /// Parses a redis value asynchronously.
    pub async fn parse_redis_value_async<R>(
        decoder: &mut combine::stream::Decoder<AnySendSyncPartialState, PointerOffset<[u8]>>,
        read: &mut R,
    ) -> RedisResult<Value>
    where
        R: AsyncRead + std::marker::Unpin,
    {
        let mut state = ValueParser::default();
        let result = combine::decode_tokio!(*decoder, *read, value(), |input, _| {
            combine::stream::state::Stream {
                stream: combine::stream::easy::Stream::from(input),
                state: &mut state as &mut dyn RedisParser,
            }
        });
        match result {
            Err(err) => Err(match err {
                combine::stream::decoder::Error::Io { error, .. } => error.into(),
                combine::stream::decoder::Error::Parse(err) => {
                    if err.is_unexpected_end_of_input() {
                        RedisError::from(io::Error::from(io::ErrorKind::UnexpectedEof))
                    } else {
                        let err = err
                            .map_range(|range| format!("{:?}", range))
                            .map_position(|pos| pos.translate_position(decoder.buffer()))
                            .to_string();
                        RedisError::from((ErrorKind::ResponseError, "parse error", err))
                    }
                }
            }),
            Ok(result) => {
                result?;
                Ok(state.take())
            }
        }
    }
}

#[cfg(feature = "aio")]
#[cfg_attr(docsrs, doc(cfg(feature = "aio")))]
pub use self::aio_support::*;

/// The internal redis response parser.
pub struct Parser {
    decoder: combine::stream::decoder::Decoder<AnySendSyncPartialState, PointerOffset<[u8]>>,
}

impl Default for Parser {
    fn default() -> Self {
        Parser::new()
    }
}

/// The parser can be used to parse redis responses into values.  Generally
/// you normally do not use this directly as it's already done for you by
/// the client but in some more complex situations it might be useful to be
/// able to parse the redis responses.
impl Parser {
    /// Creates a new parser that parses the data behind the reader.  More
    /// than one value can be behind the reader in which case the parser can
    /// be invoked multiple times.  In other words: the stream does not have
    /// to be terminated.
    pub fn new() -> Parser {
        Parser {
            decoder: combine::stream::decoder::Decoder::new(),
        }
    }

    // public api

    /// Parses synchronously into a single value from the reader.
    pub fn parse_value<T: Read>(&mut self, mut reader: T) -> RedisResult<Value> {
        let mut decoder = &mut self.decoder;
        let mut state = ValueParser::default();
        let result = combine::decode!(decoder, reader, value(), |input, _| {
            combine::stream::state::Stream {
                stream: combine::stream::easy::Stream::from(input),
                state: &mut state as &mut dyn RedisParser,
            }
        });
        match result {
            Err(err) => Err(match err {
                combine::stream::decoder::Error::Io { error, .. } => error.into(),
                combine::stream::decoder::Error::Parse(err) => {
                    if err.is_unexpected_end_of_input() {
                        RedisError::from(io::Error::from(io::ErrorKind::UnexpectedEof))
                    } else {
                        let err = err
                            .map_range(|range| format!("{:?}", range))
                            .map_position(|pos| pos.translate_position(decoder.buffer()))
                            .to_string();
                        RedisError::from((ErrorKind::ResponseError, "parse error", err))
                    }
                }
            }),
            Ok(result) => {
                result?;
                Ok(state.take())
            }
        }
    }
}

/// Parses bytes into a redis value.
///
/// This is the most straightforward way to parse something into a low
/// level redis value instead of having to use a whole parser.
pub fn parse_redis_value(bytes: &[u8]) -> RedisResult<Value> {
    let mut parser = Parser::new();
    parser.parse_value(bytes)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "aio")]
    use super::*;

    #[cfg(feature = "aio")]
    #[test]
    fn decode_eof_returns_none_at_eof() {
        use tokio_util::codec::Decoder;
        let mut codec = ValueCodec::default();

        let mut bytes = bytes::BytesMut::from(&b"+GET 123\r\n"[..]);
        assert_eq!(
            codec.decode_eof(&mut bytes),
            Ok(Some(parse_redis_value(b"+GET 123\r\n").unwrap()))
        );
        assert_eq!(codec.decode_eof(&mut bytes), Ok(None));
        assert_eq!(codec.decode_eof(&mut bytes), Ok(None));
    }
}
