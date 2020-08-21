use nom::{
    IResult,
    bytes::complete::{take_while_m_n, take_until},
    combinator::{map, map_res},
    sequence::{delimited, terminated},
    branch::{alt,},
    character::complete::{char, crlf},
    multi::many_m_n,
};
use anyhow::{Result, bail};
use thiserror::Error;
use std::str::{from_utf8};

#[derive(Debug, Eq, PartialEq)]
pub enum RESPType {
    Str(String),
    Error(String),
    Integer(i64),
    BulkStr(Option<Vec<u8>>),
    Arr(Option<Vec<RESPType>>),
}

#[derive(Error, Debug)]
pub enum ConvertErr {
    #[error("not string type")]
    NotStr,
    #[error("not int type")]
    NotInt,
    #[error("empty array")]
    EmptyArr,
    #[error("not array type")]
    NotArr,
}

impl RESPType {
    pub fn get_string(&self) -> Result<&str> {
        match self {
            RESPType::Str(v) => Ok(v),
            RESPType::BulkStr(v) => {
                if let Some(v) = v {
                    Ok(from_utf8(v)?)
                } else {
                    bail!(ConvertErr::NotStr);
                }
            },
            _ => bail!(ConvertErr::NotStr)
        }
    }
    pub fn get_int(&self) -> Result<i64> {
        if let &RESPType::Integer(v) = self {
            Ok(v)
        } else {
            bail!(ConvertErr::NotInt)
        }
    }
    pub fn to_arr(&self) -> Result<&Vec<RESPType>> {
        if let RESPType::Arr(arr) = self {
            if let Some(arr) = arr {
                if arr.is_empty() {
                    bail!(ConvertErr::EmptyArr);
                }
                return Ok(arr);
            }
        }
        Err(ConvertErr::NotArr.into())
    }
}

fn ps(c: char) -> impl FnMut(&[u8]) -> IResult<&[u8], String> {
    move |i| {
        map_res(
            delimited(char(c), take_until("\r"), crlf),
            |s: &[u8]| -> Result<String> {Ok(from_utf8(s)?.to_string())}
        )(i)
    }
}

fn parse_str(i: &[u8]) -> IResult<&[u8], RESPType> {
    map(
        ps('+'),
        |s| RESPType::Str(s)
    )(i)
}

fn parse_err(i: &[u8]) -> IResult<&[u8], RESPType> {
    map(
        ps('-'),
        |s| RESPType::Error(s)
    )(i)
}

fn parse_integer(i: &[u8]) -> IResult<&[u8], RESPType> {
    map_res(
        delimited(char(':'), take_until("\r"), crlf),
        |s: &[u8]| -> Result<RESPType> {Ok(RESPType::Integer(from_utf8(s)?.parse::<i64>()?))}
    )(i)
}

fn pi(c: char, i: &[u8]) -> IResult<&[u8], i64> {
    map_res(
        delimited(char(c), take_until("\r"), crlf),
        |s: &[u8]| -> Result<i64> {Ok(from_utf8(s)?.parse()?)}
    )(i)
}

fn parse_bulk_str(i: &[u8]) -> IResult<&[u8], RESPType> {
    let (rest, len) = pi('$', i)?;
    if len == -1 {
        return Ok((rest, RESPType::BulkStr(None)))
    }
    let m = len as usize;
    map(
        terminated(take_while_m_n(m,m, |_| true), crlf),
        |s: &[u8]| RESPType::BulkStr(Some(Vec::from(s)))
    )(rest)
}

fn parse_arr(i: &[u8]) -> IResult<&[u8], RESPType> {
    let (rest, len) = pi('*', i)?;
    if len == -1 {
        return Ok((rest, RESPType::Arr(None)));
    }
    let m = len as usize;
    map(
        many_m_n(m,m,parse_raw),
        |arr| RESPType::Arr(Some(arr))
    )(rest)
}

pub fn parse_raw(i: &[u8]) -> IResult<&[u8], RESPType> {
    alt((
        parse_str,
        parse_err,
        parse_integer,
        parse_bulk_str,
        parse_arr,
    ))(i)
}

#[cfg(test)]
mod tests {
    use super::{parse_raw, RESPType};
    #[test]
    fn test_resp_protocol() -> anyhow::Result<()> {
        let test_cases = vec![
            ("+foo\r\n", RESPType::Str("foo".to_string())),
            ("-wrong\r\n", RESPType::Error("wrong".to_string())),
            (":-10\r\n", RESPType::Integer(-10)),
            (":65535\r\n", RESPType::Integer(65535)),
            ("$10\r\nabcdefghij\r\n", RESPType::BulkStr(Some(Vec::from("abcdefghij".as_bytes())))),
            ("$-1\r\n", RESPType::BulkStr(None)),
            ("*2\r\n+foo\r\n-wrong\r\n", RESPType::Arr(Some(vec![RESPType::Str("foo".to_string()), RESPType::Error("wrong".to_string())]))),
            ("*3\r\n*3\r\n:1\r\n:-2\r\n:3\r\n$3\r\ntwe\r\n-qqq\r\n", RESPType::Arr(Some(vec![
                RESPType::Arr(Some(vec![RESPType::Integer(1), RESPType::Integer(-2), RESPType::Integer(3)])),
                RESPType::BulkStr(Some(Vec::from("twe".as_bytes()))),
                RESPType::Error("qqq".to_string()),
            ]))),
            ("*-1\r\n", RESPType::Arr(None)),
        ];
        for (s, expect) in test_cases {
            assert_eq!(parse_raw(s.as_bytes())?.1, expect);
        }

        Ok(())
    }
    #[test]
    fn test_parse_with_extra_bytes() -> anyhow::Result<()> {
        let (rest, actual) = parse_raw("+foo\r\ntt".as_bytes())?;
        assert_eq!(rest, "tt".as_bytes());
        assert_eq!(actual, RESPType::Str("foo".to_string()));
        Ok(())
    }
}