use std::{
    convert::TryFrom,
    time::Duration,
};
use crate::protocol::resp::RESPType;

use anyhow::{Result, bail, ensure};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CommandErr {
    #[error("require at least {0} args but only {1} were provided")]
    LackArgs(usize, usize),
    #[error("null_str")]
    NullStr,
    #[error("wrong value type")]
    WrongValueType,
    #[error("unknow command {0}")]
    UnknownCommand(String),
}

#[derive(Debug, Eq, PartialEq)]
pub struct SetCmd<'a> {
    key: &'a str,
    value: Value,
    expire: Option<Duration>,
    setx: Option<SetX>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum SetX {
    NX,
    XX,
}


#[derive(Debug, Eq, PartialEq)]
pub enum Value {
    Num(i64),
    BSStr(Vec<u8>),
    Str(String),
}

impl TryFrom<&RESPType> for Value {
    type Error = anyhow::Error;
    fn try_from(v: &RESPType) -> Result<Self> {
        match v {
            RESPType::Integer(v) => Ok(Value::Num(*v)),
            RESPType::BulkStr(v) => {
                if let Some(v) = v {
                    Ok(Value::BSStr(v.clone()))
                } else {
                    bail!(CommandErr::NullStr);
                }
            },
            RESPType::Str(v) => Ok(Value::Str(v.to_string())),
            _ => bail!(CommandErr::WrongValueType),
        }
    }
}

impl<'a> TryFrom<&'a [RESPType]> for SetCmd<'a> {
    type Error = anyhow::Error;
    fn try_from(mut arr: &'a [RESPType]) -> Result<Self> {
        let n = arr.len();
        ensure!(n >= 2, CommandErr::LackArgs(2, n));
        let key = arr[0].get_string()?;
        let value = Value::try_from(&arr[1])?;
        arr = &arr[2..];
        let expire = if arr.len() >= 2 {
            match arr[0].get_string()? {
                "ex" => {
                    let v = Some(Duration::from_secs(arr[1].get_string()?.parse()?));
                    arr = &arr[2..];
                    v
                },
                "px" => {
                    let v = Some(Duration::from_millis(arr[1].get_string()?.parse()?));
                    arr = &arr[2..];
                    v
                },
                _ => None,
            }
        } else {
            None
        };
        let setx = if arr.len() >= 1 {
            match arr[0].get_string()? {
                "nx" => {
                    Some(SetX::NX)
                },
                "xx" => {
                    Some(SetX::XX)
                }
                _ => None,
            }
        } else {
            None
        };
        Ok(SetCmd{
            key,
            value,
            expire,
            setx,
        })
    }
}




#[derive(Debug, Eq, PartialEq)]
pub struct Get<'a>(&'a str);

impl<'a> TryFrom<&'a [RESPType]> for Get<'a> {
    type Error = anyhow::Error;
    fn try_from(arr: &'a [RESPType]) -> Result<Self> {
        if arr.is_empty() {
            bail!(CommandErr::LackArgs(1, 0));
        }
        Ok(Get(arr[0].get_string()?))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct GetSet<'a> {
    key: &'a str,
    val: Value,
}

impl<'a> TryFrom<&'a [RESPType]> for GetSet<'a> {
    type Error = anyhow::Error;
    fn try_from(v: &'a [RESPType]) -> Result<Self> {
        let n = v.len();
        ensure!(n == 2, CommandErr::LackArgs(2, v.len()));
        Ok(Self{
            key: v[0].get_string()?,
            val: Value::try_from(&v[1])?,
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct StrLen<'a>(&'a str);

impl<'a> TryFrom<&'a [RESPType]> for StrLen<'a> {
    type Error = anyhow::Error;
    fn try_from(v: &'a [RESPType]) -> Result<Self> {
        ensure!(v.len() == 1, CommandErr::LackArgs(1, v.len()));
        let key = v[0].get_string()?;
        Ok(StrLen(key))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Exists<'a>(&'a str);

impl<'a> TryFrom<&'a [RESPType]> for Exists<'a> {
    type Error = anyhow::Error;
    fn try_from(v: &'a [RESPType]) -> Result<Self> {
        ensure!(v.len() == 1, CommandErr::LackArgs(1, v.len()));
        let key = v[0].get_string()?;
        Ok(Exists(key))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Del<'a>(Vec<&'a str>);

impl<'a> TryFrom<&'a [RESPType]> for Del<'a> {
    type Error = anyhow::Error;
    fn try_from(v: &'a [RESPType]) -> Result<Self> {
        let n = v.len();
        ensure!(n > 0, CommandErr::LackArgs(1, 0));
        let mut arr = vec![];
        for i in 0..n {
            arr.push(v[i].get_string()?);
        }
        Ok(Del(arr))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum RedisCommands<'a> {
    SetCmd(SetCmd<'a>),
    Get(Get<'a>),
    GetSet(GetSet<'a>),
    StrLen(StrLen<'a>),
    Exists(Exists<'a>),
    Del(Del<'a>),
}

impl<'a> TryFrom<&'a RESPType> for RedisCommands<'a> {
    type Error = anyhow::Error;
    fn try_from(value: &'a RESPType) -> Result<Self, Self::Error> {
        let arr = value.to_arr()?;
        let action = arr[0].get_string()?;
        match action {
            "set" => Ok(RedisCommands::SetCmd(SetCmd::try_from(&arr[1..])?)),
            "get" => Ok(RedisCommands::Get(Get::try_from(&arr[1..])?)),
            "getset" => Ok(RedisCommands::GetSet(GetSet::try_from(&arr[1..])?)),
            "strlen" => Ok(RedisCommands::StrLen(StrLen::try_from(&arr[1..])?)),
            "exists" => Ok(RedisCommands::Exists(Exists::try_from(&arr[1..])?)),
            "del" => Ok(RedisCommands::Del(Del::try_from(&arr[1..])?)),
            _ => {
                bail!(CommandErr::UnknownCommand(action.to_string()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::parse_raw;
    use super::*;
    use std::convert::TryFrom;

    #[test]
    fn test_parse_commands() -> anyhow::Result<()> {
        let test_cases = vec![
            ("*2\r\n+get\r\n+foo\r\n", RedisCommands::Get(Get("foo"))),
            ("*3\r\n+set\r\n+foo\r\n:456\r\n", RedisCommands::SetCmd(SetCmd{
                key: "foo",
                value: Value::Num(456),
                expire: None,
                setx: None,
            })),
            ("*3\r\n$3\r\nset\r\n$5\r\nmykey\r\n$5\r\nHello\r\n", RedisCommands::SetCmd(SetCmd{
                key: "mykey",
                value: Value::BSStr(Vec::from("Hello".as_bytes())),
                expire: None,
                setx: None,
            })),
            ("*3\r\n$6\r\ngetset\r\n$7\r\nthiskey\r\n$5\r\n12345\r\n", RedisCommands::GetSet(GetSet{
                key: "thiskey",
                val: Value::BSStr(Vec::from("12345".as_bytes())),
            })),
        ];
        for (i, expect) in test_cases {
            let (_, resp) = parse_raw(i.as_bytes())?;
            assert_eq!(RedisCommands::try_from(&resp)?, expect);
        }
        Ok(())
    }
}