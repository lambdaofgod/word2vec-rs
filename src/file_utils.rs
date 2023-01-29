use crate::utils::W2vError;
use itertools::Either;
use parquet::{
    errors::ParquetError,
    file::{
        reader::{ChunkReader, FilePageIterator, FileReader, RowGroupReader, SerializedFileReader},
        serialized_reader::ReadOptionsBuilder,
    },
    record::reader::RowIter,
    record::Row,
};
use std::iter::Map;
use std::rc::Rc;
use std::{
    fs::read,
    io::{stdout, BufRead, BufReader, Lines, Read, Seek, SeekFrom, Take, Write},
    string::ToString,
};
use std::{
    fs::{metadata, File},
    sync::Arc,
};
use crate::parquet_files::{*};

fn read_lines_from_file(file: File) -> std::io::Lines<std::io::BufReader<File>> {
    let br = std::io::BufReader::new(file);
    br.lines()
}

use std::io::Error as IOError;

type MaybeString = Option<String>;
type StringResult = Result<String, IOError>;

trait LineReader<ItemType, IterType: Iterator<Item = ItemType>> {
    fn next_line(&mut self) -> MaybeString;
    //fn from_file(file: File) -> Self;
    fn to_maybe_string(item: ItemType) -> MaybeString;
}

pub struct FileBufLineReader {
    lines: Lines<BufReader<File>>,
}
impl LineReader<StringResult, Lines<BufReader<File>>> for FileBufLineReader {
    fn next_line(&mut self) -> MaybeString {
        let res: Option<StringResult> = self.lines.next();
        res.map(Self::to_maybe_string).flatten()
    }
    //fn from_file<'a>(file: File) -> FileBufLineReader {
    //    let reader = FileBufLineReader { lines: BufReader::new(file).lines() };
    //    reader
    //}
    fn to_maybe_string(item: StringResult) -> MaybeString {
        match item {
            Ok(str) => Some(str),
            _ => None,
        }
    }
}

pub struct TakeBufStrReader {
    pub handle: Take<BufReader<File>>,
}
impl TakeBufStrReader {
    pub fn next_line<'a>(&mut self, buf: &'a mut String) -> bool {
        let res = self.handle.read_line(buf);
        match res {
            Ok(0) => false,
            Ok(_) => true,
            _ => false,
        }
    }

    //fn from_file<'a>(file: File) -> FileBufLineReader {
    //    let reader = FileBufLineReader { lines: BufReader::new(file).lines() };
    //    reader
    //}
}

pub struct TakeBufIter {
    handle: Take<BufReader<File>>,
    buf: String,
}
impl TakeBufIter {
    pub fn new(handle: Take<BufReader<File>>) -> Self {
        TakeBufIter {
            handle: handle,
            buf: String::new(),
        }
    }
}
//fn from_file<'a>(file: File) -> FileBufLineReader {
//    let reader = FileBufLineReader { lines: BufReader::new(file).lines() };
//    reader
//}

pub struct ParquetLineReader<'a> {
    row_iter: RowIter<'a>,
}
impl<'a> LineReader<Row, RowIter<'a>> for ParquetLineReader<'a> {
    fn next_line(&mut self) -> MaybeString {
        // jesli to jebnie to rezultat typu bedzie zly
        let next_value = self.row_iter.next();
        next_value.map(|r| r.to_string())
    }
    fn to_maybe_string(item: Row) -> MaybeString {
        Some(item.to_string())
    }
}



/*
- czy to filereader powinien rozszerzać iterator
- czy on nie powinien mieć metody która zwraca iterator


1. zrób traita który jest iterator<string>
2. zrób metody które zwracają go dla różnych typów
*/

/*
podejscie: mutable builder
*/

pub enum LineIterable {
    Parquet(ParquetGroupReader),
    TextFile(TakeBufIter),
}

pub enum LoadOptions {
    Parquet(usize),
    TextFile(u64, u64),
}

impl LineIterable {
    pub fn new(file: File, load_options: LoadOptions) -> Result<LineIterable, W2vError> {
        match load_options {
            LoadOptions::Parquet(gp) => {
                Ok(Self::Parquet(ParquetGroupReader::from_file(file, gp)?))
            }
            LoadOptions::TextFile(start, end) => {
                Self::get_text_file_line_iterable(file, start, end)
            }
        }
    }

    pub fn get_iter(liter: LineIterable) -> Result<Box<dyn Iterator<Item = String>>, W2vError> {
        Ok(match liter {
            Self::Parquet(pqt_reader) => Box::new(pqt_reader.into_iter()?),
            Self::TextFile(take_buf_iter) => Box::new(take_buf_iter.into_iter()),
        })
    }

    fn get_text_file_line_iterable(
        file: File,
        start_pos: u64,
        end_pos: u64,
    ) -> Result<LineIterable, W2vError> {
        let mut reader = BufReader::with_capacity(10000, file);
        reader.seek(SeekFrom::Start(start_pos))?;
        let handle = reader.take(end_pos - start_pos);
        let b_iter = TakeBufIter::new(handle);
        Ok(LineIterable::TextFile(b_iter))
    }
}

impl Iterator for TakeBufIter {
    type Item = String;
    fn next(&mut self) -> Option<Self::Item> {
        let res = self.handle.read_line(&mut self.buf);
        let buffer_value = self.buf.clone();
        self.buf.clear();
        match res {
            Ok(0) => None,
            Ok(_) => Some(buffer_value),
            _ => None,
        }
    }
}

/*
pub fn get_from_file(
    file_name: String,
    start_pos: u64,
    end_pos: u64,
) -> Result<LineIterable, W2vError> {
    if file_name.contains("parquet") {
        Self::get_parquet_line_iterable(file_name, start_pos, end_pos)
    } else {
        Self::get_text_file_line_iterable(file_name, start_pos, end_pos)
    }
}
*/

/*
pub enum LineIterableBuilder {
    Parquet(Rc<SerializedFileReader<File>>, usize),
    TextFile(Take<BufReader<File>>),
}

impl LineIterable {
    fn get_text_file_line_iterable(
        filename: String,
        start_pos: u64,
        end_pos: u64,
    ) -> Result<Self, W2vError> {
        let input_file = File::open(filename)?;
        let mut reader = BufReader::with_capacity(10000, input_file);
        reader.seek(SeekFrom::Start(start_pos))?;
        let handle = reader.take(end_pos - start_pos);
        Ok(LineIterable::TextFile(handle))
    }

    fn get_parquet_line_iterable(
        file_name: String,
        start_pos: u64,
        end_pos: u64,
    ) -> Result<Self, W2vError> {
        println!("split for {}-{}", start_pos, end_pos);
        let file = File::open(file_name)?;
        let options = ReadOptionsBuilder::new()
            .with_range(start_pos as i64, end_pos as i64)
            .build();
        let reader = SerializedFileReader::new(file)?;
        Ok(LineIterable::Parquet(Rc::new(reader), start_pos as usize))
    }

    pub fn get_from_file(
        file_name: String,
        start_pos: u64,
        end_pos: u64,
    ) -> Result<LineIterable, W2vError> {
        if file_name.contains("parquet") {
            Self::get_parquet_line_iterable(file_name, start_pos, end_pos)
        } else {
            Self::get_text_file_line_iterable(file_name, start_pos, end_pos)
        }
    }

    fn create_parquet_str_iter<'a>(
        reader: Rc<SerializedFileReader<File>>,
        gp: &'a usize,
    ) -> Result<GetParqetStrIterator<'a>, W2vError> {
    }

    fn create_take_buf_iter<'a>(
        handle: &'a mut Take<BufReader<File>>,
        buf: &'a mut String,
    ) -> TakeBufIter<'a> {
        TakeBufIter {
            handle: handle,
            buf: buf,
        }
    }

    // -> Result<Box<dyn Iterator<Item = String>>, W2vError>
    pub fn get_line_iterator<'a: 'static>(
        self,
        buf: &'a mut String,
    ) -> Result<(Self, Either<GetParqetStrIterator, TakeBufIter>), W2vError> {
        Ok((
            self,
            match self {
                LineIterable::Parquet(reader, ref gp) => {
                    Either::Left(Self::create_parquet_str_iter(reader, gp)?)
                }
                LineIterable::TextFile(ref mut handle) => {
                    Either::Right(Self::create_take_buf_iter(handle, buf))
                }
            },
        ))
    }
}
*/
