use itertools::Either;
use parquet::{
    file::{
        reader::{ChunkReader, FilePageIterator, FileReader, SerializedFileReader},
        serialized_reader::ReadOptionsBuilder,
    },
    record::reader::RowIter,
    record::Row,
};
use std::{
    fs::read,
    io::{stdout, BufRead, BufReader, Lines, Read, Seek, SeekFrom, Take, Write},
    string::ToString,
};
use std::{
    fs::{metadata, File},
    sync::Arc,
};

fn read_lines_from_file(file: File) -> std::io::Lines<std::io::BufReader<File>> {
    let br = std::io::BufReader::new(file);
    br.lines()
}

use std::io::Error as IOError;

use crate::W2vError;
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

pub struct TakeBufIter<'a> {
    pub handle: Take<BufReader<File>>,
    pub buf: &'a mut String,
}

impl<'a> Iterator for TakeBufIter<'a> {
    type Item = String;
    fn next(&mut self) -> Option<Self::Item> {
        let res = self.handle.read_line(self.buf);
        let buffer_value = self.buf.clone();
        self.buf.clear();
        match res {
            Ok(0) => None,
            Ok(_) => Some(buffer_value),
            _ => None,
        }
    }

    //fn from_file<'a>(file: File) -> FileBufLineReader {
    //    let reader = FileBufLineReader { lines: BufReader::new(file).lines() };
    //    reader
    //}
}

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

*/

trait GetStrIterator<T, I>
where
    T: ToString,
    I: Iterator<Item = T>,
{
    fn get_iterator(self) -> I;
}

struct GetParqetStrIterator {
    reader: SerializedFileReader<File>,
}

impl<'a> GetStrIterator<Row, RowIter<'a>> for GetParqetStrIterator {
    fn get_iterator(self) -> RowIter<'a> {
        self.reader.into_iter()
    }
}

pub enum LineIterable {
    Parquet(SerializedFileReader<File>),
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
        let file = File::open(file_name)?;
        let options = ReadOptionsBuilder::new()
            .with_range(start_pos as i64, end_pos as i64)
            .build();
        let reader = SerializedFileReader::new_with_options(file, options)?;
        Ok(LineIterable::Parquet(reader))
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

    pub fn get_line_iterator<'a>(
        self,
        buf: &'a mut String,
    ) -> impl Iterator<Item = String> + 'a {
        match self {
            LineIterable::Parquet(reader) => {
                println!("preparing parquet iterator");
                Either::Left(
                GetParqetStrIterator { reader: reader }
                    .get_iterator()
                    .map(|x| x.to_string())
                    .into_iter()
                )},
            LineIterable::TextFile(handle) => {
                println!("preparing text file iterator");
                Either::Right(TakeBufIter {
                handle: handle,
                buf: buf,
            })},
        }
    }
}
