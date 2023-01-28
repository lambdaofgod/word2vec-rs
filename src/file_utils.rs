use parquet::{
    arrow::arrow_reader,
    errors::ParquetError,
    file::{
        metadata::ParquetMetaData,
        reader::{ChunkReader, FilePageIterator, FileReader, SerializedFileReader},
        serialized_reader::ReadOptionsBuilder,
    },
    record::reader::RowIter,
    record::Row,
};
use std::{
    fs::read,
    io::{stdout, BufRead, BufReader, Lines, Read, Seek, SeekFrom, Take, Write}, usize,
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
    pub buf: String
}
impl TakeBufStrReader {
    pub fn next_line<'a>(&mut self, buf: &'a mut String) -> Result<&'a mut String, IOError> {
        let res = self.handle.read_line(buf);
        res.map(|b| buf)
    }

    //fn from_file<'a>(file: File) -> FileBufLineReader {
    //    let reader = FileBufLineReader { lines: BufReader::new(file).lines() };
    //    reader
    //}
    pub fn from_handle(handle: Take<BufReader<File>>) -> Self {
        let buf = String::new();
        TakeBufStrReader { handle: handle, buf: buf }
    }
}


impl Iterator for TakeBufStrReader {
    type Item = String;
    fn next(&mut self) -> Option<Self::Item> {
        let res = self.handle.read_line(&mut self.buf);
        match res {
            Ok(0) => None,
            Ok(_) => Some(self.buf.clone()),
            _ => None
        }
    }
}

pub struct ParquetStrReader<'a> {
    row_iter: RowIter<'a>,
}
impl<'a> ParquetStrReader<'a> {
    pub fn next_line(&mut self, buf: &'a mut String) -> Result<&'a mut String, IOError> {
        let res = self.row_iter.next();
        res.map(|row| *buf = row.to_string());
        Ok(buf)
    }
}

pub fn get_parquet_file_slice_reader(
    file_name: String,
    start_pos: u64,
    end_pos: u64,
) -> Result<SerializedFileReader<File>, parquet::errors::ParquetError> {
    let file = File::open(file_name)?;
    let options = ReadOptionsBuilder::new()
        .with_range(start_pos as i64, end_pos as i64)
        .build();
    SerializedFileReader::new_with_options(file, options)
}

fn words_from_parquet_file(input_file: File, column: &str) -> Option<String> {
    let reader = SerializedFileReader::new(input_file).unwrap();

    let parquet_metadata: &ParquetMetaData = reader.metadata();
    assert_eq!(parquet_metadata.num_row_groups(), 1);

    // let row_group_reader = reader.get_row_group(0).unwrap();
    let mut row_iter = reader.get_row_iter(None).ok()?;
    let mut res = row_iter.next();
    res.map(|r| r.to_string())
    //let iter = row_iter.map(|row| row.to_string());
}
