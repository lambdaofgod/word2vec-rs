use parquet::{
    file::reader::{ChunkReader, FileReader, SerializedFileReader, FilePageIterator},
    record::reader::RowIter,
    record::Row,
};
use std::{fs::{metadata, File}, sync::Arc};
use std::{
    fs::read,
    io::{stdout, BufRead, BufReader, Lines, Read, Seek, SeekFrom, Take, Write},
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
}
impl TakeBufStrReader {
    pub fn next_line<'a>(&mut self, buf: &'a mut String) -> bool {
        let res = self.handle.read_line(buf);
        match res {
            Ok(0) => false,
            Ok(_) => true,
            _ => false 
        }
    }

    //fn from_file<'a>(file: File) -> FileBufLineReader {
    //    let reader = FileBufLineReader { lines: BufReader::new(file).lines() };
    //    reader
    //}
}


pub struct TakeBufIter<'a> {
    pub handle: Take<BufReader<File>>,
    pub buf: &'a mut String
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
            _ => None
        }
    }

    //fn from_file<'a>(file: File) -> FileBufLineReader {
    //    let reader = FileBufLineReader { lines: BufReader::new(file).lines() };
    //    reader
    //}
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

fn parquet_slice_row_iter(
    file_name: &str,
    start_pos: u64,
    end_pos: u64) -> Result<(), parquet::errors::ParquetError> {
    let reader = Arc::new(SerializedFileReader::new(File::open(file_name)?)?);
    // idea: use row groups to control parallelism
    Ok(())
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

fn words_from_parquet_file(input_file: File, column: &str) -> Option<String> {
    let reader = SerializedFileReader::new(input_file).unwrap();

    let parquet_metadata = reader.metadata();
    assert_eq!(parquet_metadata.num_row_groups(), 1);

    // let row_group_reader = reader.get_row_group(0).unwrap();
    let mut row_iter = reader.get_row_iter(None).ok()?;
    let mut res = row_iter.next();
    res.map(|r| r.to_string())
    //let iter = row_iter.map(|row| row.to_string());
}
