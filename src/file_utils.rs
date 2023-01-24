use std::{io::{stdout, BufRead, BufReader, Read, Seek, SeekFrom, Write, Lines}, fs::read};
use parquet::{file::reader::{FileReader, SerializedFileReader}, record::reader::RowIter, record::Row};
use std::fs::{metadata, File};

fn read_lines_from_file(file: File) -> std::io::Lines<std::io::BufReader<File>> {
    let br= std::io::BufReader::new(file);
    br.lines()
}
 
trait LineReader<Err, ItemType, IterType: Iterator<Item = ItemType>> {
    fn next_line(&mut self) -> Result<String, Err>;
//fn from_file(file: File) -> Self;
    fn to_string(item: ItemType) -> String;
}

pub struct FileBufLineReader {
    lines: Lines<BufReader<File>>
}
impl LineReader<std::io::Error, Result<String; std::io::Error>, Lines<BufReader<File>>> for FileBufLineReader {
    fn next_line(&mut self) -> Result<String, std::io::Error> {
        let res = self.lines.next().transpose();
        res.map(|opt| opt.unwrap())
    }
    //fn from_file<'a>(file: File) -> FileBufLineReader {
    //    let reader = FileBufLineReader { lines: BufReader::new(file).lines() };
    //    reader
    //}
    fn to_string(item: String) -> String {
        item
    }
}

pub struct ParquetLineReader<'a> {
    row_iter: RowIter<'a>
}
impl <'a> LineReader<parquet::errors::ParquetError, Row, RowIter<'a>> for ParquetLineReader<'a> {
    fn next_line(&mut self) -> Result<String, parquet::errors::ParquetError> {
        // jesli to jebnie to rezultat typu bedzie zly
        let next_value = self.row_iter.next().map(Self::to_string(item));
        next_value.unwrap()
    }
    fn to_string(item: Row) -> String {
        item.to_string()
    }
}

fn words_from_parquet_file(input_file: File, column: &str) -> Result<String, parquet::errors::ParquetError> {
    let reader = SerializedFileReader::new(input_file).unwrap();

    let parquet_metadata = reader.metadata();
    assert_eq!(parquet_metadata.num_row_groups(), 1);

    let row_group_reader = reader.get_row_group(0).unwrap();
    let row_iter = reader.get_row_iter(None)?;
    let res = row_iter;
    Ok(res.next().to_string())
    //let iter = row_iter.map(|row| row.to_string());
}
