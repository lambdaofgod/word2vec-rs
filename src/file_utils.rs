use std::{io::{stdout, BufRead, BufReader, Read, Seek, SeekFrom, Write, Lines}, fs::read};
use parquet::{file::reader::{FileReader, SerializedFileReader}, record::reader::RowIter};
use std::fs::{metadata, File};

fn read_lines_from_file(file: File) -> std::io::Lines<std::io::BufReader<File>> {
    let br= std::io::BufReader::new(file);
    br.lines()
}
 
trait LineReader <Err: std::error::Error,T: Iterator<Item = Result<String, Err>>> {
    fn next_line(&mut self) -> Result<String, Err>;
}

struct FileBufLineReader<'a> {
    buf_reader: &'a mut BufReader<File>
}
impl<'a> LineReader<std::io::Error, Lines<BufReader<File>>> for FileBufLineReader<'a> {
    fn next_line(&mut self) -> Result<String, std::io::Error> {
        let mut lines= self.buf_reader.lines();
        let res = lines.next().transpose();
        res.map(|opt| opt.unwrap())
    }
}

fn words_from_parquet_file(input_file: File, column: &str){
    let reader = SerializedFileReader::new(input_file).unwrap();

    let parquet_metadata = reader.metadata();
    assert_eq!(parquet_metadata.num_row_groups(), 1);

    let row_group_reader = reader.get_row_group(0).unwrap();
    let row_iter = reader.get_row_iter(None);
    //let iter = row_iter.map(|row| row.to_string());
}
