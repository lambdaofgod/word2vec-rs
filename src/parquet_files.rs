use crate::utils::W2vError;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::reader::RowIter;
use std::fs::{metadata, File};

pub struct GetParqetStrIterator<'a> {
    row_iter: RowIter<'a>,
    reader: SerializedFileReader<File>,
}
/*
impl<'a> GetParqetStrIterator<'a> {
    fn setup_reader(file: File, gp: usize) -> Result<GetParqetStrIterator<'a>, W2vError> {
        let builder = ParquetStrIteratorBuilder::new();
        builder.set_reader(file)?.set_row_iter(gp)?;
        Ok(builder.to_parquet_str_iterator().unwrap())
    }
}
*/

impl<'a> Iterator for GetParqetStrIterator<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        self.row_iter.next().map(|r| r.to_string())
    }
}

pub struct ParquetGroupReader {
    reader: SerializedFileReader<File>,
    gp: usize,
}

impl ParquetGroupReader {
    pub fn from_file(file: File, gp: usize) -> Result<Self, W2vError> {
        Ok(ParquetGroupReader {
            reader: SerializedFileReader::new(file)?,
            gp: gp
        })
    }
    pub fn into_iter<'a>(self) -> Result<GetParqetStrIterator<'a>, W2vError> {
        let rg = self.reader.get_row_group(self.gp)?;
        Ok(GetParqetStrIterator {
            reader: self.reader,
            row_iter: rg.get_row_iter(None)?,
        })
    }
}
