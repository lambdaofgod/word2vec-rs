extern crate rand;
extern crate time;
extern crate libc;
mod model;
use model::Model;

mod dictionary;
use dictionary::Dict;
mod matrix;
use matrix::Matrix;
mod utils;
pub use utils::{Argument, parse_arguments, Command};

mod file_utils;
use file_utils::{LineIterable};
mod train;
pub use train::train;
const SIGMOID_TABLE_SIZE: usize = 512;
const MAX_SIGMOID: f32 = 8f32;
const NEGATIVE_TABLE_SIZE: usize = 10000000;
const LOG_TABLE_SIZE: usize = 512;

mod w2v;
use w2v::Word2vec;
pub use utils::W2vError;
#[macro_use]
extern crate clap;

mod ffi;
use ffi::*;

use libc::size_t;

#[link(name = "vec_arith")]
extern {
    fn simd_dot_product_x4(a: *const f32,b:*const f32,
                            length: size_t )->f32;
    fn simd_saxpy(dst:* mut f32, source:*const f32,scale:f32,size:size_t);

    fn saxpy_x4(dst:* mut f32, source:*const f32,scale:f32,size:size_t);

    fn simd_dot_product(a: *const f32,b:*const f32,
                            length: size_t )->f32;

    fn dot_product(a: *const f32,b:*const f32,
                            length: size_t )->f32;


    fn saxpy(dst:* mut f32, source:*const f32,scale:f32,size:size_t);
    /*
    fn snappy_compress(input: *const u8,
                       input_length: size_t,
                       compressed: *mut u8,
                       compressed_length: *mut size_t) -> c_int;
    fn snappy_uncompress(compressed: *const u8,
                         compressed_length: size_t,
                         uncompressed: *mut u8,
                         uncompressed_length: *mut size_t) -> c_int;
    fn snappy_max_compressed_length(source_length: size_t) -> size_t;
    fn snappy_uncompressed_length(compressed: *const u8,
                                  compressed_length: size_t,
                                  result: *mut size_t) -> c_int;
    fn snappy_validate_compressed_buffer(compressed: *const u8,
                                         compressed_length: size_t) -> c_int;
    */
}
