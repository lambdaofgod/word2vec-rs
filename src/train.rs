use crate::file_utils::TakeBufIter;
use crate::W2vError;
use crate::Word2vec;
use crate::{Argument, Dict, Matrix, Model};
use parquet::file::reader::FileReader;
use parquet::file::reader::SerializedFileReader;
use rand::distributions::{IndependentSample, Range};
use rand::StdRng;
use std::fs::{metadata, File};
use std::io::Take;
use std::io::{stdout, BufRead, BufReader, Lines, Read, Seek, SeekFrom, Write};
use std::iter;
use std::ops;
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::sync::Arc;
use std::thread;
use time::Instant;
static ALL_WORDS: AtomicUsize = ATOMIC_USIZE_INIT;
use crate::file_utils::LineIterable;

fn skipgram(model: &mut Model, line: &Vec<usize>, rng: &mut StdRng, unifrom: &Range<isize>) {
    let length = line.len() as i32;
    for w in 0..length {
        let bound = unifrom.ind_sample(rng) as i32;
        for c in -bound..bound + 1 {
            if c != 0 && w + c >= 0 && w + c < length {
                model.update(line[w as usize], line[(w + c) as usize]);
            }
        }
    }
}
fn print_progress(model: &Model, progress: f32, words: f32, start_time: &Instant) {
    print!(
        "\rProgress:{:.1}% words/sec:{:<7.0} lr:{:.4} loss:{:.5}",
        progress * 100.,
        ((words * 1.) / (start_time.elapsed().whole_seconds()) as f32) as u64,
        model.get_lr(),
        model.get_loss()
    );
    stdout().flush().unwrap();
}

/*
pass part of file to a thread and run training
*/
fn train_thread(
    dict: &Dict,
    mut input: &mut Matrix,
    mut output: &mut Matrix,
    arg: Argument,
    tid: u32,
    neg_table: Arc<Vec<usize>>,
    start_pos: u64,
    end_pos: u64,
) -> Result<bool, W2vError> {
    let between = Range::new(1, (arg.win + 1) as isize);
    let mut rng = StdRng::new().unwrap();
    let mut model = Model::new(&mut input, &mut output, arg.dim, arg.lr, arg.neg, neg_table);
    let start_time = Instant::now();
    let mut line: Vec<usize> = Vec::new();
    let (mut token_count, mut epoch) = (0, 0);
    let all_tokens = arg.epoch as usize * dict.ntokens;
    while epoch < arg.epoch {
        let mut buffer = String::new();
        let line_iterable = LineIterable::new(arg.input.clone(), crate::file_utils::LoadOptions::Parquet(start_pos as usize))?;
        let mut b_iter = line_iterable.to_iter();
        for mut line_buffer in b_iter {
            println!("line buffer: {}", line_buffer);
            break;
            token_count += dict.read_line(&mut line_buffer, &mut line);
            line_buffer.clear();
            skipgram(&mut model, &line, &mut rng, &between);
            line.clear();
            if token_count > arg.lr_update as usize {
                let words = ALL_WORDS.fetch_add(token_count, Ordering::SeqCst) as f32;
                let progress = words / all_tokens as f32;
                model.set_lr(arg.lr * (1.0 - progress));
                token_count = 0;
                if tid == 0 {
                    //if arg.verbose {
                    print_progress(&model, progress, words, &start_time);
                    //}
                }
            }
        }
        epoch += 1;
    }
    ALL_WORDS.fetch_add(token_count, Ordering::SeqCst);
    if tid == 0 && arg.verbose {
        loop {
            let words = ALL_WORDS.load(Ordering::SeqCst);
            let progress = words as f32 / all_tokens as f32;
            print_progress(&model, progress, words as f32, &start_time);
            if words >= all_tokens {
                assert_eq!(words, all_tokens);
                print_progress(&model, progress, words as f32, &start_time);
                println!(
                    "\ntotal train time:{} s",
                    start_time.elapsed().whole_seconds()
                );
                break;
            }
        }
    }
    Ok(true)
}
fn text_file_split_indices(filename: &str, n_split: u64) -> Result<Vec<u64>, W2vError> {
    let all_tokens = metadata(filename)?.len();
    let input_file = File::open(filename)?;
    let mut reader = BufReader::with_capacity(1000, input_file);
    let offset = all_tokens / n_split;
    let mut junk = Vec::new();
    let mut bytes = Vec::new();
    bytes.push(0);
    for i in 1..n_split {
        reader.seek(SeekFrom::Start(offset * i)).unwrap();
        let extra = reader.read_until(b'\n', &mut junk)?;
        bytes.push(offset * i + extra as u64);
    }
    bytes.push(all_tokens);
    Ok(bytes)
}

fn parquet_file_split_indices(filename: &str, n_split: u64) -> Result<Vec<u64>, W2vError> {
    let file = File::open(filename)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata().file_metadata();
    let n_rows = metadata.num_rows();
    let rng = ops::Range {
        start: 0,
        end: n_rows,
    };
    let offset = ((n_rows as u64) / n_split) as usize;

    let mut split_indices: Vec<u64> = rng.step_by(offset).map(|u| u as u64).collect();
    split_indices.push(n_rows as u64);
    Ok(split_indices)
}

fn file_split_indices(filename: &str, n_split: u64) -> Result<Vec<u64>, W2vError> {
    if !filename.contains("parquet") {
        text_file_split_indices(filename, n_split)
    } else {
        parquet_file_split_indices(filename, n_split)
    }
}

pub fn train(args: &Argument) -> Result<Word2vec, W2vError> {
    let dict = Dict::new_from_file(
        &args.input,
        args.min_count,
        args.threshold,
        args.verbose,
        args.max_rows,
    )?;

    let dict = Arc::new(dict);
    let mut input_mat = Matrix::new(dict.nsize(), args.dim);
    let mut output_mat = Matrix::new(dict.nsize(), args.dim);

    input_mat.unifrom(1.0f32 / args.dim as f32);
    output_mat.zero();
    let input = Arc::new(input_mat.make_send());
    let output = Arc::new(output_mat.make_send());
    let neg_table = dict.init_negative_table();
    let splits = file_split_indices(&args.input, args.nthreads as u64)?;
    let mut handles = Vec::new();
    for i in 0..args.nthreads {
        let (input, output, dict, arg, neg_table) = (
            input.clone(),
            output.clone(),
            dict.clone(),
            args.clone(),
            neg_table.clone(),
        );
        let splits = splits.clone();
        handles.push(thread::spawn(move || {
            let dict: &Dict = dict.as_ref();
            let input = input.as_ref().inner.get();
            let output = output.as_ref().inner.get();
            train_thread(
                &dict,
                unsafe { &mut *input },
                unsafe { &mut *output },
                arg,
                i,
                neg_table,
                splits[i as usize],
                splits[(i + 1) as usize],
            )
        }));
    }
    for h in handles {
        h.join().unwrap()?;
    }

    let input = Arc::try_unwrap(input).unwrap();
    let output = Arc::try_unwrap(output).unwrap();
    let dict = Arc::try_unwrap(dict).unwrap();
    let w2v = Word2vec::new(
        unsafe { input.inner.into_inner() },
        unsafe { output.inner.into_inner() },
        args.dim,
        dict,
    );
    Ok(w2v)
}
