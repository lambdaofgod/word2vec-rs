use clap;
use std::error;
use std::fmt;
use std::io;
use std::num;
#[derive(Debug)]
pub enum W2vError {
    File(io::Error),
    RuntimeError,
}
impl From<io::Error> for W2vError {
    fn from(err: io::Error) -> W2vError {
        W2vError::File(err)
    }
}

impl fmt::Display for W2vError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            W2vError::File(ref reason) => write!(f, "open file error:{}", reason),
            W2vError::RuntimeError => write!(f, "word2vec runtime error"),
        }
    }
}

impl error::Error for W2vError {
    fn description(&self) -> &str {
        match *self {
            W2vError::File(ref err) => err.description(),
            W2vError::RuntimeError => "RuntimeError",
        }
    }
}
pub enum ArgumentError {
    ParseArg(clap::Error),
    ParseInt(num::ParseIntError),
    ParseFloat(num::ParseFloatError),
}
impl From<clap::Error> for ArgumentError {
    fn from(err: clap::Error) -> ArgumentError {
        ArgumentError::ParseArg(err)
    }
}
impl From<num::ParseIntError> for ArgumentError {
    fn from(err: num::ParseIntError) -> ArgumentError {
        ArgumentError::ParseInt(err)
    }
}

impl From<num::ParseFloatError> for ArgumentError {
    fn from(err: num::ParseFloatError) -> ArgumentError {
        ArgumentError::ParseFloat(err)
    }
}

impl fmt::Display for ArgumentError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ArgumentError::ParseArg(ref err) => write!(f, "Parse args:{}", err),
            ArgumentError::ParseInt(ref err) => write!(f, "Parse int:{}", err),
            ArgumentError::ParseFloat(ref err) => write!(f, "Parse float:{}", err),
        }
    }
}
impl fmt::Debug for ArgumentError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ArgumentError::ParseArg(ref err) => write!(f, "Parse args:{:?}", err),
            ArgumentError::ParseInt(ref err) => write!(f, "Parse int:{:?}", err),
            ArgumentError::ParseFloat(ref err) => write!(f, "Parse float:{:?}", err),
        }
    }
}
impl error::Error for ArgumentError {
    fn description(&self) -> &str {
        match *self {
            ArgumentError::ParseArg(ref err) => err.description(),
            ArgumentError::ParseFloat(ref err) => err.description(),
            ArgumentError::ParseInt(ref err) => err.description(),
        }
    }
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            ArgumentError::ParseArg(ref err) => Some(err),
            ArgumentError::ParseFloat(ref err) => Some(err),
            ArgumentError::ParseInt(ref err) => Some(err),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Command {
    Train,
    Test,
}

#[derive(Debug, Clone)]
pub struct Argument {
    pub input: String,
    pub output: String,
    pub lr: f32,
    pub dim: usize,
    pub win: usize,
    pub epoch: u32,
    pub neg: usize,
    pub nthreads: u32,
    pub min_count: u32,
    pub threshold: f32,
    pub lr_update: u32,
    pub command: Command,
    pub verbose: bool,
    pub max_rows: Option<usize>,
}

struct ArgumentBuilder {
    pub input: String,
    pub output: String,
    pub lr: f32,
    pub dim: usize,
    pub win: usize,
    pub epoch: u32,
    pub neg: usize,
    pub nthreads: u32,
    pub min_count: u32,
    pub threshold: f32,
    pub lr_update: u32,
    pub command: Command,
    pub verbose: bool,
    pub max_rows: Option<usize>,
}
impl ArgumentBuilder {
    pub fn new(input: String, command: Command) -> ArgumentBuilder {
        ArgumentBuilder {
            input: input,
            output: "".to_string(),
            lr: 0.05,
            dim: 100,
            win: 5,
            epoch: 5,
            neg: 5,
            nthreads: 12,
            min_count: 5,
            threshold: 1e-4,
            lr_update: 100,
            command: command,
            verbose: false,
            max_rows: None,
        }
    }
    #[allow(dead_code)]
    pub fn output(&mut self, output: String) -> &mut Self {
        self.output = output;
        self
    }
    #[allow(dead_code)]
    pub fn lr(&mut self, lr: f32) -> &mut Self {
        self.lr = lr;
        self
    }
    #[allow(dead_code)]
    pub fn dim(&mut self, dim: usize) -> &mut Self {
        self.dim = dim;
        self
    }
    #[allow(dead_code)]
    pub fn win(&mut self, win: usize) -> &mut Self {
        self.win = win;
        self
    }
    #[allow(dead_code)]
    pub fn epoch(&mut self, epoch: u32) -> &mut Self {
        self.epoch = epoch;
        self
    }
    #[allow(dead_code)]
    pub fn neg(&mut self, neg: usize) -> &mut Self {
        self.neg = neg;
        self
    }
    #[allow(dead_code)]
    pub fn threads(&mut self, threads: u32) -> &mut Self {
        self.nthreads = threads;
        self
    }
    #[allow(dead_code)]
    pub fn min_count(&mut self, min_count: u32) -> &mut Self {
        self.min_count = min_count;
        self
    }
    #[allow(dead_code)]
    pub fn threshold(&mut self, threshold: f32) -> &mut Self {
        self.threshold = threshold;
        self
    }
    #[allow(dead_code)]
    pub fn lr_update(&mut self, lr_update: u32) -> &mut Self {
        self.lr_update = lr_update;
        self
    }
    #[allow(dead_code)]
    pub fn verbose(&mut self, verbose: bool) -> &mut Self {
        self.verbose = verbose;
        self
    }
    #[allow(dead_code)]
    pub fn max_rows(&mut self, max_rows: Option<usize>) -> &mut Self {
        self.max_rows = max_rows;
        self
    }
    #[allow(dead_code)]
    pub fn finalize(&self) -> Argument {
        Argument {
            input: self.input.to_owned(),
            output: self.output.to_owned(),
            win: self.win,
            epoch: self.epoch,
            lr: self.lr,
            dim: self.dim,
            neg: self.neg,
            nthreads: self.nthreads,
            min_count: self.min_count,
            threshold: self.threshold,
            lr_update: self.lr_update,
            command: self.command,
            verbose: self.verbose,
            max_rows: self.max_rows,
        }
    }
}

pub fn parse_arguments<'a>(args: &'a Vec<String>) -> Result<Argument, ArgumentError> {
    let app = clap_app!(word2vec =>
        (version: "1.0")
        (author: "Frank Lee <golifang1234@gmail.com>")
        (about: "word2vec implemention for rust")
        (@subcommand test =>
        (about: "test word similarity")
        (@arg input:+required "input parameter file path( use train subcommand to train a model)")
        (@arg verbose: --verbose "print internal log")
        )
       (@subcommand train =>
            (about: "train model")
            (version: "0.1")
         //argument
        (@arg input: +required "input corpus file path")
        (@arg output: +required "file name to save params")
        //options
        (@arg win: --win +takes_value "window size(5)")
        (@arg neg: --neg +takes_value "negative sampling size(5)")
        (@arg lr: --lr +takes_value "learning rate(0.05)")
        (@arg lr_update: --lr_update +takes_value "learning rate update rate(100)")
        (@arg dim: --dim +takes_value "size of word vectors(100)")
        (@arg epoch: --epoch +takes_value "number of epochs(5)")
        (@arg min_count: --min_count +takes_value "number of word occurences(5)")
        (@arg nthreads: --thread +takes_value "number of threads(12)")
        (@arg threshold: --threshold +takes_value "sampling threshold(1e-4)")
        (@arg verbose: --verbose "print internal log")
        (@arg max_rows: --max_rows +takes_value "maximal rows from the training corpus")
       )
    );
    let matches = app.get_matches_from(args);

    if let Some(train_info) = matches.subcommand_matches("train") {
        let input = train_info
            .value_of("input")
            .ok_or(clap::Error::argument_not_found_auto("input"));
        let output = train_info
            .value_of("output")
            .ok_or(clap::Error::argument_not_found_auto("output"));
        let win = train_info.value_of("win").unwrap_or("5").parse::<usize>()?;
        let neg = train_info.value_of("neg").unwrap_or("5").parse::<usize>()?;
        let lr = train_info.value_of("lr").unwrap_or("0.05").parse::<f32>()?;
        let lr_update = train_info
            .value_of("lr_update")
            .unwrap_or("5000")
            .parse::<u32>()?;
        let vector_size = train_info
            .value_of("dim")
            .unwrap_or("100")
            .parse::<usize>()?;
        let epoch = train_info.value_of("epoch").unwrap_or("5").parse::<u32>()?;
        let min_count = train_info
            .value_of("min_count")
            .unwrap_or("5")
            .parse::<u32>()?;
        let nthreads = train_info
            .value_of("nthreads")
            .unwrap_or("12")
            .parse::<u32>()?;
        let threshold = train_info
            .value_of("threshold")
            .unwrap_or("1e-4")
            .parse::<f32>()?;
        let max_rows = match train_info.value_of("max_rows") {
            Some(s) => Some(str::parse::<usize>(s)?),
            None => None,
        };
        Ok(Argument {
            input: input?.to_string(),
            output: output?.to_string(),
            lr: lr,
            dim: vector_size,
            win: win,
            epoch: epoch,
            neg: neg,
            nthreads: nthreads,
            min_count: min_count,
            threshold: threshold,
            lr_update: lr_update,
            command: Command::Train,
            verbose: train_info.is_present("verbose"),
            max_rows: max_rows,
        })
    } else if let Some(ref test_info) = matches.subcommand_matches("test") {
        let input = test_info
            .value_of("input")
            .ok_or(clap::Error::argument_not_found_auto("input"));
        Ok(ArgumentBuilder::new(input?.to_string(), Command::Test)
            .verbose(test_info.is_present("verbose"))
            .finalize())
    } else {
        Err(ArgumentError::ParseArg(
            clap::Error::argument_not_found_auto("missing arguments"),
        ))
    }
}
