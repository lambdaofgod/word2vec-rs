extern crate rand;
use libc;
use std::sync::Arc;
use std::mem::size_of;
use crate::matrix::Matrix;
use crate::saxpy;


use crate::{MAX_SIGMOID, SIGMOID_TABLE_SIZE, LOG_TABLE_SIZE};
const SIGMOID_TABLE_SIZE_F: f32 = SIGMOID_TABLE_SIZE as f32;
const LOG_TABLE_SIZE_F: f32 = LOG_TABLE_SIZE as f32;


fn init_sigmoid_table() -> [f32; SIGMOID_TABLE_SIZE + 1] {
    let mut sigmoid_table = [0f32; SIGMOID_TABLE_SIZE + 1];
    for i in 0..SIGMOID_TABLE_SIZE + 1 {
        let x = (i as f32 * 2. * MAX_SIGMOID) / SIGMOID_TABLE_SIZE_F - MAX_SIGMOID;
        sigmoid_table[i] = 1.0 / (1.0 + (-x).exp());
    }
    sigmoid_table
}
fn init_log_table() -> [f32; LOG_TABLE_SIZE + 1] {
    let mut log_table = [0f32; LOG_TABLE_SIZE + 1];
    for i in 0..LOG_TABLE_SIZE + 1 {
        let x = (i as f32 + 1e-5) / LOG_TABLE_SIZE_F;
        log_table[i] = x.ln();
    }
    log_table
}

pub struct Model<'a> {
    pub input: &'a mut Matrix,
    output: &'a mut Matrix,
    dim: usize,
    lr: f32,
    neg: usize,
    grad_: Vec<f32>,
    neg_pos: usize,
    sigmoid_table: [f32; SIGMOID_TABLE_SIZE + 1],
    log_table: [f32; LOG_TABLE_SIZE + 1],
    negative_table: Arc<Vec<usize>>,
    loss: f64,
    nsamples: u64, /* t_loss: f32,
                    * t_sample: u64, */
}
impl<'a> Model<'a> {
    pub fn new(input: &'a mut Matrix,
               output: &'a mut Matrix,
               dim: usize,
               lr: f32,
               // tid: u32,
               neg: usize,
               neg_table: Arc<Vec<usize>>)
               -> Model<'a> {
        Model {
            input: input,
            output: output,
            dim: dim,
            lr: lr,
            neg: neg,
            grad_: vec![0f32;dim],
            neg_pos: 0,
            sigmoid_table: init_sigmoid_table(),
            log_table: init_log_table(),
            negative_table: neg_table,
            loss: 0.,
            nsamples: 0, /* t_sample: 0,
                          * t_loss: 0., */
        }

    }
    #[inline]
    fn log(&self, x: f32) -> f32 {
        if x > 1.0 {
            x
        } else {
            let i = (x * (LOG_TABLE_SIZE_F)) as usize;
            unsafe { *self.log_table.get_unchecked(i) }
        }
    }
    #[inline]
    fn sigmoid(&self, x: f32) -> f32 {
        if x < -MAX_SIGMOID {
            0f32
        } else if x > MAX_SIGMOID {
            1f32
        } else {
            let i = (x + MAX_SIGMOID) * SIGMOID_TABLE_SIZE_F / MAX_SIGMOID / 2.;
            unsafe { *self.sigmoid_table.get_unchecked(i as usize) }
        }
    }
    #[inline]
    pub fn get_loss(&self) -> f64 {
        self.loss / self.nsamples as f64
    }
    #[inline(always)]
    pub fn set_lr(&mut self, lr: f32) {
        self.lr = lr;
    }
    #[inline(always)]
    pub fn get_lr(&self) -> f32 {
        self.lr
    }

    fn binary_losgistic(&mut self, input_emb: *mut f32, target: usize, label: i32) -> f32 {
        let sum = self.output.dot_row(input_emb, target);
        let score = self.sigmoid(sum);
        let alpha = self.lr * (label as f32 - score);
        let tar_emb = self.output.get_row(target);
        self.add_mul_row(tar_emb, alpha);
        self.output.add_row(input_emb, target, alpha);
        if label == 1 {
            -self.log(score)
        } else {
            -self.log(1.0 - score)
        }
    }
    #[inline(always)]
    pub fn update(&mut self, input: usize, target: usize) {
        self.loss += self.negative_sampling(input, target);
        self.nsamples += 1;
    }

    fn negative_sampling(&mut self, input: usize, target: usize) -> f64 {
        let input_emb = self.input.get_row(input);
        let mut loss = 0f32;
        self.grad_zero();
        for i in 0..self.neg + 1 {
            if i == 0 {
                loss += self.binary_losgistic(input_emb, target, 1);
            } else {
                let neg_sample = self.get_negative(target);
                loss += self.binary_losgistic(input_emb, neg_sample, 0);
            }
        }
        self.input.add_row(self.grad_.as_mut_ptr(), input, 1.0);
        loss as f64
    }
    fn get_negative(&mut self, target: usize) -> usize {
        loop {
            let negative = self.negative_table[self.neg_pos];
            self.neg_pos = (self.neg_pos + 1) % self.negative_table.len();
            if target != negative {
                return negative;
            }
        }
    }

    #[inline(always)]
    fn grad_zero(&mut self) {
        unsafe {
            libc::memset(self.grad_.as_mut_ptr() as *mut libc::c_void,
                         0,
                         self.dim * size_of::<f32>())
        };
    }


    #[inline(always)]
    fn add_mul_row(&mut self, other: *const f32, a: f32) {
        unsafe {
            saxpy(self.grad_.as_mut_ptr(),other,a,self.dim);
        }
    }

}
