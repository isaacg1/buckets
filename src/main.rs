#![allow(non_snake_case, dead_code, unused_assignments, non_camel_case_types)]
use integer_partitions::Partitions;
use noisy_float::prelude::*;
use rand::prelude::*;
use rand_distr::Exp;
use rand_distr::Gamma;
use rand_distr::Uniform;
use smallvec::{SmallVec, smallvec};
use std::f64::INFINITY;
use std::collections::HashSet;
use statrs::distribution::{Normal as StatNormal, ContinuousCDF};
//use rand::seq::SliceRandom;

//hy: time count
use std::time::Instant;


// hy: Read csv files
use std::fs::File;
use std::io::{BufRead, BufReader};


const EPSILON: f64 = 1e-8;
const DEBUG: bool = false;
//const DEBUG: bool = true;

fn main() {
    println!("Lambda; Mean Response Time;");

    //let dist = Dist::Hyperexp(1.0,job_size_mu,0.5);
    //let dist = Dist::Gamma(3.0, 0.3);
    //let dist = Dist::Uniform(0.01,1.0);
    let dist = Dist::Expon(1.0);
    let num_servers = 1;
    let num_jobs = 1_000_000;
    // let num_jobs = 1_000_000;
    // let num_jobs = 7010740;
    // let num_jobs = 19366;
    let seed = 3;

    // homogenous job service requirement:
    // let job_req_dist = Dist::Constant(0.45);
    // let job_req_dist = Dist::Uniform(0.0, 1.0);
    // let job_req_dist = Dist::MUnif(
    //     [0.2, 0.8],   
    //     [0.8, 0.2],   
    // );
    // let job_req_dist = Dist::Triangular(1.0);
    // let job_req_dist = Dist::BExp(0.5);
    // let job_req_dist = Dist::BLomax(2.0, 1.0);

    // let job_req_dist = Dist:: TruncatedN(0.5,1.0);
    let job_req_dist = Dist:: TruncatedN(0.375, 0.083*0.083);
    // let job_req_dist = Dist:: TwoUnif( 0.45, 0.5, 0.4, 0.28, 0.33, 0.6 );
    // let job_req_dist = Dist:: TwoUnif( 0.22, 0.25, 0.5714, 0.30, 0.33, 0.4286 );
    // let job_req_dist = Dist::TwoTruncatedN(0.24, 0.005*0.005,0.5714, 0.32, 0.005*0.005, 0.4286);
    // let job_req_dist = Dist::TwoTruncatedN(0.48, 0.005*0.005,0.4, 0.32, 0.005*0.005, 0.6);
    // let job_req_dist = Dist::TwoTruncatedN(0.29, 0.083*0.083,0.7, 0.95, 0.05*0.05, 0.3);

    // let policy = Policy::AdaptiveBPTB(2.0);
    // let policy = Policy::FCFS;
    // let policy = Policy::FCFSB;
    // let policy = Policy::LSF;
    // let policy = Policy::MSF;
    // let policy = Policy::DB(64);
    // let policy = Policy::DBB(16);
    // let policy = Policy::BPT(8);
    // let policy = Policy::BPTB(16);
    // let policy = Policy::AdaptiveDBB(1.5);
    // let policy = Policy::BPTB(64);
    // let policy = Policy::BPTB(1024);
    // let policy = Policy::AdaptiveBPTB(1.5);

    // let policy =Policy:: IPB(12);

    // let policy =Policy:: IPBB(12);
    // let policies = vec![Policy::DB(4),Policy::DB(8), Policy::DB(16),Policy::DB(32), Policy::DB(64),
    //                     Policy::DBB(4),Policy::DBB(8), Policy::DBB(16),Policy::DBB(32), Policy::DBB(64),
    //                     Policy::FCFS, Policy::FCFSB,Policy::MSF,Policy::AdaptiveBPTB(1.5),Policy::AdaptiveBPTB(2.0)];

    let policies = vec![
                        // Policy::DB(8),
                        // Policy::DB(16),
                        // Policy::DB(64),
                        // Policy::DBB(8),
                        // Policy::DBB(16),
                        // Policy::DBB(64),
                        // Policy::FCFS, // 2.1
                        // Policy::FCFSB, // 2.3
                        // Policy::MSF, // 2.3
                        // Policy::BPT(8),
                        // Policy::BPT(64),
                        //Policy::BPTB(64),
                        // Policy::BPTB(1024),
                        // Policy:: IPB(8),
                        // Policy:: IPB(16),
                         Policy:: IPB(24),
                        // Policy:: IPBB(6),
                        // Policy:: IPBB(18),
                        // Policy:: IPBB(8),
                        // Policy:: IPBB(16),
                        // Policy::LSF,
                        // Policy::AdaptiveBPTB(1.5),
    ];

    // let main_req_trace = read_one_col_csv("Memory_Borg.csv");
    // let main_req_trace: Vec<f64> = main_req_trace[..30_000].to_vec();
    // for &policy in &policies {
        // for seed in 0..10 {
        //let mut rng = StdRng::seed_from_u64(seed);
        //let mut req_trace = main_req_trace.clone();
        //req_trace.shuffle(&mut rng);
        for &policy in &policies {
            // HY: If we do SIMPLE CONTINUOUS job req dist.
            println!(
                "Policy : {:?}, Duration: {:?}, Requirement: {:?}, Jobs per data point: {}, Seed: {}",
                policy, dist, job_req_dist, num_jobs, seed
            );

            // HY: If we use real-world data

            // println!(
            //     "Policy : {:?}, Duration: {:?}, Seed: {}",
            //     policy, dist, seed
            // );
            println!(
                " Duration: {:?}, Seed: {}",
                dist, seed
            );
            let mut lambdas: Vec<f64> = Vec::new();

            // for i in (13..=14).rev() {
            for i in 24..=27{//64 {
                // lambdas.push(i as f64 / 10.0);
                lambdas.push(i as f64 / 10.0);
            }
            // for i in 291..=299 {
            //     lambdas.push(i as f64 / 100.0);
            // }

            for lambda in lambdas {
                // for lam_base in 181..190{
                // for lam_base in 301..310{//} 1..20 {

                // let lambda = lam_base as f64 /100.0;
                // let lambda = lam_base as f64 * 10.0;
                // HY: If we do SIMPLE CONTINUOUS job req dist.
                let start = Instant::now();
                let result = simulateInLoop(
                    policy,
                    num_servers,
                    num_jobs,
                    dist,
                    lambda,
                    seed,
                    job_req_dist,
                );
                //
                // let elapsed = start.elapsed();
                if result.overflow {
                    println!(
                        "{}; OVERFLOW; arrivals={};",
                        lambda, result.num_arrivals,
                    );
                    break;
                } else {
                    println!(
                        "{}; {};",
                        lambda,
                        result.mean_response,
                    );
                    if result.mean_response > 1000.00 as f64 {
                        println!(
                            "{}; OVERFLOW (MRT>1000); arrivals={};",
                            lambda, result.num_arrivals,
                        );
                        break;
                    }

                    // HY: Use this code for simple distribution but not Borg traces
                    // if (result.mean_response > 1000.00 as f64) {
                    //     // let mut lambdas2: Vec<f64> = Vec::new();
                    //     // let mut lambdaStart = (lambda * 10) as i32;
                    //     // for j in lambdaStart+1 as i32..=lambdaStart+9 {
                    //     //     // lambdas.push(i as f64 / 10.0);
                    //     //     lambdas2.push(j as f64 / 100.0);
                    //     // }
                    //     break;
                    // }

                    // println!(
                    //     "{}; {};",
                    //     lambda,
                    //     result.mean_response
                    // );
                }


                // HY: If we use real-world data

                // let start = Instant::now();
                // let result = simulateInLoop_traces_revised(
                //     //let check = simulateInLoop_traces(
                //     num_servers,
                //     lambda,
                //     seed,
                //     &main_req_trace, // HY: some array we can read from traces of, for example, Google Borg memory requirement
                // );
                //
                // let elapsed = start.elapsed();
                // if result.overflow {
                //     println!(
                //         "{}; OVERFLOW; arrivals={}; time={:?}",
                //         lambda, result.num_arrivals, elapsed
                //     );
                //     break;
                // } else {
                //     // println!(
                //     //     "{}; {};{:?}",
                //     //     lambda,
                //     //     result.mean_response,
                //     //     elapsed
                //     // );
                //     println!(
                //         "{}; {};",
                //         lambda,
                //         result.mean_response
                //     );
                // }
            }
    //    }


        // for lam_base in 181..190{
        //     // for lam_base in 301..310{//} 1..20 {
        //     // let lambda = lam_base as f64 / 10.0;
        //     let lambda = lam_base as f64 / 100.0;
        //     // HY: If we do SIMPLE CONTINUOUS job req dist.
        //     let start = Instant::now();
        //     let check = simulate(
        //         policy,
        //         num_servers,
        //         num_jobs,
        //         dist,
        //         lambda,
        //         seed,
        //         job_req_dist,
        //     );
        //     let elapsed = start.elapsed();
        //     // println!("{}; {};", lambda, check);
        //     println!("{}; {}; {:?}", lambda, check, elapsed);
        // }
        }
}

fn read_one_col_csv(path: &str) -> Vec<f64> {
    let file = File::open(path).expect("fail to open this file");
    let reader = BufReader::new(file);
    reader
        .lines()
        .map(|l| l.unwrap().trim().to_string())
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<f64>().expect("parse f64 fails"))
        .collect()
}

#[derive(Debug)]
struct Job {
    arrival_time: f64,
    original_size: f64,
    rem_size: f64,
    service_req: f64,
}

struct ExpJob {
    service_req: f64,
}

// Make a distribution enum

#[derive(Debug, Clone, Copy)]
enum Dist {
    // various distribution functions
    Expon(f64),
    Hyperexp(f64, f64, f64),
    Gamma(f64, f64),
    Uniform(f64, f64),
    Constant(f64),
    BLomax(f64, f64), // hy: Bounded Lomax with shape parameter \alpha and scaling parameter \lambda
    MUnif([f64; 2], [f64; 2]), // hy: mixed uniform with two breakpoints
    //MUnif(Vec<f64>, Vec<f64>), // hy: mixed uniform with decreasing dexsity
    Triangular(f64), //hy: triangular distribution with right endpoint u
    BExp(f64), // hy: Bounded exponential distribution: exponential truncated to [0,1], density \prop e^(−\lambda t)
    TruncatedN(f64, f64), // hy: Bounded normal distribution: normal distribution truncated to [0,1]
    TwoUnif (f64, f64, f64, f64, f64, f64,),
    TwoTruncatedN (f64,  f64, f64,  f64,  f64,  f64),
}

impl Dist {
    fn sample<R: Rng>(&self, rng: &mut R) -> f64 {
        // take a sample from a given distribution
        match self {
            Dist::Hyperexp(low_mu, high_mu, prob_low) => {
                let mu = if *prob_low == 1.0 {
                    low_mu
                } else if rng.r#gen::<f64>() < *prob_low {
                    low_mu
                } else {
                    high_mu
                };
                Exp::new(*mu).unwrap().sample(rng)
            }
            Dist::Expon(lambda) => Exp::new(*lambda).unwrap().sample(rng),

            Dist::Gamma(k, scale) => Gamma::new(*k, *scale).unwrap().sample(rng),
            Dist::Uniform(low, high) => Uniform::try_from(*low..*high).unwrap().sample(rng),
            Dist::Constant(val) => *val,
            Dist::BLomax(alpha, lambda) => {
                // hy: compute the normalization constant c
                let t = 1.0 + 1.0 / lambda;
                let r = t.powf(-alpha);
                let c = 1.0 - r;
                // hy: obtain the CDF and use inverse-transform sampling
                let u = rng.r#gen::<f64>();
                let inner = 1.0 - u * c;
                lambda * (inner.powf(-1.0 / alpha) - 1.0)
            }
            Dist::MUnif(v, p) => {
                let d = v.len();
                // hy: check dimension match
                assert_eq!(p.len(), d, "v and p must have same length");
                // hy: check v_0 = 0 < v_0 < … < v_d ≤ 1
                let mut last = 0.0;
                for &vi in v {
                    assert!(vi > last, "v must be strictly increasing");
                    assert!(vi <= 1.0, "v[d-1] must be ≤ 1");
                    last = vi;
                }
                // hy: check p sums to 1
                let sum_p: f64 = p.iter().sum();
                assert!((sum_p - 1.0).abs() < 1e-12, "p must sum to 1");
                // hy: check decreasing densities: p[i]/(v[i]-v[i-1])
                let mut last_density = f64::INFINITY;
                for i in 0..d {
                    let width = if i == 0 { v[0] } else { v[i] - v[i - 1] };
                    assert!(width > 0.0, "interval width must be positive");
                    let density = p[i] / width;
                    assert!(
                        density < last_density,
                        "p[i]/(v[i]-v[i-1]) must decrease with i"
                    );
                    last_density = density;
                }
                // hy: sample an index i \in {0..d} according to pmf p
                let u = rng.r#gen::<f64>();
                let mut csum = 0.0;
                for i in 0..d {
                    csum += p[i];
                    if u < csum {
                        let low = if i == 0 { 0.0 } else { v[i - 1] };
                        let high = v[i];
                        return Uniform::new(low, high).sample(rng);
                    }
                }
                unreachable!()
            }
            Dist::Triangular(u) => {
                // hy: Inverse‐CDF: F(t) = (2ut - t^2)/u^2, so
                // hy: t = u * (1 - sqrt(1 - U)), U~Unif(0,1)
                let U = rng.r#gen::<f64>();
                u * (1.0 - (1.0 - U).sqrt())
            }
            Dist::BExp(lambda) => {
                let exp_neg = (-lambda).exp();
                let c = 1.0 - exp_neg;

                let u = rng.r#gen::<f64>();
                let inner = 1.0 - u * c;
                -inner.ln() / lambda
            }
            Dist::TruncatedN(m, v) => {
                let sigma = v.sqrt();

                let stdn = StatNormal::new(0.0, 1.0).unwrap();

                let alpha = (0.0 - m) / sigma;
                let beta  = (1.0 - m) / sigma;

                let fa = stdn.cdf(alpha);
                let fb = stdn.cdf(beta);
                let z  = (fb - fa).max(1e-16); 
                let u: f64 = rng.r#gen();
                let y = stdn.inverse_cdf(fa + u * z);    
                let x = m + sigma * y;                  

                x
            }

            Dist::TwoUnif ( a1, b1, p1, a2, b2, p2 ) => {

                let u: f64 = rng.r#gen();
                if u < *p1 {
                Uniform::new(a1, b1).sample(rng)
                } else {
                Uniform::new(a2, b2).sample(rng)
                }
            }
            Dist::TwoTruncatedN ( u1, v1, p1, u2, v2, p2 ) => {

                let u: f64 = rng.r#gen();
                if u < *p1 {
                    Dist::TruncatedN(*u1, *v1).sample(rng)
                } else {
                    Dist::TruncatedN(*u2, *v2).sample(rng)
                }
            }

        }
    }
    fn mean(&self) -> f64 {
        // return the mean of a given distribution
        use Dist::*;
        match self {
            Hyperexp(low_mu, high_mu, prob_low) => prob_low / low_mu + (1.0 - prob_low) / high_mu,
            Expon(lambda) => 1.0 / lambda,
            Gamma(k, scale) => k * scale,
            Uniform(low, high) => (low + high) / 2.0,
            Constant(val) => *val,
            Dist::BLomax(alpha, lambda) => {
                let t = 1.0 + 1.0 / lambda;
                let r = t.powf(-alpha);
                let c = 1.0 - r;

                let term1 = alpha * t.powf(1.0 - alpha) / (1.0 - alpha);
                let term2 = t.powf(-alpha);
                let term3 = 1.0 / (1.0 - alpha);

                // E[X] = \lambda·( (term1 - term3) + term2 ) / c
                lambda * ((term1 - term3) + term2) / c
            }
            Dist::MUnif(v, p) => {
                // E[X] = \sum_i p[i] · E[U(v[i−1], v[i])]
                //      = \sum_i p[i] · (v[i−1] + v[i]) / 2
                let d = v.len();
                p.iter()
                    .enumerate()
                    .map(|(i, &pi)| {
                        let a = if i == 0 { 0.0 } else { v[i - 1] };
                        let b = v[i];
                        pi * (a + b) * 0.5
                    })
                    .sum()
            }
            Dist::Triangular(u) => u / 3.0,
            Dist::BExp(lambda) => {
                let r = (-lambda).exp();
                let c = 1.0 - r;
                (1.0 - (lambda + 1.0) * r) / (lambda * c)
            }
            Dist::TruncatedN(m, v) => {
                let sigma = v.sqrt();
                let stdn = StatNormal::new(0.0, 1.0).unwrap();

                let alpha = (0.0 - m) / sigma;
                let beta  = (1.0 - m) / sigma;

                let fa = stdn.cdf(alpha);
                let fb = stdn.cdf(beta);
                let z  = (fb - fa).max(1e-16);

                let phi = |t: f64| (-(t*t)/2.0).exp() / (std::f64::consts::TAU).sqrt();
                let mu_z = (phi(alpha) - phi(beta)) / z;
                m + sigma * mu_z
            }
            Dist::TwoUnif  (a1, b1, p1, a2, b2, p2 ) => {
                p1 * (a1 + b1) * 0.5 + p2 * (a2 + b2) * 0.5
            }
            Dist::TwoTruncatedN ( u1, v1, p1, u2, v2, p2 ) => {
                let m1 = Dist::TruncatedN(*u1, *v1).mean();
                let m2 = Dist::TruncatedN(*u2, *v2).mean();
                *p1 * m1 + *p2 * m2
            }
        }
    }

    fn meansquare(&self) -> f64 {
        // return the mean square of a given distribution // hy: Second moment
        use Dist::*;
        match self {
            Hyperexp(low_mu, high_mu, prob_low) => {
                (2.0 / (low_mu.powf(2.0)) * prob_low)
                    + (2.0 / (high_mu.powf(2.0)) * (1.0 - prob_low))
            }
            Expon(lambda) => 2.0 / lambda.powf(2.0),
            Gamma(k, scale) => ((k + 1.0) * k) / (1.0 / scale).powf(2.0),
            Uniform(low, high) => (1.0 / 3.0) * ((high.powf(3.0) - low.powf(3.0)) / (low - high)),
            Constant(val) => val.powi(2),
            Dist::BLomax(alpha, lambda) => {
                let t = 1.0 + 1.0 / lambda;
                let r = t.powf(-alpha);
                let c = 1.0 - r;

                let bu = t.powf(2.0 - alpha) / (2.0 - alpha)
                    - 2.0 * t.powf(1.0 - alpha) / (1.0 - alpha)
                    + t.powf(-alpha) / (-alpha);

                let bl = 1.0 / (2.0 - alpha) - 2.0 / (1.0 - alpha) - 1.0 / alpha;


                alpha * lambda * lambda * (bu - bl) / c
            }
            Dist::MUnif(v, p) => {

                let d = v.len();
                p.iter()
                    .enumerate()
                    .map(|(i, &pi)| {
                        let a = if i == 0 { 0.0 } else { v[i - 1] };
                        let b = v[i];
                        let num = b.powi(3) - a.powi(3);
                        let denom = 3.0 * (b - a);
                        pi * (num / denom)
                    })
                    .sum()
            }
            Dist::Triangular(u) => u * u / 6.0,
            Dist::BExp(lambda) => {
                let r = (-lambda).exp();
                let c = 1.0 - r;
                let num = 2.0 - (lambda * lambda + 2.0 * lambda + 2.0) * r;
                num / (lambda * lambda * c)
            }
            Dist::TruncatedN(m, v) => {
                let sigma = v.sqrt();
                let stdn = StatNormal::new(0.0, 1.0).unwrap();

                let alpha = (0.0 - m) / sigma;
                let beta  = (1.0 - m) / sigma;

                let fa = stdn.cdf(alpha);
                let fb = stdn.cdf(beta);
                let z  = (fb - fa).max(1e-16);

                let phi = |t: f64| (-(t*t)/2.0).exp() / (std::f64::consts::TAU).sqrt();
                let mu_z  = (phi(alpha) - phi(beta)) / z;
                let var_z = 1.0 + (alpha * phi(alpha) - beta * phi(beta)) / z - mu_z * mu_z;

                let ex   = m + sigma * mu_z;
                let varx = sigma * sigma * var_z;

                varx + ex * ex
            }
            Dist::TwoUnif ( a1, b1, p1, a2, b2, p2 ) => {
                let e2_1 = (b1.powi(3) - a1.powi(3)) / (3.0 * (b1 - a1));
                let e2_2 = (b2.powi(3) - a2.powi(3)) / (3.0 * (b2 - a2));
                p1 * e2_1 + p2 * e2_2
            }
            Dist::TwoTruncatedN ( u1, v1, p1, u2, v2, p2 ) => {
                let e2_1 = Dist::TruncatedN(*u1, *v1).meansquare();
                let e2_2 = Dist::TruncatedN(*u2, *v2).meansquare();
                *p1 * e2_1 + *p2 * e2_2
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Policy {
    // Baseline policies
    FCFS,                                 // First-Come First-Served
    PLCFS,                                // Preemptive Last-Come First-Served
    SRPT,                                 // Shortest Remaining Processing Time
    FCFSB,                                // First-Come First-Served, preemptive backfilling
    SRPTB,                     // Shortest Remaining Processing Time, preemptive backfilling
    PLCFSB,                    // Preemptive Last-Come First-Served
    LSF,                       // Preemptive Least Servers First
    LSFB,                      // Preemptive Least Servers First, preemptive backfilling
    MSF,                       // Preemptive Most Servers First
    MSFB,                      // Preemptive Most Servers First, preemptive backfilling
    SRA,                       // Smallest remaining area
    SRAB,                      // Smallest remaining area, preemptive backfilling
    LRA,                       // Largest remaining area
    LRAB,                      // Largest remaining area, preemptive backfilling
    DB(usize),                 // Double bucket, explicit K
    DBB(usize),                // Double bucket, explicit K, preemptive backfilling
    DBE,                       // Double bucket, K based on lambda
    DBEB,                      // Double bucket, K based on lambda, preemptive backfilling
    BPT(usize),                // Bucket powers of 2, explicit K
    BPTB(usize),               // BPT w/ backfilling
    AdaptiveDB(f64),
    AdaptiveDBB(f64),
    AdaptiveBPT(f64), // Double bucket, K based on queue length raised to 1/parameter
    AdaptiveBPTB(f64), // Double bucket, K based on queue length raised to 1/parameter
    AdaptiveIPB(f64), // Double bucket, K based on queue length raised to 1/parameter
    AdaptiveIPBB(f64), // Double bucket, K based on queue length raised to 1/parameter
    IPB(usize),       // Integer partitions buckets, explicit K
    IPBB(usize),      // Integer partitions buckets, explicit K, Backfilling
    BPTB_LSF(usize), // 2B KEMW LSF
}

impl Policy {
    // return whichever criterion jobs get sorted by.

    fn index(&self, job: &Job) -> f64 {
        match self {
            Policy::FCFS | Policy::FCFSB => job.arrival_time, //hy: based on the order of the arrival time
            Policy::PLCFS | Policy::PLCFSB => -job.arrival_time, //hy: based on the reverse order of the arrival time
            Policy::SRPT | Policy::SRPTB => job.rem_size, //hy: based on the remaining size of the job
            Policy::LSF | Policy::LSFB => job.service_req, //hy: based on the service requirement of the job
            Policy::MSF | Policy::MSFB => -job.service_req, //hy: based on the reverse service requirement of the job
            Policy::SRA | Policy::SRAB => job.rem_size * job.service_req, //hy: based on the remaining size of the job and the service requirement of the job
            Policy::LRA | Policy::LRAB => -job.rem_size * job.service_req, //hy: based on the remaining size of the job and the reverse service requirement of the job
            Policy::DB(_)
            | Policy::DBE
            | Policy::DBB(_)
            | Policy::DBEB
            | Policy::BPT(_)
            | Policy::BPTB(_)
            | Policy::AdaptiveDB(_)
            | Policy::AdaptiveDBB(_)
            | Policy::AdaptiveBPT(_)
            | Policy::AdaptiveBPTB(_)
            | Policy::AdaptiveIPB(_)
            | Policy::AdaptiveIPBB(_)
            | Policy::IPB(_)
            | Policy::IPBB(_) => job.arrival_time,
            Policy:: BPTB_LSF(_) => job.service_req, //hy:Borg ???
        }
    }
}

fn fcfstest(arr_lambda: f64, size_dist: &Dist) {
    // if FCFS is being used, can predict expected behavior of queueing system
    // used to ensure that simulation worked previously.

    let avg_size = size_dist.mean();

    // rho -- must be less than 1
    let rho = arr_lambda * avg_size;

    let esquare = size_dist.meansquare();

    // we have everything needed to find E[T] and E[N]
    let ET = (arr_lambda * esquare) / (2.0 * (1.0 - rho)) + avg_size;
    let EN = ET * arr_lambda;
    if DEBUG {
        println!("Mean Response time is: {}, Mean Queue Length is {}", ET, EN);
    }
}

fn qscan(vec: &Vec<Job>, num_servers: usize) -> usize {
    // only works for sorting-based policies.
    // iterate through a queue until the service requirement is maxed out.
    let mut index = 0;
    let total_resource = num_servers as f64;
    if DEBUG {
        println!("Total resource = {}", total_resource);
        println!("Queue length={}", vec.len());
    }
    // count how much "size" we have remaining in this timestep
    let mut taken_service: f64 = EPSILON;

    // very naive while loop
    while taken_service < total_resource {
        if index >= vec.len() {
            if DEBUG {
                println!("Max Length reached");
            }
            return vec.len();
        };
        taken_service = taken_service + vec[index].service_req;
        index = index + 1;
    }
    index - 1
}

fn take_to_vec(num_take: usize) -> Vec<usize> {
    // convert a usize to a collection of indices of jobs that can be worked on if qscan was used.
    let v: Vec<usize> = (0..num_take).collect();
    v
}

fn backfill(vec: &Vec<Job>, num_servers: usize) -> Vec<usize> {
    // backfilling that assumes you haven't chosen anything to work on yet.
    // works for sorting-based policies with backfilling
    let total_resource = num_servers as f64;
    if DEBUG {
        println!("Backfilling up to {}", total_resource);
    }

    // initialize the taken_resource counter, loop with a skip
    let mut taken_resource = 0.0;
    let mut indices: Vec<usize> = vec![];

    for ii in 0..vec.len() {
        let trial_take = taken_resource + vec[ii].service_req;
        if trial_take > total_resource {
            continue;
        }
        if trial_take + EPSILON <= total_resource {
            taken_resource = trial_take;
            indices.push(ii);
        }
    }
    indices
}

fn backfill_hogged(vec: &Vec<Job>, hogged: f64, hog_indices: Vec<usize>) -> Vec<usize> {
    // backfilling function that takes in a vector of indices that have already been chosen
    // by your policy. returns a vector of all indices that can be worked on after backfilling.
    let total_resource = 1.0;
    if DEBUG {
        println!("Backfilling up to {}", total_resource);
    }

    let mut taken_resource = hogged;
    let mut indices = hog_indices.clone();

    for ii in 0..vec.len() {
        if hog_indices.contains(&ii) {
            continue;
        }
        let trial_take = taken_resource + vec[ii].service_req;
        if trial_take > total_resource {
            continue;
        }
        if trial_take + EPSILON <= total_resource {
            taken_resource = trial_take;
            indices.push(ii);
        }
    }
    indices
}

// make the bucket giving a separate function for the IP policy to do its thing
fn assign_buckets(vec: &Vec<Job>, k: usize, upper: f64, lower: f64) -> Vec<usize> {
    //hy: In our setting, upper = 1.0, lower = 0.0
    // return a vector the same length as the queue,
    // containing the bucket label of the job at its corresponding index.
    let increment = (upper - lower) / (k as f64);
    if DEBUG {
        println!("Increment is {}, k is {}", increment, k);
    }
    let all_indices: Vec<usize> = (0..vec.len()).collect();

    let bucket_numbers: Vec<usize> = all_indices
        .iter()
        .map(|index| (vec[*index].service_req / increment).floor() as usize) //hy: maps the service requirement of the job to the bucket number 1,...,k
        .collect();
    bucket_numbers
}

fn eval_buckets(vec: &Vec<Job>, k: usize, upper: f64, lower: f64, backfill: bool) -> Vec<usize> {
    //Double bucket policy, forms and scores bucket pairs.
    // hy: This evaluation is only useful for our example of uniform((0,1]))
    //assert!(k % 2 == 1);

    let bucket_numbers: Vec<usize> = assign_buckets(vec, k, upper, lower);
    // evaluate bucket scores
    let mut bucket_counts: Vec<f64> = vec![0.0; k];
    for ii in 0..vec.len() {
        bucket_counts[bucket_numbers[ii]] += 1.0;
    }
    // square all bucket scores //HY:!!! why squ of 2
    let bucket_scores: Vec<f64> = bucket_counts.iter().map(|score| score.powf(1.0)).collect();

    // compare bucket scores and return the highest one
    let mut target = 0; // 0 corresponds to bucket pair 0,k-1
    let mut temp_new = 0.0;
    let mut sitting_best = 0.0;

    if  k % 2 == 1 {
        for jj in 0..((bucket_scores.len() - 1) / 2) {
            temp_new = bucket_scores[jj] + bucket_scores[k - jj - 2];

            if temp_new > sitting_best + EPSILON {
                sitting_best = temp_new;
                target = jj; // assign target var
            }
        }
    }
    else {
        for jj in 0..(bucket_scores.len()  / 2) {
            temp_new = bucket_scores[jj] + bucket_scores[k - jj - 2];

            if temp_new > sitting_best + EPSILON {
                sitting_best = temp_new;
                target = jj; // assign target var
            }
        }
    }


    // check the last bucket
    let mut last = false;
    if bucket_scores[k - 1] > sitting_best {
        target = k - 1;
        last = true;
    }

    if DEBUG {
        println!("Bucket scores: {:?}", bucket_scores);
        println!("Bucket numbers of jobs: {:?}", bucket_numbers);
        println!("Last bucket targeted?: {:?}", last);
    }
    let mut ret_indices: Vec<usize> = vec![];

    // fetch the indices of the jobs corresponding to the winning bucket
    if k % 2 == 0{
        if target == k / 2 - 1 {
            for kk in 0..vec.len() {
                if bucket_numbers[kk] == target {
                    ret_indices.push(kk);
                    for ll in kk + 1..vec.len() {
                        if bucket_numbers[ll] == target {
                            ret_indices.push(ll);
                            break;
                        }
                    }
                    break;
                }
            }
        } else {
            for kk in 0..vec.len() {
                if bucket_numbers[kk] == target {
                    ret_indices.push(kk);
                    break;
                }
            }
            if !last {
                for kk in 0..vec.len() {
                    if bucket_numbers[kk] == k - target - 2 {
                        ret_indices.push(kk);
                        break;
                    }
                }
            }
        }
    }
    else {
        for kk in 0..vec.len() {
            if bucket_numbers[kk] == target {
                ret_indices.push(kk);
                break;
            }
        }
        if !last {
            for kk in 0..vec.len() {
                if bucket_numbers[kk] == k - target - 2 {
                    ret_indices.push(kk);
                    break;
                }
            }
        }
    }

    // backfilling
    if backfill {
        let taken_resource: f64 = ret_indices
            .iter()
            .map(|index| vec[*index].service_req)
            .sum();
        ret_indices = backfill_hogged(vec, taken_resource, ret_indices);
    }

    if DEBUG {
        println!("Working on jobs {:?}", ret_indices);
    }

    ret_indices
}

fn get_d(c: usize) -> usize {
    // not needed
    // calculate the lowest power of two greater than c.
    if c == 1 {
        return 1;
    }
    let mut d: usize = 2;
    while d < c {
        d = 2 * d;
    }
    d
}

fn c_to_bucket_pair(c: usize) -> SmallVec<[usize; 2]> {
    // convert c (a number corresponding to a certain pair of buckets of powers of two)
    // eg. can return just [4], or [3,1] and other combinations that fit regardless of repetitions.

    let d = c.next_power_of_two(); // highest power of 2 //hy: 5.next_power_of_two() = 8
    assert!((d & (d - 1)) == 0);

    let mut bucket_set = smallvec![c];
    if d - c != 0 {
        bucket_set.push(d - c); //hy: e.g. c = 3, output = [3,1]; c = 9, output = [9,7]; c = 8, output = [8]
    }
    bucket_set
}

fn p2_buckets_exp(vec: &Vec<ExpJob>, k: usize, backfill: bool) -> Vec<usize> {
    // assigns buckets of powers of two to each job in a queue and returns indices corresponding to
    // the highest-scoring set.

    assert!((k & (k - 1)) == 0);

    // bucket 1 is the smallest bucket
    let bucket_numbers: Vec<usize> = assign_buckets_exp(vec, k, 1.0, 0.0)
        .iter()
        .map(|b| b + 1)
        .collect(); //hy: e.g., [3,6] mapped to [4,7] 

    // evaluate bucket set scores
    let mut set_scores: Vec<usize> = vec![0; k];

    let mut bucket_counts = vec![0; k];
    for &num in &bucket_numbers {
        bucket_counts[num - 1] += 1
    }
    /*
    for ii in 0..k {
        let c = ii + 1;
        // set 1 is the first set
        let bucket_set = c_to_bucket_pair(c);

        let reps: usize = k / c.next_power_of_two();

        // counts of buckets in bucket_set starting from bucket 1
        let q: Vec<usize> = bucket_set
            .iter()
            .map(|&c| bucket_numbers.iter().filter(|&num| *num == c).count())
            .collect();
        if DEBUG {
            println!("There are {:?} jobs of bucket {} in queue", q, c);
        }

        // calculate score of the iith set
        let mut score: usize = 0;
        for jj in 0..q.len() {
            score = score + q[jj].min(reps);
        }

        set_scores[ii] = score;
    }
    */
    for ii in 0..k {
        let c = ii + 1;
        // set 1 is the first set
        let bucket_set = c_to_bucket_pair(c);

        let reps: usize = k / c.next_power_of_two();
        // hy: how many job pairs [c, c.next_power_of_two() -c] I can serve together

        for jj in 0..bucket_set.len() {
            let num_in_bucket = bucket_counts[bucket_set[jj] - 1];
            let num_jobs_served = num_in_bucket.min(reps);
            set_scores[ii] += num_in_bucket * num_jobs_served;
        }
    }

    // now we know the bucket scores. find the highest scoring set, then return non-repeating
    // indices to corresponding jobs

    let (s_i, _big_score) = set_scores
        .iter()
        .enumerate()
        .max_by_key(|(_index, score)| *score)
        .expect("At least one set");
    /*
    let big_score = *set_scores.iter().max().unwrap();

    //get the index of the top_scoring set.
    let s_i = set_scores
        .iter()
        .position(|x| x == &big_score)
        .expect("Top score not found");
        */

    let target_c = s_i + 1;
    let target_reps = k / target_c.next_power_of_two();
    let target_buckets = c_to_bucket_pair(target_c);

    if DEBUG {
        println!(
            "searching for bucket set with c value {}, repeating {} times, bucket values {:?}",
            target_c, target_reps, target_buckets
        );
    }

    let mut found_indices: Vec<usize> = vec![];

    // go through and find indices of buckets that match
    /*
    for _rep in 0..target_reps {
        for kk in 0..bucket_numbers.len() {
            let current = &bucket_numbers[kk];
            if target_buckets.contains(&current) & !found_indices.contains(&kk) {
                found_indices.push(kk);

            }
        }
    }

    for _rep in 0..target_reps {
        for target_b in target_buckets {
            for kk in 0..bucket_numbers.len() {
                let current = bucket_numbers[kk];

            }
        }
    }
    */

    for target_b in target_buckets {
        let mut count = 0;
        for kk in 0..bucket_numbers.len() {
            let current = bucket_numbers[kk];
            if target_b == current {
                found_indices.push(kk);
                count += 1;
                if count == target_reps {
                    break;
                }
            }
        }
    }

    if backfill {
        let taken_resource: f64 = found_indices
            .iter()
            .map(|index| vec[*index].service_req)
            .sum();
        found_indices = backfill_hogged_exp(vec, taken_resource, found_indices);
    }
    // now we have indices of the buckets to work on
    // TODO: write a test for this

    found_indices
}
fn backfill_hogged_exp(vec: &Vec<ExpJob>, hogged: f64, hog_indices: Vec<usize>) -> Vec<usize> {
    // backfilling function that takes in a vector of indices that have already been chosen
    // by your policy. returns a vector of all indices that can be worked on after backfilling.
    let total_resource = 1.0;
    if DEBUG {
        println!("Backfilling up to {}", total_resource);
    }

    let mut taken_resource = hogged;
    let mut indices = hog_indices.clone();

    for ii in 0..vec.len() {
        if hog_indices.contains(&ii) {
            continue;
        }
        let trial_take = taken_resource + vec[ii].service_req;
        if trial_take > total_resource {
            continue;
        }
        if trial_take + EPSILON <= total_resource {
            taken_resource = trial_take;
            indices.push(ii);
        }
    }
    indices
}

fn p2_buckets(vec: &Vec<Job>, k: usize, backfill: bool) -> Vec<usize> {
    // assigns buckets of powers of two to each job in a queue and returns indices corresponding to
    // the highest-scoring set.

    assert!((k & (k - 1)) == 0);

    // bucket 1 is the smallest bucket
    let bucket_numbers: Vec<usize> = assign_buckets(vec, k, 1.0, 0.0)
        .iter()
        .map(|b| b + 1)
        .collect(); //hy: e.g., [3,6] mapped to [4,7]

    // evaluate bucket set scores
    let mut set_scores: Vec<usize> = vec![0; k];

    let mut bucket_counts = vec![0; k];
    for &num in &bucket_numbers {
        bucket_counts[num - 1] += 1
    }
    /*
    for ii in 0..k {
        let c = ii + 1;
        // set 1 is the first set
        let bucket_set = c_to_bucket_pair(c);

        let reps: usize = k / c.next_power_of_two();

        // counts of buckets in bucket_set starting from bucket 1
        let q: Vec<usize> = bucket_set
            .iter()
            .map(|&c| bucket_numbers.iter().filter(|&num| *num == c).count())
            .collect();
        if DEBUG {
            println!("There are {:?} jobs of bucket {} in queue", q, c);
        }

        // calculate score of the iith set
        let mut score: usize = 0;
        for jj in 0..q.len() {
            score = score + q[jj].min(reps);
        }

        set_scores[ii] = score;
    }
    */
    for ii in 0..k {
        let c = ii + 1;
        // set 1 is the first set
        let bucket_set = c_to_bucket_pair(c);

        let reps: usize = k / c.next_power_of_two();
        // hy: how many job pairs [c, c.next_power_of_two() -c] I can serve together

        for jj in 0..bucket_set.len() {
            let num_in_bucket = bucket_counts[bucket_set[jj] - 1];
            let num_jobs_served = num_in_bucket.min(reps);
            set_scores[ii] += num_in_bucket * num_jobs_served;
        }
    }

    // now we know the bucket scores. find the highest scoring set, then return non-repeating
    // indices to corresponding jobs

    let (s_i, _big_score) = set_scores
        .iter()
        .enumerate()
        .max_by_key(|(_index, score)| *score)
        .expect("At least one set");
    /*
    let big_score = *set_scores.iter().max().unwrap();

    //get the index of the top_scoring set.
    let s_i = set_scores
        .iter()
        .position(|x| x == &big_score)
        .expect("Top score not found");
        */

    let target_c = s_i + 1;
    let target_reps = k / target_c.next_power_of_two();
    let target_buckets = c_to_bucket_pair(target_c);

    if DEBUG {
        println!(
            "searching for bucket set with c value {}, repeating {} times, bucket values {:?}",
            target_c, target_reps, target_buckets
        );
    }

    let mut found_indices: Vec<usize> = vec![];

    // go through and find indices of buckets that match
    /*
    for _rep in 0..target_reps {
        for kk in 0..bucket_numbers.len() {
            let current = &bucket_numbers[kk];
            if target_buckets.contains(&current) & !found_indices.contains(&kk) {
                found_indices.push(kk);

            }
        }
    }

    for _rep in 0..target_reps {
        for target_b in target_buckets {
            for kk in 0..bucket_numbers.len() {
                let current = bucket_numbers[kk];

            }
        }
    }
    */

    for target_b in target_buckets {
        let mut count = 0;
        for kk in 0..bucket_numbers.len() {
            let current = bucket_numbers[kk];
            if target_b == current {
                found_indices.push(kk);
                count += 1;
                if count == target_reps {
                    break;
                }
            }
        }
    }

    if backfill {
        let taken_resource: f64 = found_indices
            .iter()
            .map(|index| vec[*index].service_req)
            .sum();
        found_indices = backfill_hogged(vec, taken_resource, found_indices);
    }
    // now we have indices of the buckets to work on
    // TODO: write a test for this

    found_indices
}

// make the bucket giving a separate function for the IP policy to do its thing
fn assign_buckets_exp(vec: &Vec<ExpJob>, k: usize, upper: f64, lower: f64) -> Vec<usize> {
    //hy: In our setting, upper = 1.0, lower = 0.0
    // return a vector the same length as the queue,
    // containing the bucket label of the job at its corresponding index.
    let increment = (upper - lower) / (k as f64);
    if DEBUG {
        println!("Increment is {}, k is {}", increment, k);
    }
    let all_indices: Vec<usize> = (0..vec.len()).collect();

    let bucket_numbers: Vec<usize> = all_indices
        .iter()
        .map(|index| (vec[*index].service_req / increment).floor() as usize) //hy: maps the service requirement of the job to the bucket number 1,...,k
        .collect();
    bucket_numbers
}


#[derive(Debug, Clone)]
struct score_ip {
    partition: Vec<usize>,
    score: usize,
}

#[derive(Debug, Clone)]
struct scored_vec_mult {
    vect: Vec<usize>,
    multiplicities: Vec<usize>,
    score: usize,
}

fn k_to_partitions_mults(k: usize) -> Vec<scored_vec_mult> {
    // takes a k value and returns a vector of integer partitions with multiplicities of each
    // number listed and no duplicates (hopefully)

    let mut ipar = Partitions::new(k); //hy: for k = 4, [[4], [3,1], [2,2], [2,1,1], [1,1,1,1]]
    let mut to_remove = HashSet::new();
    if k % 2 == 0 {
        let mut half_ipar = Partitions::new(k/2);
        let mut half_ipar_vec: Vec<Vec<usize>> = vec![];
        while let Some(part) = half_ipar.next() {
            half_ipar_vec.push(part.to_vec());
        }
        for i in 0..half_ipar_vec.len() {
            for j in i+1..half_ipar_vec.len() {
                let mut new = vec![];
                new.extend(&half_ipar_vec[i]);
                new.extend(&half_ipar_vec[j]);
                new.sort();
                to_remove.insert(new);
            }
        }
    }
    println!("to_remove: {}", to_remove.len());
    let mut partition_vector: Vec<scored_vec_mult> = vec![];

    while let Some(part) = ipar.next() {
        let current_partition: Vec<usize> = part.to_vec();
        if to_remove.contains(&current_partition) {
            continue
        }
        let mut no_duplicates: Vec<usize> = vec![];
        let mut mults: Vec<usize> = vec![];
        // hy: current_partition might be, e.g., [2, 1, 1].
        // hy: no_duplicates: every distinct summand in the order we first see it (e.g. [2, 1]),
        // hy: mults: the corresponding counts ([1, 2] here).
        for ii in 0..current_partition.len() {
            let num = current_partition[ii];
            if no_duplicates.contains(&num) {
                continue;
            } else {
                no_duplicates.push(num); // hy: count how many times `num` occurs in the full partition
                let multiplicity = current_partition.iter().filter(|&n| n == &num).count();
                mults.push(multiplicity)
            }
        }
        let current_set = scored_vec_mult {
            vect: no_duplicates,
            multiplicities: mults,
            score: 0, //Heyuan: why is the score 0? Ah because it needs the info. of the bucket (# jobs)
        };
        partition_vector.push(current_set);
    }
    println!("{}", partition_vector.len());
    assert!(false);
    partition_vector
}

fn vec_mult_to_work(
    job_vec: &Vec<Job>,
    k: usize,
    sets: &Vec<scored_vec_mult>,
    backfill: bool,
) -> Vec<usize> {
    //smallest bucket is 1
    let bucket_numbers: Vec<usize> = assign_buckets(&job_vec, k, 1.0, 0.0)
        .iter()
        .map(|b| b + 1)
        .collect();
    if DEBUG {
        println!("Bucket numbers: {:?}", bucket_numbers)
    }
    let mut score_vec: Vec<scored_vec_mult> = sets.clone(); // hy: copy a new vector identical to the original one  

    // bucket_counts is the quantity of jobs in each bucket

    let mut bucket_counts = vec![0; k];
    for &num in &bucket_numbers {
        bucket_counts[num - 1] += 1
    }

    for qq in 0..score_vec.len() {
        //hy: set is some policy, e.g., vect = [1,3], mults = [5,1] means solving five 1-sized jobs and one 3-sized job together
        let set = score_vec[qq].clone();
        let current_partition: Vec<usize> = set.vect; //Heyuan: it is no_duplicates`
        let mul_vec = set.multiplicities;
        let mut current_score: usize = 0;
        for ii in 0..current_partition.len() {
            let multiplicity = mul_vec[ii];
            let count = bucket_counts[current_partition[ii] - 1];
            let num_jobs_served = multiplicity.min(count);
            current_score += count * num_jobs_served;
            // hy: choose min{# jobs in this bucket, # jobs we plan to solve in this bucket } as the "score"
            // Heyuan: By summing them up we obtain <q, M>, where this M is multiplicity.min(count), and q is count
            // Q1: So should the code be current_score += multiplicity.min(count)*count
        }
        score_vec[qq].score = current_score;
    }

    let score_vec_for_eval = score_vec.clone();

    let top_scorer: scored_vec_mult = score_vec_for_eval
        .iter()
        .max_by_key(|p: &&scored_vec_mult| p.score)
        .unwrap()
        .clone();
    let target_buckets = top_scorer.vect;
    let target_reps = top_scorer.multiplicities;
    assert!(target_buckets.len() == target_reps.len());

    if DEBUG {
        println!(
            "Chosen partition: {:?} with multiplicities {:?}",
            target_buckets, target_reps
        );
    }
    let mut found_indices: Vec<usize> = vec![];

    for jj in 0..target_buckets.len() {
        let bucket_num = target_buckets[jj];
        let multiplicity = target_reps[jj];
        let mut count = 0;
        for kk in 0..bucket_numbers.len() {
            let current = bucket_numbers[kk];
            if bucket_num == current {
                found_indices.push(kk);
                count += 1;
                if count == multiplicity {
                    break;
                }
            }
        }
    }

    if backfill {
        let taken_resource: f64 = found_indices
            .iter()
            .map(|index| job_vec[*index].service_req)
            .sum();
        found_indices = backfill_hogged(job_vec, taken_resource, found_indices);
    }


    found_indices
}

fn ipar_buckets(vec: &Vec<Job>, k: usize, backfill: bool) -> Vec<usize> {
    // assigns buckets of powers of two to each job in a queue and returns indices corresponding to
    // the highest-scoring set.

    // smallest bucket is 1
    let bucket_numbers: Vec<usize> = assign_buckets(vec, k, 1.0, 0.0)
        .iter()
        .map(|b| b + 1)
        .collect();

    if DEBUG {
        println!("Bucket numbers: {:?}", bucket_numbers)
    }

    // evaluate bucket set scores
    let mut ip_scores: Vec<score_ip> = vec![];

    // bucket_counts is the quantity of jobs in each bucket

    let mut bucket_counts = vec![0; k];
    for &num in &bucket_numbers {
        bucket_counts[num - 1] += 1
    }
    // get integer partitions are score them

    let mut ipar = Partitions::new(k);

    while let Some(part) = ipar.next() {
        // get the current partition
        let current_partition: Vec<usize> = part.to_vec();
        let mut current_score: usize = 0;
        // iterate over the partition and match the counts to bucket_counts, then score
        let mut seen = vec![];
        for bucket_num in &current_partition {
            // only add score for new numbers because of the multiplicity calculation
            // (sorry)
            if seen.contains(&bucket_num) {
                continue;
            } else {
                seen.push(bucket_num);
            }
            // get multiplicity of each bucket number in the integer partition
            let multiplicity = current_partition
                .iter()
                .filter(|&num| num == bucket_num)
                .count();

            let count = bucket_counts[bucket_num - 1]; // number of bucket_num buckets we have
            current_score += multiplicity.min(count);
        }
        let pair = score_ip {
            partition: current_partition,
            score: current_score,
        };
        ip_scores.push(pair);
    }

    let top_scorer: score_ip = ip_scores
        .iter()
        .max_by_key(|p: &&score_ip| p.score)
        .unwrap()
        .clone();

    let target_buckets = top_scorer.partition;
    if DEBUG {
        println!("Chosen partition: {:?}", target_buckets);
    }
    let mut found_indices: Vec<usize> = vec![];

    // thisll be less efficient than the powers of two one

    /*
    for ii in 0..bucket_numbers.len() {
        for jj in found_count..target_buckets.len() {
           if target_buckets[jj] == bucket_numbers[ii] {
               found_indices.push(ii);
               found_count += 1;
               break;
           }
           else {
               continue;
           }
        }
    }
    */

    let mut seen = vec![];

    for bucket_num in &target_buckets {
        if seen.contains(&bucket_num) {
            continue;
        } else {
            seen.push(bucket_num);
        }
        // get multiplicity of each bucket number in the integer partition
        let multiplicity = target_buckets
            .iter()
            .filter(|&num| num == bucket_num)
            .count();
        let mut count = 0;
        for kk in 0..bucket_numbers.len() {
            let current = bucket_numbers[kk];
            if *bucket_num == current {
                found_indices.push(kk);
                count += 1;
                if count == multiplicity {
                    break;
                }
            }
        }
    }

    // TODO: add backfilling:

    assert!(found_indices.len() <= target_buckets.len());
    found_indices
}

fn lambda_to_k(lambda: f64) -> usize {
    // convert lambda to k assuming epsilon = (2-lambda)/(2*lambda)
    // hy: This is for uniform((0,1]) case
    let k_mid = (lambda + 2.0) / (2.0 - lambda);
    let mut attempt_k = k_mid.ceil() as usize;
    if attempt_k % 2 == 0 {
        attempt_k = attempt_k + 1
    }
    attempt_k as usize
}

fn length_to_k(p: f64, length: usize) -> usize {
    // get a workable k value by square rooting the number of jobs in the queue.
    // hy: This is some possible way for adaptive MW
    assert!(p > 0.0, "p must be nonzero");
    if length == 0 {
        2
    } else {
        let root = (length as f64).powf(1.0 / p);
        // Round up to the nearest integer
        let n = root.ceil() as usize;
        // Next power of two \geq n
        if n.next_power_of_two() <2 {
            2
        }
        else {
            n.next_power_of_two()
        }
    }
}

fn queue_indices(vec: &Vec<Job>, num_servers: usize, policy: Policy, lambda: f64, cache: &mut Option<Vec<scored_vec_mult>>) -> Vec<usize> {
    // use various policies to get a vector of indices of jobs in the queue that can be worked on.
    let l_lim = 0.0;
    let u_lim = num_servers as f64;
    match policy {
        Policy::FCFS => take_to_vec(qscan(vec, num_servers)),
        Policy::PLCFS => take_to_vec(qscan(vec, num_servers)),
        Policy::SRPT => take_to_vec(qscan(vec, num_servers)),
        Policy::FCFSB => backfill(vec, num_servers),
        Policy::SRPTB => backfill(vec, num_servers),
        Policy::PLCFSB => backfill(vec, num_servers),
        Policy::LSF => take_to_vec(qscan(vec, num_servers)),
        Policy::MSF => take_to_vec(qscan(vec, num_servers)),
        Policy::LSFB => backfill(vec, num_servers),
        Policy::MSFB => backfill(vec, num_servers),
        Policy::SRA => take_to_vec(qscan(vec, num_servers)),
        Policy::LRA => take_to_vec(qscan(vec, num_servers)),
        Policy::SRAB => backfill(vec, num_servers),
        Policy::LRAB => backfill(vec, num_servers),
        Policy::DB(k) => eval_buckets(vec, k, u_lim, l_lim, false),
        Policy::DBB(k) => eval_buckets(vec, k, u_lim, l_lim, true),
        Policy::DBE => eval_buckets(vec, lambda_to_k(lambda), u_lim, l_lim, false),
        Policy::DBEB => eval_buckets(vec, lambda_to_k(lambda), u_lim, l_lim, true),
        Policy::BPT(k) => p2_buckets(vec, k, false),
        Policy::BPTB(k) => p2_buckets(vec, k, true),
        Policy::BPTB_LSF(k) => p2_buckets(vec, k, true), // hy:Borg ??????
        Policy::AdaptiveDB(pow) => {
            eval_buckets(vec, length_to_k(pow, vec.len()), u_lim, l_lim,false)
        }
        Policy::AdaptiveDBB(pow) => {
            eval_buckets(vec, length_to_k(pow, vec.len()), u_lim, l_lim,true)
        }
        Policy::AdaptiveBPT(pow) => {
            p2_buckets(vec, length_to_k(pow, vec.len()), false)
        }
        Policy::AdaptiveBPTB(pow) => {
            p2_buckets(vec, length_to_k(pow, vec.len()), true)
        }
        Policy::IPB(k) => {
            if cache.is_none() {
                *cache = Some(k_to_partitions_mults(k));
            }

            //let set_mul_vec = k_to_partitions_mults(k);
            vec_mult_to_work(vec, k, cache.as_ref().expect("Cache is filled"), false)
        }
        Policy::IPBB(k) => {
            let set_mul_vec = k_to_partitions_mults(k);
            vec_mult_to_work(vec, k, &set_mul_vec, true)
        }
        Policy::AdaptiveIPB(p) => vec_mult_to_work(
            vec,
            length_to_k(p, vec.len()),
            &k_to_partitions_mults(length_to_k(p, vec.len())),
            false,
        ),
        Policy::AdaptiveIPBB(p) => vec_mult_to_work(
            vec,
            length_to_k(p, vec.len()),
            &k_to_partitions_mults(length_to_k(p, vec.len())),
            true,
        ),
    }
}


struct SimResult {
    mean_response: f64,
    num_arrivals: u64,
    num_completions: u64,
    overflow: bool,   // HY: if queue length threshold exceeded
}

struct SimResult_Borg {
    mean_response: f64,
    num_arrivals: u64,
    num_completions: u64,
    overflow: bool,   // HY: if queue length threshold exceeded
    weight_mean_response: f64,
}





fn simulateInLoop(
    // main simulation loop.
    policy: Policy,
    num_servers: usize,
    num_jobs: u64,
    dist: Dist,
    arr_lambda: f64,
    seed: u64,
    req_dist: Dist,
) -> SimResult {
    let mut num_completions = 0;
    let mut queue: Vec<Job> = vec![];
    let mut total_response = 0.0;
    // let mut weight_total_response = 0.0;
    let mut time = 0.0;
    let mut rng = StdRng::seed_from_u64(seed);
    let arrival_dist = Exp::new(arr_lambda).unwrap();
    let mut total_work = 0.0;
    let mut num_arrivals = 0;
    let mut cache = None;

    // predict what outcome should be (if fcfs):
    if DEBUG {
        fcfstest(arr_lambda, &dist);
    }

    // initialize a first job arrival
    let mut next_arrival_time = arrival_dist.sample(&mut rng);

    while num_completions < num_jobs {
        queue.sort_by_key(|job| n64(policy.index(job)));
        // if queue.len() > num_jobs.isqrt() as usize {
        if queue.len() > 10000 { // simple case
        // if queue.len() > 100000 { // Borg case
            // HY: let the Borg case be 100000
            // izzy: At this point, it is indistinguishable
            // from an unstable fair random walk.
            // If you still think it's a stable,
            // A longer simulation is needed to demonstrate.
            println!("Error: queue length past threshold");
            SimResult {
                mean_response: 0.0,
                num_arrivals,
                num_completions,
                overflow: true,// weight_mean_response: 0.0,
            };
            break;
            return SimResult {
                mean_response: 0.0,
                num_arrivals,
                num_completions,
                overflow: true,// weight_mean_response: 0.0,
            };


        }
        if DEBUG {
            println!(
                "Time is {}: | Queue: {:?} | Current work: {} Total work: {}",
                time,
                queue,
                queue.iter().map(|job| job.rem_size).sum::<f64>(),
                total_work,
            );
            std::io::stdin()
                .read_line(&mut String::new())
                .expect("whatever");
            // find next event (arrival or completion)
            // next_completion is NOT a time, it is a duration
        }

        // determine how many jobs need to get worked on in the sorted queue.
        //let num_workable = qscan(&queue, num_servers);
        //
        let mut index_workable = queue_indices(&queue, num_servers, policy, arr_lambda, &mut cache);
        index_workable.sort();

        if DEBUG {
            println!("Indices of jobs chosen for work: {:?}", index_workable);
        }

        let capacity: f64 = index_workable
            .iter()
            .map(|index| queue[*index].service_req)
            .sum();
        assert!(capacity < 1.0 + EPSILON);

        let next_completion = index_workable
            .iter()
            .map(|index| queue[*index].rem_size)
            .min_by_key(|f| n64(*f))
            .unwrap_or(INFINITY);

        //find next completion time out of eligible jobs
        /*
        let next_completion = queue
            .iter()
            .take(num_workable)
            .map(|job| job.rem_size as f64)
            .min_by_key(|f| n64(*f))
            .unwrap_or(INFINITY);
        */
        let timestep = next_completion.min(next_arrival_time - time);
        let was_arrival = timestep < next_completion;

        // time moves forward
        time += timestep;

        // all jobs currently in service get worked on
        /*
        queue
            .iter_mut()
            .take(num_workable) // or just 1 for now
            .for_each(|job| job.rem_size -= timestep as f64);
        */

        index_workable
            .iter()
            .for_each(|index| queue[*index].rem_size -= timestep);

        for &index in index_workable.iter().rev() {
            assert!(index < queue.len());
            if queue[index].rem_size < EPSILON {
                let job = queue.remove(index);
                total_response += time - job.arrival_time;
                // weight_total_response += (time - job.arrival_time)*job.service_req; //hy:Borg Delete when not Borg
                num_completions += 1;
            }
        }
        /*
        for i in (0.. .min(queue.len())).rev() {
            if queue[i].rem_size < EPSILON {
                let job = queue.remove(i);
                total_response += time - job.arrival_time;
                num_completions += 1;
            }
        }
        */
        // if the job was an arrival, tick up the total work in the queue (sum of rem_sizes)
        // and add a new job to the queue.

        if was_arrival {
            total_work += queue.iter().map(|job| job.rem_size).sum::<f64>();
            num_arrivals += 1;
            let new_job_size = dist.sample(&mut rng);
            let new_service_req = req_dist.sample(&mut rng);
            let new_job = Job {
                rem_size: new_job_size,
                original_size: new_job_size,
                arrival_time: time,
                service_req: new_service_req,
            };
            queue.push(new_job);
            next_arrival_time = time + arrival_dist.sample(&mut rng);
        }
    }

    // report mean queue load
    //total_work / num_arrivals as f64
    //OR report mean response time
    // total_response / num_arrivals as f64
    SimResult {
        mean_response: total_response / num_arrivals.max(1) as f64,
        num_arrivals,
        num_completions,
        overflow: false,
        // weight_mean_response: weight_total_response as f64,
    }
}



fn simulate(
    // main simulation loop.
    policy: Policy,
    num_servers: usize,
    num_jobs: u64,
    dist: Dist,
    arr_lambda: f64,
    seed: u64,
    req_dist: Dist,
) -> f64 {
    let mut num_completions = 0;
    let mut queue: Vec<Job> = vec![];
    let mut total_response = 0.0;
    let mut time = 0.0;
    let mut rng = StdRng::seed_from_u64(seed);
    let arrival_dist = Exp::new(arr_lambda).unwrap();
    let mut total_work = 0.0;
    let mut num_arrivals = 0;
    let mut cache = None;

    // predict what outcome should be (if fcfs):
    if DEBUG {
        fcfstest(arr_lambda, &dist);
    }

    // initialize a first job arrival
    let mut next_arrival_time = arrival_dist.sample(&mut rng);

    while num_completions < num_jobs {
        queue.sort_by_key(|job| n64(policy.index(job)));
        // if queue.len() > num_jobs.isqrt() as usize {
        // if queue.len() > 10000 { // simple case
        if queue.len() > 10000 { // Borg case
            // HY: let the Borg case be 100000
            // izzy: At this point, it is indistinguishable
            // from an unstable fair random walk.
            // If you still think it's a stable,
            // a longer simulation is needed to demonstrate.
            println!("Error: queue length past threshold");
            break;
        }
        if DEBUG {
            println!(
                "Time is {}: | Queue: {:?} | Current work: {} Total work: {}",
                time,
                queue,
                queue.iter().map(|job| job.rem_size).sum::<f64>(),
                total_work,
            );
            std::io::stdin()
                .read_line(&mut String::new())
                .expect("whatever");
            // find next event (arrival or completion)
            // next_completion is NOT a time, it is a duration
        }

        // determine how many jobs need to get worked on in the sorted queue.
        // let num_workable = qscan(&queue, num_servers);
        //
        let mut index_workable = queue_indices(&queue, num_servers, policy, arr_lambda, &mut cache);
        index_workable.sort();

        if DEBUG {
            println!("Indices of jobs chosen for work: {:?}", index_workable);
        }

        let capacity: f64 = index_workable
            .iter()
            .map(|index| queue[*index].service_req)
            .sum();
        assert!(capacity < 1.0 + EPSILON);

        let next_completion = index_workable
            .iter()
            .map(|index| queue[*index].rem_size)
            .min_by_key(|f| n64(*f))
            .unwrap_or(INFINITY);

        //find next completion time out of eligible jobs
        /*
        let next_completion = queue
            .iter()
            .take(num_workable)
            .map(|job| job.rem_size as f64)
            .min_by_key(|f| n64(*f))
            .unwrap_or(INFINITY);
        */
        let timestep = next_completion.min(next_arrival_time - time);
        let was_arrival = timestep < next_completion;

        // time moves forward
        time += timestep;

        // all jobs currently in service get worked on
        /*
        queue
            .iter_mut()
            .take(num_workable) // or just 1 for now
            .for_each(|job| job.rem_size -= timestep as f64);
        */

        index_workable
            .iter()
            .for_each(|index| queue[*index].rem_size -= timestep);

        for &index in index_workable.iter().rev() {
            assert!(index < queue.len());
            if queue[index].rem_size < EPSILON {
                let job = queue.remove(index);
                total_response += time - job.arrival_time;
                num_completions += 1;
            }
        }
        /*
        for i in (0.. .min(queue.len())).rev() {
            if queue[i].rem_size < EPSILON {
                let job = queue.remove(i);
                total_response += time - job.arrival_time;
                num_completions += 1;
            }
        }
        */
        // if the job was an arrival, tick up the total work in the queue (sum of rem_sizes)
        // and add a new job to the queue.

        if was_arrival {
            total_work += queue.iter().map(|job| job.rem_size).sum::<f64>();
            num_arrivals += 1;
            let new_job_size = dist.sample(&mut rng);
            let new_service_req = req_dist.sample(&mut rng);
            let new_job = Job {
                rem_size: new_job_size,
                original_size: new_job_size,
                arrival_time: time,
                service_req: new_service_req,
            };
            queue.push(new_job);
            next_arrival_time = time + arrival_dist.sample(&mut rng);
        }
    }

    // report mean queue load
    //total_work / num_arrivals as f64
    //OR report mean response time
    total_response / num_arrivals as f64
}

fn simulate_traces(
    policy: Policy,
    num_servers: usize,
    dist: Dist,
    arr_lambda: f64,
    seed: u64,
    req_trace: &Vec<f64>, // HY: some array we can read from traces of, for example, Google Borg memory requirement
) -> f64 {
    let num_jobs: u64 = req_trace.len() as u64;

    let mut num_completions = 0;
    let mut queue: Vec<Job> = vec![];
    let mut total_response = 0.0;
    let mut time = 0.0;
    let mut rng = StdRng::seed_from_u64(seed);
    let arrival_dist = Exp::new(arr_lambda).unwrap();
    let mut total_work = 0.0;
    let mut num_arrivals: u64 = 0;
    let mut cache = None;

    if DEBUG {
        fcfstest(arr_lambda, &dist);
    }

    let mut next_arrival_time = arrival_dist.sample(&mut rng);

    while num_completions < num_jobs {
        queue.sort_by_key(|job| n64(policy.index(job)));

        // if queue.len() > num_jobs.isqrt() as usize {
        if queue.len() > 10000 { // simple case
        // if queue.len() > 100000 { // hy:Borg case use 100K
            println!("Error: queue length past threshold");
            break;
        }

        let mut index_workable =
            queue_indices(&queue, num_servers, policy, arr_lambda, &mut cache);
        index_workable.sort();

        let capacity: f64 = index_workable
            .iter()
            .map(|index| queue[*index].service_req)
            .sum();
        assert!(capacity < 1.0 + EPSILON);

        let next_completion = index_workable
            .iter()
            .map(|index| queue[*index].rem_size)
            .min_by_key(|f| n64(*f))
            .unwrap_or(INFINITY);

        let timestep = next_completion.min(next_arrival_time - time);
        let was_arrival = timestep < next_completion;

        time += timestep;

        index_workable
            .iter()
            .for_each(|index| queue[*index].rem_size -= timestep);

        for &index in index_workable.iter().rev() {
            if queue[index].rem_size < EPSILON {
                let job = queue.remove(index);
                total_response += time - job.arrival_time;
                num_completions += 1;
            }
        }

        if was_arrival {
            if (num_arrivals as usize) >= req_trace.len() {
                break;
            }

            total_work += queue.iter().map(|job| job.rem_size).sum::<f64>();

            let new_job_size = dist.sample(&mut rng);
            let new_service_req = req_trace[num_arrivals as usize];

            let new_job = Job {
                rem_size: new_job_size,
                original_size: new_job_size,
                arrival_time: time,
                service_req: new_service_req,
            };

            queue.push(new_job);
            num_arrivals += 1;
            next_arrival_time = time + arrival_dist.sample(&mut rng);
        }
    }

    total_response / num_arrivals.max(1) as f64
}

fn simulateInLoop_traces(
    // main simulation loop.
    policy: Policy,
    num_servers: usize,
    dist: Dist,
    arr_lambda: f64,
    seed: u64,
    req_trace: &Vec<f64>,
) -> SimResult {
    let num_jobs: u64 = req_trace.len() as u64;

    let mut num_completions = 0;
    let mut queue: Vec<Job> = vec![];
    let mut total_response = 0.0;
    let mut time = 0.0;
    let mut rng = StdRng::seed_from_u64(seed);
    let arrival_dist = Exp::new(arr_lambda).unwrap();
    let mut total_work = 0.0;
    let mut num_arrivals: u64 = 0;
    let mut cache = None;

    // predict what outcome should be (if fcfs):
    if DEBUG {
        fcfstest(arr_lambda, &dist);
    }

    // initialize a first job arrival
    let mut next_arrival_time = arrival_dist.sample(&mut rng);

    while num_completions < num_jobs {
        queue.sort_by_key(|job| n64(policy.index(job)));
        // if queue.len() > num_jobs.isqrt() as usize {
        if queue.len() > 100000 { // Borg case
            // HY: let the Borg case be 100000
            // izzy: At this point, it is indistinguishable
            // from an unstable fair random walk.
            // If you still think it's a stable,
            //A longer simulation is needed to demonstrate.
            return SimResult {
                mean_response: 0.0,
                num_arrivals,
                num_completions,
                overflow: true,
            };

            // println!("Error: queue length past threshold");
            // break;
        }

        // determine how many jobs need to get worked on in the sorted queue.
        //let num_workable = qscan(&queue, num_servers);
        //
        let mut index_workable =
            queue_indices(&queue, num_servers, policy, arr_lambda, &mut cache);
        index_workable.sort();

        let capacity: f64 = index_workable
            .iter()
            .map(|index| queue[*index].service_req)
            .sum();
        assert!(capacity < 1.0 + EPSILON);

        let next_completion = index_workable
            .iter()
            .map(|index| queue[*index].rem_size)
            .min_by_key(|f| n64(*f))
            .unwrap_or(INFINITY);

        let timestep = next_completion.min(next_arrival_time - time);
        let was_arrival = timestep < next_completion;

        time += timestep;

        index_workable
            .iter()
            .for_each(|index| queue[*index].rem_size -= timestep);

        for &index in index_workable.iter().rev() {
            if queue[index].rem_size < EPSILON {
                let job = queue.remove(index);
                total_response += time - job.arrival_time;
                num_completions += 1;
            }
        }

        if was_arrival {
            if (num_arrivals as usize) >= req_trace.len() {
                break;
            }

            total_work += queue.iter().map(|job| job.rem_size).sum::<f64>();

            let new_job_size = dist.sample(&mut rng);
            let new_service_req = req_trace[num_arrivals as usize];

            let new_job = Job {
                rem_size: new_job_size,
                original_size: new_job_size,
                arrival_time: time,
                service_req: new_service_req,
            };

            queue.push(new_job);
            num_arrivals += 1;
            next_arrival_time = time + arrival_dist.sample(&mut rng);
        }
    }

    // report mean queue load
    //total_work / num_arrivals as f64
    //OR report mean response time
    // total_response / num_arrivals as f64
    SimResult {
        mean_response: total_response / num_arrivals.max(1) as f64,
        num_arrivals,
        num_completions,
        overflow: false,
    }
}

fn simulateInLoop_traces_revised(
    // main simulation loop.
    num_servers: usize,
    arr_lambda: f64,
    seed: u64,
    req_trace: &Vec<f64>,
) -> SimResult {
    let num_jobs: u64 = req_trace.len() as u64;

    let mut num_completions = 0;
    let mut queue: Vec<ExpJob> = vec![];
    //let mut total_response = 0.0;
    let mut total_number = 0;
    //let mut time = 0.0;
    let mut rng = StdRng::seed_from_u64(seed);
    //let arrival_dist = Exp::new(arr_lambda).unwrap();
    //let mut total_work = 0.0;
    let mut num_arrivals: u64 = 0;
    //let mut cache = None;

    while num_completions < num_jobs {
        if queue.len() > 100000 { // Borg case
            // HY: let the Borg case be 100000
            return SimResult {
                mean_response: 0.0,
                num_arrivals,
                num_completions,
                overflow: true,
            };
        }

        let mut index_workable =
            p2_buckets_exp(&queue, 1024, true);
            // p2_buckets_exp(&queue, num_servers, true); // HY: Hi Izzy, Here num_servers = 1 but we need 1024
        //FCFS    take_to_vec(qscan(&queue, num_servers));

        let capacity: f64 = index_workable
            .iter()
            .map(|index| queue[*index].service_req)
            .sum();
        assert!(capacity < 1.0 + EPSILON);

        let event = rng.gen_range(0.0 .. arr_lambda + index_workable.len() as f64);

        let was_arrival = event < arr_lambda;

        if !was_arrival {
            let completion = (event - arr_lambda) as usize; // HY: randomly pick one as the served job
            assert!(completion < index_workable.len());
            let job = (&mut queue).remove(index_workable[completion]);
            num_completions += 1;
        }

        if was_arrival {
            if (num_arrivals as usize) >= req_trace.len() {
                break;
            }
            total_number += queue.len();

            let new_service_req = req_trace[num_arrivals as usize];

            let new_job = ExpJob {
                service_req: new_service_req,
            };

            queue.push(new_job);
            num_arrivals += 1;
        }
    }

    // report mean queue load
    //total_work / num_arrivals as f64
    //OR report mean response time
    // total_response / num_arrivals as f64
    SimResult {
        mean_response: (total_number as f64 / num_arrivals.max(1) as f64)/ arr_lambda,
        num_arrivals,
        num_completions,
        overflow: false,
    }
}
