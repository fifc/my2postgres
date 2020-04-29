extern crate yaml_rust;
extern crate rand;

use rand::prelude::*;
use std::io::{BufRead, Write};
use std::collections::HashMap;
//use term::Attr::BackgroundColor;

pub const BASE_ID:u64 = 1000000;
const TOTAL_NUM:u64 = 184779;

pub fn init_pool() -> (Vec<u64>, u64) {
	let mut max = TOTAL_NUM;
	let res = std::fs::File::open("pool.dat");
	match res {
		Ok(file) => {
			println!("using existing pool ...");
			let mut pool = Vec::<u64>::new();
			let mut map = HashMap::new();
			let reader = std::io::BufReader::new(file);
			for (i, line) in reader.lines().enumerate() {
				let line = line.unwrap();
				if i == 0 && line.starts_with("::") {
					let tmp = parse_meta(line.as_str());
					if tmp > 0 { max = tmp; }
					continue;
				}
				let arr = line.split(" ");
				for item in arr {
					let item = item.trim();
					if item.len() > 0 {
						let id = item.parse::<u64>().unwrap();
						let p = map.entry(id).or_insert(0);
						*p += 1;
                        if *p == 1 {
							pool.push(item.parse::<u64>().unwrap());
						}
					}
				}
			}
			let mut dup = 0u64;
			for (id, num) in map {
				if num > 1 {
					dup += num - 1;
					println!("{}\t{}", id, num);
				}
			}
			println!("dup: {}", dup);
			(pool, max)
		}
		Err(err) => {
			match err.raw_os_error(){
				Some(2) => {
					println!("creating new pool ...");
				}
				_ => {
					return (Vec::<u64>::new(),0);
				}
			}
			let mut pool: Vec<u64> = (0u64..TOTAL_NUM).collect();
			let mut rng = rand::thread_rng();
			pool.shuffle(&mut rng);
			(pool,max)
		}
	}
}

pub fn save_pool(pool: &mut [u64], max: u64) {
	let mut rng = rand::thread_rng();
	pool.shuffle(&mut rng);
	let mut file = std::fs::File::create("pool.dat").unwrap();
	file.write_all(format!(":: max: {}, total: {}\n", max, pool.len()).as_ref()).unwrap();
	for value in pool {
		let res = file.write_all(format!("{}\n", value).as_ref());
		match res {
			Err(e) => println!("{}",e),
			Ok(()) => {}
		}
	}
}

pub fn extend_pool(pool: &mut Vec<u64>, base: u64, num: u64) {
	for i in base..base+num {
		pool.push(i);
	}
	//let mut rng = rand::thread_rng();
	//pool.shuffle(&mut rng);
}

fn parse_meta(line: &str) -> u64 {
	let mut max = 0u64;
	let yaml_str = "{".to_string() + &line[2..].to_string() + "}";
	let doc = yaml_rust::YamlLoader::load_from_str(yaml_str.as_str()).unwrap();
	let node = &doc[0];

	if ! node.is_badvalue() {
		let opt = &node["max"];
		if !opt.is_badvalue() {
			max = opt.as_i64().unwrap() as u64;
		}
	}

	max
}

pub fn check_pool(pool: &mut Vec<u64>, db: &HashMap<u64,u64>, max: u64) {
	let mut map = HashMap::new();
	let mut dup = Vec::<u64>::new();
	for i in &(*pool) {
		if db.contains_key(&(BASE_ID + *i)) {
			dup.push(*i);
		} else {
			let p = map.entry(*i).or_insert(0);
			*p += 1;
		}
	}
	if dup.len() > 0 {
		print!("removing: ");
		for i in &dup {
			pool.remove_item(i);
			print!("{} ", *i);
		}
		println!();
	}
	let mut count = 0u64;
	for i in 1..max {
		let id = BASE_ID + i;
		if ! db.contains_key(&id) &&  ! map.contains_key(&i) {
			pool.push(i);
			count += 1;
			println!("added missing id {} ...", i);
		}
	}

	if count > 0 {
		println!(">>>>>>>>> num of missing id = {}", count);
	} else {
		println!(">>>>>>>>> ok. no missing id detected");
	}
}

