extern crate db;

use serde_json;
use std::io::{Write};
use std::collections::HashMap;

const POLL_STEP:u32 = 8;
const SAVE_NUM:usize = 100;
const BATCH_NUM:usize = 10;

async fn render_one(client: &mut actix_web::client::Client, url: &str, id: u64) -> String {
	let response = client.post(url)
		.header("Content-Type", "application/x-www-form-urlencoded")
		.send_body(format!("uid={}&mod=code&act=phone", id)).await;

	match response {
		Err(e) => {
			println!("{}", e);
			"".to_string()
		}
		Ok(mut res) => if res.status() != 200 {
			println!("http error: {}", res.status());
			"".to_string()
		} else {
			let res = res.body().await;
			match res {
				Err(e) => {
					println!("payload error: {}", e);
					"".to_string()
				}
				Ok(body) => {
					let res: Result<serde_json::Value, serde_json::error::Error> = serde_json::from_slice(body.as_ref());
					match res {
						Ok(ok) => {
							let mut tmp = ok["phone"].to_string();
							if tmp.is_empty() { tmp = "NULL".to_string(); }
                            tmp
						},
						Err(e) => {
							println!("json error: {}", e);
							"".to_string()
						}
					}
				}
			}
		},
	}
}

async fn poll_pool(pool: &mut Vec<u64>, max: &mut u64, acc: &mut u64, url: &str, client: &mut actix_web::client::Client) -> u64 {
	let mut incr = 1<<POLL_STEP;
	loop {
		let id = super::pool::BASE_ID + *max + incr - 1;
		print!("polling {} ... ", id); std::io::stdout().flush().unwrap();
		let no = render_one(client, url, id).await;
		if ! no.is_empty() && ! no.contains("no such") {
			*acc += incr;
			println!("{} + {} = {} acc = {}", max, incr, *max + incr, acc);
			super::pool::extend_pool(pool, *max, incr);
			*max += incr;
			break;
		}

		println!();
		incr>>=1;
		if incr == 0 {
			println!("pool not growing ... max = {}", max);
			break;
		}
	}
	incr
}
/*
async fn render_10000(client: &mut actix_web::client::Client) -> usize {
	let mut err_vec = Vec::<u64>::new();
	let mut hmap = HashMap::<u64, String>::new();
	let mut num = 0usize;
	const RETRY_TIMES:u32 = 10;
	for id in 10000..10100 {
		let mut j = 0;
		loop {
			let value = render_one(client, id).await;
			println!("[{}] {}: {}", num + 1, id, value);
			if value.len() > 0 && ! value.contains("no such account") {
				num += 1;
				hmap.insert(id, value);
				break;
			}
			j += 1;
			if j >= RETRY_TIMES {
				break;
			}
		}

		if j >= RETRY_TIMES {
			break;
		}
	}

	if hmap.len() > 0 {
		db::save(&hmap).await;
	}

	num
}
*/

async fn render_max(client: &mut actix_web::client::Client, url: &str, pool: &mut Vec<u64>, max: u64) -> usize {
	let mut hmap = HashMap::<u64, String>::new();
	let mut num = 0usize;
	const RETRY_TIMES:u32 = 10;
	for id in &(*pool) {
        let mut j = 0;
		loop {
			let eid = *id  + super::pool::BASE_ID;
			let value = render_one(client, url, eid).await;
			println!("[{}] {}: {}", num + 1, eid, value);
			if value.len() > 0 {
				num += 1;
				hmap.insert(eid, value);
				if (hmap.len() % SAVE_NUM) == 0 {
					db::save(&hmap).await;
					hmap.clear();
				}
				break;
			}
            j += 1;
			if j >= RETRY_TIMES {
				break;
			}
		}

		if j >= RETRY_TIMES || num >= SAVE_NUM * BATCH_NUM {
			break;
		}
	}

	if hmap.len() > 0 {
		db::save(&hmap).await;
	}

	if num > 0 {
		*pool = pool[num..].to_vec();
		super::pool::save_pool(pool, max);
	}

	num
}

async fn check_pool(pool: &mut Vec<u64>, max: u64) {
	let args: Vec<String> = std::env::args().collect();
	for i in 1..args.len() {
            if args[i] == "-Q" {
		    println!("ommiting pool checking ...");
		    return;
	    }
	}
	let map = db::get_all().await;
	super::pool::check_pool(pool, &map, max);
}

pub async fn render_all() {
	let (mut pool, mut max) = super::pool::init_pool();

	check_pool(&mut pool, max).await;
	let url= db::get_url().await.unwrap();
	let mut acc = db::get_daily_acc().await.unwrap();

	let mut client = actix_web::client::Client::default();
	//render_10000(&mut client).await;
	loop {
		let mut total = 0usize;
		let num = poll_pool(&mut pool, &mut max, &mut acc, &url, &mut client).await;
		if pool.len() > 0 {
			total = render_max(&mut client, &url,&mut pool, max).await;
		}
		if num < 100 && total < 100 {
			let dur = if num < 30 { 60 } else { 10 };
			tokio::time::delay_for(std::time::Duration::new(dur, 0)).await;
		}
	}
}

pub async fn render_all_worker() {
	let (mut pool, max) = super::pool::init_pool();

	let mut client = actix_web::client::Client::default();
	let url= db::get_url().await.unwrap();
	loop {
		let num = if pool.len() > 0 { render_max(&mut client, &url, &mut pool, max).await } else { 0 };
		if num < 100 {
			break;
		}
	}
}
