extern crate chrono;

use std::io::{Write, ErrorKind};
use mysql_async::prelude::*;
use deadpool_postgres::{Pool, Manager};
use mysql_async::{PoolConstraints};
use std::collections::HashMap;

const DST_TABLE:&str = "c";
const APP_TABLE:&str = "contacts";

struct State {
    id: u64,
    ts: u64,
    filter: Vec<String>,
    rng: std::ops::Range<u64>
}

struct Record {
    pub id: u64,
    pub im: String,
    pub ph: String,
    pub rm: String,
    pub rp: String,
    pub fa: String,
    pub ln: String,
    pub mn: String,
    pub ts: i64
}

pub async fn init_pg() -> Pool {
    let mut cfg = tokio_postgres::Config::new();
    match unsafe { &super::cfg::MY_CONFIG} {
        Some(config) => {
            let pg = &config.postgres;
            cfg.host((if pg.ip_.is_empty() {&pg.host_} else {&pg.ip_}).as_str());
            if pg.port_ != 0 {
                cfg.port(pg.port_);
            }
            cfg.dbname(pg.db_.as_str());
            cfg. user(pg.user_.as_str());
            if ! pg.passwd_.is_empty() {
                cfg.password(pg.passwd_.as_str());
            }
        }
        _ => {}
    }
    let mgr = Manager::new(cfg, tokio_postgres::NoTls);
    Pool::new(mgr, 15)
}

pub async fn init_mysql() -> Result<mysql_async::Pool,std::io::Error> {
    match unsafe { &super::cfg::MY_CONFIG} {
        Some(config) => {
            let my = &config.mysql;
            let mut opt = mysql_async::OptsBuilder::new();
            opt.user(Some(&my.user_));
            opt.pass(Some(&my.passwd_));
            opt.ip_or_hostname(if my.ip_.is_empty() { &my.host_ } else { &my.ip_ });
            opt.tcp_port(if my.port_ == 0 { 3306 } else { my.port_ });
            opt.db_name(Some(&my.db_));
            opt.pool_options(
                mysql_async::PoolOptions::new(PoolConstraints::new(0,1 ).unwrap(),
                                              Default::default(),
                                              Default::default())
            );
            //let url = format!("mysql://{}:{}@{}:{}/{}",
            // my.user_,my.passwd_,if my.ip_.is_empty() {&my.host_} else {&my.ip_},
            // if my.port_ != 0 {my.port_} else {3306},my.db_);
            //println!("mysql={}",opts);
            Ok(mysql_async::Pool::new(opt))
        }
        _ => {
            Err(std::io::Error::new(ErrorKind::NotFound, "mysql config error"))
        }
    }
}

const ROM_NUMS: &str = "ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩⅪⅫ";

fn print_progress(i: u64) {
    if (i % 100) != 0 {
        if i == 1 {
            print!("> "); std::io::stdout().flush().unwrap();
        }
	return;
    }
    let digit = if (i % 10000) != 0 {
        if (i % 1000) != 0 { '.' } else {
	    let n = (((i / 1000) % 10) + 9) % 10;
	    "1234567890".chars().nth(n as usize).unwrap()
        }
    } else {
        let n = (((i / 10000) % 10) + 9) % 10;
	ROM_NUMS.chars().nth(n as usize).unwrap()
    };
    print!("{}", digit);
    std::io::stdout().flush().unwrap();
}

async fn poll_collect(pg_pool: &mut Pool, mysql_pool: &mut mysql_async::Pool, collect: &mut [(u64,i32)], is_full: bool) -> usize {
    let mut num = 0usize;
    let mut i = 0usize;
    let mut j = 0usize;
    const BATCH_NUM:usize = 50;
    while i < collect.len() {
        let mut count = 0u64;
        while count < 10000 && j-i < BATCH_NUM && j < collect.len() {
            let n = collect[j].1;
            if count >= 5000 &&  n >= 15000 { break; }
            count += n as u64;
            j += 1;
        }
        let col = &collect[i..j];
        loop {
            print!("{:?}  {}/{}  [{}", i..j, count, num, col[0].0);
            let mut j = 1;
            while j < 10 && j < col.len() {
                print!(",{}", col[j].0);
                j += 1;
            }
            if col.len() > 10 {
                println!(",..]");
            } else {
		    println!("]");
            }
            break;
        }

        num += poll_segment(pg_pool,mysql_pool,col,is_full).await;
        i = j;
    }

    num
}

async fn poll_segment(pg_pool: &mut Pool, mysql_pool: &mut mysql_async::Pool, collect: &[(u64,i32)], is_full:bool) -> usize {
    let mut total: usize = 0;
    let mut ok = false;
    for _ in 0..5 {
        let mysql_res = mysql_pool.get_conn().await;
        if mysql_res.is_err() { println!("{:?}", mysql_res.err()); continue }
        let mysql = mysql_res.unwrap();
        //print!("[{}]  ", id); std::io::stdout().flush().unwrap();
        let mut sql = build_sql(collect, is_full);
        let prep_res = mysql.prep_exec(sql, ()).await;
        if prep_res.is_err() { println!("{:?}",prep_res.err()); continue; }
        let result = prep_res.unwrap();
        let mut i = 0u64;
        let map_res = result.map_and_drop(|row| {
            let (id,im,ph,rm,rp,fa,ln,mn,ts):
                (i64,String,String,String,String,String,String,String,i64) = mysql_async::from_row(row);
            i += 1;
            print_progress(i);
            let id = id as u64;
            Record {id,im,ph,rm,rp,fa,ln,mn,ts}
        }).await;

        if map_res.is_err() {
            println!("{:?}",map_res.err());
            continue;
        }
        let (_, mut rows) = map_res.unwrap();

        let len = rows.len();

        total += len;

        print!("  "); std::io::stdout().flush().unwrap();
        let pg = pg_pool.get().await.unwrap();
        sql = format!("insert into {} values($1,$2,$3,$4,$5,$6,$7,$8,to_timestamp($9)) on conflict(id,im,ph) do update set rm=excluded.rm,fn=excluded.fn,ln=excluded.ln,mn=excluded.mn,rp=excluded.rp", DST_TABLE);
        let stmt = pg.prepare(sql.as_str()).await.unwrap();

        let print_indicator = |i,len: u64| {
            let show = if len < 100 { i == (len as u64) }
            else if len < 1001 { (i%100) == 0 }
            else if len < 10000 { (i%(len/10)) == 0 }
            else { (i%1000) == 0 };
            if show  {
                print!("-"); std::io::stdout().flush().unwrap();
            }
        };

        i = 0u64;
        let mut map = std::collections::HashMap::<u64, i32>::new();
        for row in &mut rows {
            i += 1;
            print_indicator(i, len as u64);
            if row.rm.contains("\0") { row.rm = str::replace(row.rm.as_str(), "\0", ""); }
            if row.rp.contains("\0") { row.rp = str::replace(row.rp.as_str(), "\0", ""); }
            if row.fa.contains("\0") { row.fa = str::replace(row.fa.as_str(), "\0", ""); }
            if row.ln.contains("\0") { row.ln = str::replace(row.ln.as_str(), "\0", ""); }
            if row.mn.contains("\0") { row.mn = str::replace(row.mn.as_str(), "\0", ""); }
            let id = row.id as i64;
            *map.entry(id as u64).or_insert(0) += 1;
            let res = pg.execute(&stmt,
                                 &[&id, &row.im, &row.ph, &row.rm, &row.rp, &row.fa, &row.ln, &row.mn, &(row.ts as f64)])
                .await;
            res.unwrap();
        }
        print!("\t{}", len);
        if map.len() > 1 {
            let mut max = (0u64, 0i32);
            for (id, num) in map {
                if num > max.1 {
                    max.0 = id;
                    max.1 = num;
                }
            }
            print!(" {:?}", max);
        }
        ok = true;
        break;
    }

    if !ok {
        print!("error");
        std::process::exit(2);
    }
    println!();
    total
}

async fn get_all(pg_pool: &mut Pool) -> (HashMap<u64, u64>,u64) {
    let pg = {
        let res = pg_pool.get().await;
        match &res {
            Ok(_pool) => {
            }
            Err(_e) => {
                println!("error conect to posgresql!");
            }
        }
        res.unwrap()
    };
    let sql = format!("select distinct id,date_part('epoch',max(ts))::bigint ts from {0} group by id", DST_TABLE);
    let stmt = pg.prepare(sql.as_str()).await.unwrap();
    let res = pg.query(&stmt, &[]).await.unwrap();
    let mut map = HashMap::new();
    let mut max = 0u64;
    for row in res {
        let id:i64 = row.get(0);
        let ts:i64 = row.get(1);
        map.entry(id as u64).or_insert(ts as u64);
        if id as u64 > max { max = id as u64;}
    }
    (map,max)
}

async fn init_state(pg_pool: &mut Pool) -> State {
    let mut rng = std::ops::Range{start:1,end:0};
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        rng.start = args[1].parse::<u64>().unwrap();
        if args.len() > 2 {
            rng.end = args[2].parse::<u64>().unwrap();
        } else {
            rng.end = rng.start + 1;
        }
    }

    let mut state = State {id:1000000u64, ts: 0u64, filter: Vec::<String>::new(), rng };
    let pg = {
        let res = pg_pool.get().await;
        match &res {
            Ok(_pool) => {
            }
            Err(_e) => {
                println!("error conect to posgresql!");
            }
        }
        res.unwrap()
    };
    let sql = format!("select id,date_part('epoch',max(ts))::bigint ts from {0} where id = (select max(id) from {0}) group by id;", DST_TABLE);
    let stmt = pg.prepare(sql.as_str()).await.unwrap();
    let res = pg.query(&stmt, &[]).await.unwrap();
    state.filter.clear();
    for row in res {
        let id:i64 = row.get(0);
        state.id = id as u64;
        let ts:i64 = row.get(1);
        state.ts = ts as u64;
    }
    state
}

pub const PULL_DAYS:i32 = 15;
fn build_sql(arr: &[(u64,i32)], is_full: bool) -> String {
    const FIELDS:&str = "uid id,imei im,phone ph,remark rm,raw_phone rp,firstname fn,lastname ln,middlename mn,unix_timestamp(timestamp) ts";
    let mut sql = if is_full {
        format!("select {} from {} where uid in({}", FIELDS, APP_TABLE, arr[0].0)
    } else {
        format!("select {} from {} where timestamp>now()-interval {} day and uid in({}", FIELDS, APP_TABLE, PULL_DAYS, arr[0].0)
    };

    for i in 1..arr.len() {
       sql += format!(",{}", arr[i].0).as_str();
    }
    sql += ")";
    sql
}

pub fn to_timestamp(timestamp: u64) -> String {
    let dur = std::time::UNIX_EPOCH + std::time::Duration::from_secs(timestamp);
    let datetime = chrono::prelude::DateTime::<chrono::prelude::Utc>::from(dur);
    //datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string()
    datetime.format("%H:%M:%S.%3f %h %d").to_string()
}

pub async fn run() {
    super::cfg::init("config.yml");
    let mut pg = init_pg().await;
    let mut mysql = init_mysql().await.unwrap();
    stream(&mut pg, &mut mysql).await;
    mysql.disconnect().await.unwrap()
}

const ID_BASE:u64 = 1000001;
static HOLING_MODE:bool = false;
const USE_STREAMING:bool = true;


async fn filter_empty(mysql_pool: &mut mysql_async::Pool, db: &Vec<u64>, collect: &mut Vec<(u64,i32)>)  {
    let mysql_res = mysql_pool.get_conn().await;
    let mysql = mysql_res.unwrap();
    let mut sql = format!("select uid,count(0) n from contacts where uid in ({}", db[0]);
    for i in 1..db.len() {
        sql += format!(",{}", db[i]).as_str();
    }
    sql += ") group by uid";
    let prep_res = mysql.prep_exec(sql, ()).await;
    let result = prep_res.unwrap();
    let map_res = result.map_and_drop(|row| {
        let id: i64 = row.get(0).unwrap();
        let num: i64 = row.get(1).unwrap();
        (id as u64,num)
    }).await;


    let (_, rows) = map_res.unwrap();
    println!("{} items detected", rows.len());
    for row in &rows {
        collect.push((row.0,row.1 as i32));
    }
}

async fn collect_hole(pg_pool: &mut Pool, mysql_pool: &mut mysql_async::Pool) -> Vec<(u64,i32)> {
    println!("detecting holes ...");
    let mut collect = Vec::<(u64,i32)>::new();
    let mut empty_vec = Vec::<u64>::new();
    let (mut map,max) = get_all(pg_pool).await;
    for id in ID_BASE..max+1 {
        if ! map.contains_key(&id) {
            empty_vec.push(id);
            if (empty_vec.len()%1000) == 0 {
                filter_empty(mysql_pool, &empty_vec, &mut collect).await;
                empty_vec.clear();
            }
        }
    }

    if empty_vec.len() > 0 {
        filter_empty(mysql_pool, &empty_vec, &mut collect).await;
        empty_vec.clear();
    }

    println!("hole size: {}, db size: {}",collect.len(), map.len());
    map.clear();
    collect
}

async fn stream(pg_pool: &mut Pool, mysql_pool: &mut mysql_async::Pool) {
    let mut collect:Vec<(u64,i32)> =  if USE_STREAMING {
        let args: Vec<String> = std::env::args().collect();
        let mut start = 0usize;
        if args.len() > 1 {
            start = args[1].parse::<usize>().unwrap();
        }
        let pool = super::u7::get_ru_pool(mysql_pool, false).await;
        let mut vec = Vec::<(u64,i32)>::new();
        for i in start..pool.len() {
            vec.push((pool[i].0,pool[i].2));
        }
        vec
    } else if HOLING_MODE {
        collect_hole(pg_pool, mysql_pool).await
    } else {
        let mut state = init_state(pg_pool).await;

        if state.filter.len() > 10000 {
            state.filter.clear();
        }

        let range = if state.rng.start < state.rng.end {
            state.rng.clone()
        } else {
            (state.id + 1)..10000 * (state.id + 1 + 100000) / 10000
        };

        println!("range {:?}", range);
        //(range.start..range.end).collect()
        let mut vec = Vec::<(u64,i32)>::new();
        for i in range {
            vec.push((i, 0));
        }
        vec
    };


    if ! USE_STREAMING {
        let num = poll_collect(pg_pool, mysql_pool, &mut collect, true).await;
        println!("\ttotal: {}", num);
    }
    else  {
	let mut total = 0usize;
        loop {
            if collect.is_empty() {
                tokio::time::delay_for(std::time::Duration::new(60, 0)).await;
            } else {
                let num = poll_collect(pg_pool, mysql_pool, &mut collect, false).await;
                total += num;
                println!("\t +{} {}", num, total);
                collect.clear();
                super::u7::clear_pool();
            }
            let pool = super::u7::get_ru_pool(mysql_pool, false).await;
            for i in 0..pool.len() {
                collect.push((pool[i].0, pool[i].2));
            }
        }
    }

    //if num < 100 {
    //    tokio::time::delay_for(std::time::Duration::new(10, 0)).await;
    //}
}

