extern crate chrono;

use std::io::Write;
use futures::io::ErrorKind;
use mysql_async::prelude::*;
use deadpool_postgres::{Manager, Pool};

const BATCH_NUM:i32 = 5000;
const DST_TABLE:&str = "s";
const APP_TABLE:&str = "accstat";

struct State {
    timestamp: u64,
    filter: Vec<u64>
}

struct Record {
    pub id: i64,
    pub qua: String,
    pub dev: String,
    pub net: String,
    pub code: i32,
    pub host: String,
    pub port: i32,
    pub cost: i32,
    pub addr: String,
    pub ts: i64
}

async fn init_pg() -> Pool {
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

async fn init_mysql() -> Result<mysql_async::Pool,std::io::Error> {
    match unsafe { &super::cfg::MY_CONFIG} {
        Some(config) => {
            let my = &config.mysql;
            let url = format!("mysql://{}:{}@{}:{}/{}", my.user_,my.passwd_,if my.ip_.is_empty() {&my.host_} else {&my.ip_},if my.port_ != 0 {my.port_} else {3306},my.db_);
            //println!("mysql={}",url);
            Ok(mysql_async::Pool::new(url))
        }
        _ => {
            Err(std::io::Error::new(ErrorKind::NotFound, "mysql config error"))
        }
    }
}

async fn poll_stream(pg_pool: &mut Pool, mysql_pool: &mut mysql_async::Pool, state: &mut State) -> usize {
    let mut total: usize = 0;
    const STEP:usize = (BATCH_NUM as usize/1000*100);

    let mysql = mysql_pool.get_conn().await.unwrap();
    print!(">\t"); std::io::stdout().flush().unwrap();
    let mut sql = build_sql(state.timestamp, &state.filter);
    let result = mysql.prep_exec(sql, ()).await.unwrap();
    let (_, rows) = result.map_and_drop(|row| {
        let (id,qua,dev,net,code,host,port,cost,addr,ts):
            (i64,String,String,String,i32,String,i32,i32,String,i64) = mysql_async::from_row(row);
        total += 1;
        if (total%STEP) == 0 { print!("*"); std::io::stdout().flush().unwrap(); }
        Record {id,qua,dev,net,code,host,port,cost,addr,ts}
    }).await.unwrap();

    print!("  "); std::io::stdout().flush().unwrap();
    let pg = pg_pool.get().await.unwrap();
    sql = format!("insert into {} values($1,$2,$3,$4,$5,$6,$7,$8,$9,to_timestamp($10))", DST_TABLE);
    let stmt = pg.prepare(sql.as_str()).await.unwrap();

    total = 0usize;
    for row in &rows {
        total += 1;
        if (total%STEP) == 0 /*|| i == rows.len() - 1*/ { print!("="); std::io::stdout().flush().unwrap();}
        pg.execute(&stmt,
                   &[&row.id, &row.qua, &row.dev, &row.net, &row.code, &row.host, &row.port, &row.cost, &row.addr, &(row.ts as f64)])
            .await.unwrap() as usize;
    }

    total
}


async fn init_state(pg_pool: &mut Pool, state: &mut State) {
    let pg = pg_pool.get().await.unwrap();
    let sql = format!("select uid,deviceinfo,date_part('epoch',timestamp)::int ts from {0} where timestamp in (select max(timestamp) from {0})", DST_TABLE);
    let stmt = pg.prepare(sql.as_str()).await.unwrap();
    let res = pg.query(&stmt, &[]).await.unwrap();
    state.filter.clear();
    let mut timestamp = 0i32;
    for row in res {
        timestamp = row.get(2);
        let id:i64 = row.get(0);
        if ! state.filter.contains(&(id as u64)) {
            state.filter.push(id as u64);
        }
    }

    if timestamp > 0 {
        state.timestamp = timestamp as u64;
    }
}

fn build_sql(timestamp: u64, filter: &[u64]) -> String {
    let select = "select uid,qua,deviceinfo dev,network,code,host,port,timecost cost,addr,unix_timestamp(timestamp) ts from";
    if filter.is_empty() {
        format!("{} {} where timestamp>from_unixtime({}) limit {}", select, APP_TABLE, timestamp, BATCH_NUM)
    } else {
        let mut filter_set = format!("{}", filter[0]);
        for i in 1..filter.len() {
            filter_set += format!(",{}", filter[i]).as_str();
        }
        format!("{0} {1} where timestamp>from_unixtime({2}) or (timestamp=from_unixtime({2}) and uid not in ({3})) limit {4}",
                select, APP_TABLE, timestamp, filter_set, BATCH_NUM)
    }
}

fn to_timestamp(timestamp: u64) -> String {
    let dur = std::time::UNIX_EPOCH + std::time::Duration::from_secs(timestamp);
    let datetime = chrono::prelude::DateTime::<chrono::prelude::Utc>::from(dur);
    //datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string()
    datetime.format("%m-%d %H:%M:%S.%3f").to_string()
}

pub async fn run() {
    super::cfg::init("config.yml");
    let mut pg = init_pg().await;
    let mut mysql = init_mysql().await.unwrap();
    super::stream::stream(&mut pg, &mut mysql).await;
    mysql.disconnect().await.unwrap()
}

pub async fn stream(pg_pool: &mut Pool, mysql_pool: &mut mysql_async::Pool) {
    let mut total = 0usize;
    let mut state = State {timestamp: 0u64, filter: Vec::<u64>::new()};
    loop {
        init_state(pg_pool, &mut state).await;
        print!("[{}] ", to_timestamp(state.timestamp));
        std::io::stdout().flush().unwrap();

        if state.filter.len() > 10000 {
            state.filter.clear();
        }

        let num = poll_stream(pg_pool, mysql_pool, &mut state).await;
        total += num as usize;
        println!("\t+{}\t[{}]", num, total);

        if num < 100 {
            tokio::time::delay_for(std::time::Duration::new(10, 0)).await;
        }
    }
}

