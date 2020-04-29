extern crate chrono;

use std::io::Write;
use mysql_async::prelude::*;
use deadpool_postgres::{Pool};

const BATCH_NUM:i32 = 1000;
const DST_TABLE:&str = "c";
const APP_TABLE:&str = "contacts";

struct State {
    timestamp: u64,
    filter: Vec<String>
}

struct Record {
    pub id: i64,
    pub im: String,
    pub ph: String,
    pub rm: String,
    pub rp: String,
    pub fa: String,
    pub ln: String,
    pub mn: String,
    pub ts: i64
}

async fn poll_stream(pg_pool: &mut Pool, mysql_pool: &mut mysql_async::Pool, state: &mut State) -> usize {
    let mut total: usize = 0;
    const STEP:usize = BATCH_NUM as usize/1000*100;

    let mysql = mysql_pool.get_conn().await.unwrap();
    print!("->  "); std::io::stdout().flush().unwrap();
    let mut sql = build_sql(state);
    let result = mysql.prep_exec(sql, ()).await.unwrap();
    let (_, rows) = result.map_and_drop(|row| {
        let (id,im,ph,rm,rp,fa,ln,mn,ts):
            (i64,String,String,String,String,String,String,String,i64) = mysql_async::from_row(row);
        total += 1;
        if (total%STEP) == 0 { print!("*"); std::io::stdout().flush().unwrap(); }
        Record {id,im,ph,rm,rp,fa,ln,mn,ts}
    }).await.unwrap();

    print!("  "); std::io::stdout().flush().unwrap();
    let pg = pg_pool.get().await.unwrap();
    sql = format!("insert into {} values($1,$2,$3,$4,$5,$6,$7,$8,super::stream::to_timestamp($9)) on conflict do nothing", DST_TABLE);
    let stmt = pg.prepare(sql.as_str()).await.unwrap();

    total = 0usize;
    for row in &rows {
        total += 1;
        if (total%STEP) == 0 /*|| i == rows.len() - 1*/ { print!("s"); std::io::stdout().flush().unwrap();}
        pg.execute(&stmt,
                   &[&row.id, &row.im, &row.ph, &row.rm, &row.rp, &row.fa, &row.ln, &row.mn, &(row.ts as f64)])
            .await.unwrap() as usize;
    }

    total
}


async fn init_state(pg_pool: &mut Pool, state: &mut State) {
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
    let sql = format!("select id,im,ph,date_part('epoch',ts)::int ts from {0} where ts=(select max(ts) from {0})", DST_TABLE);
    let stmt = pg.prepare(sql.as_str()).await.unwrap();
    let res = pg.query(&stmt, &[]).await.unwrap();
    state.filter.clear();
    let mut timestamp = 0i32;
    for row in res {
        timestamp = row.get(3);
        //let id:i64 = row.get(0);
        let no:String = row.get(2);
        if ! state.filter.contains(&no) {
            state.filter.push(no);
        }
    }

    if timestamp > 0 {
        state.timestamp = timestamp as u64;
    }
}

fn build_sql(state: &State) -> String {
    let select = "select uid id,imei im,phone ph,remark rm,raw_phone rp,firstname fn,lastname ln,middlename mn,unix_timestamp(timestamp) ts from";
    if state.filter.is_empty() {
        format!("{} {} where timestamp>from_unixtime({}) order by timestamp limit {}", select, APP_TABLE, state.timestamp, BATCH_NUM)
    } else {
        let mut filter_set = format!("'{}'", state.filter[0]);
        for i in 1..state.filter.len() {
            filter_set += format!(",'{}'", state.filter[i]).as_str();
        }
        format!("{0} {1} where timestamp>from_unixtime({2}) or (timestamp=from_unixtime({2}) and phone not in ({3})) order by timestamp limit {4}",
                select, APP_TABLE, state.timestamp, filter_set, BATCH_NUM)
    }
}

pub async fn run() {
    super::cfg::init("config.yml");
    let mut pg = super::stream::init_pg().await;
    let mut mysql = super::stream::init_mysql().await.unwrap();
    stream(&mut pg, &mut mysql).await;
    mysql.disconnect().await.unwrap()
}

pub async fn stream(pg_pool: &mut Pool, mysql_pool: &mut mysql_async::Pool) {
    let mut total = 0usize;
    let mut state = State {timestamp: 0u64, filter: Vec::<String>::new()};
    loop {
        init_state(pg_pool, &mut state).await;
        print!("[{}] ", super::stream::to_timestamp(state.timestamp));
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

