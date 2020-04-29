#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

use deadpool_postgres::{Manager, Pool};
use std::collections::HashMap;

static mut POOL: Option<Pool> = None;

pub fn init() {
    let mut cfg = tokio_postgres::Config::new();
    //cfg.host("127.0.0.1");
    cfg.host("192.168.56.107");
    cfg.dbname("im");
    cfg.user("y");
    //cfg.password("db");
    println!("postgres: {:?}", cfg);
    let mgr = Manager::new(cfg, tokio_postgres::NoTls);
    unsafe { POOL = Some(Pool::new(mgr, 15)) };
    //let conn = pool.get().await.unwrap();
}

pub async fn save(hmap: &HashMap::<u64, String>) {
    let conn = unsafe {
        match &POOL {
            Some(pool) => {
                pool.get().await
            }
            None => {
                return;
            }
        }
    };

    if conn.is_err() {
        println!("{}", conn.err().unwrap());
        return;
    }
    let pg = &mut conn.unwrap();

    let mut sql = String::from("INSERT into u(id,nr) VALUES");
    let mut count:u64 = 0;
    for (k, v) in hmap {
        if count < 1 {
            sql += format!("({},'{}')", k, v).as_str();
        } else {
            sql += format!(",({},'{}')", k, v).as_str();
        }
        count += 1;
    }
    sql += "ON CONFLICT (id) DO UPDATE SET nr = EXCLUDED.nr";
    if count > 0 {
        let statement = pg.prepare(&sql).await.unwrap();
        let res = pg.execute(&statement, &[]).await;
        if res.is_ok() {
            let rows = res.unwrap();
            println!("{}/{} rows inserted", rows, count);
        } else {
            println!("{}", res.err().unwrap());
        }
    }
}

pub async fn get_daily_acc() -> Result<u64, String> {
    let conn = unsafe {
        match &POOL {
            Some(pool) => {
                pool.get().await
            }
            None => {
                return Err("db connect error".to_string());
            }
        }
    };

    if conn.is_err() {
        println!("{}", conn.err().unwrap());
        return Err("db connect error".to_string());
    }

    let pg = &mut conn.unwrap();
    let sql = "select count(0) a from u where timestamp>now()-interval '32 hour'";
    let stmt = pg.prepare(sql).await.unwrap();
    let res = pg.query(&stmt, &[]).await.unwrap();
    let mut acc= 0i64;
    for row in res {
        acc = row.get(0);
    }
    Ok(acc as u64)
}

pub async fn get_url() -> Result<String, String> {
    let conn = unsafe {
        match &POOL {
            Some(pool) => {
                pool.get().await
            }
            None => {
                return Err("db connect error".to_string());
            }
        }
    };

    if conn.is_err() {
        println!("{}", conn.err().unwrap());
        return Err("db connect error".to_string());
    }

    let pg = &mut conn.unwrap();
    let sql = "select value from config where key='url'";
    let stmt = pg.prepare(sql).await.unwrap();
    let res = pg.query(&stmt, &[]).await.unwrap();
    let mut url= "".to_string();
    for row in res {
        url = row.get(0);
    }
    Ok(url)
}

pub async fn get_all() -> std::collections::HashMap<u64,u64> {
    let mut map = std::collections::HashMap::new();
    let conn = unsafe {
        match &POOL {
            Some(pool) => {
                pool.get().await
            }
            None => {
                return map;
            }
        }
    };

    if conn.is_err() {
        println!("{}", conn.err().unwrap());
        return map;
    }

    let pg = &mut conn.unwrap();
    let sql = "select distinct id from u";
    let stmt = pg.prepare(sql).await.unwrap();
    let res = pg.query(&stmt, &[]).await.unwrap();
    for row in res {
        let id: i64 = row.get(0);
        let p = map.entry(id as u64).or_insert(0);
        *p += 1;
    }
    map
}
