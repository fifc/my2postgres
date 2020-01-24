use deadpool_postgres::{Manager, Pool};
use std::io::BufRead;

pub fn init() -> Pool {
    let mut cfg = tokio_postgres::Config::new();
    cfg.host("192.168.56.107");
    cfg.dbname("im");
    cfg.user("y");
    //cfg.password("db");
    let mgr = Manager::new(cfg, tokio_postgres::NoTls);
    Pool::new(mgr, 15)
}


async fn test_pg(pool: &mut Pool) -> Result<u32, String> {
    let id = 5i64;
    let name = "hello";

    let pg = pool.get().await.unwrap();
    let res = pg.execute("insert into t(id,name) values($1, $2)", &[&id, &name]).await;
    if res.is_ok() {
        println!("insert ok");
    } else {
        println!("{}", res.err().unwrap());
    }

    for row in pg.query("select id, name from t", &[]).await.unwrap() {
        let id: i64 = row.get(0);
        let name: &str = row.get(1);
        println!("id: {}, name: {}", id, name);
    }
    Ok(0)
}

async fn import(pool: &mut Pool, reader: &mut std::io::BufReader<std::fs::File>) {
    let mut total: u32 = 0;
    let mut sqlnum: u32 = 0;
    let mut rownum: u32 = 0;

    let pg = pool.get().await.unwrap();
    for (_i, line) in reader.lines().enumerate() {
        total += 1;
        let mut line = line.unwrap();
        if line.starts_with("INSERT INTO ") {
            sqlnum += 1;
            let mut sql = "INSERT INTO s".to_owned();
            //let pos = if line.chars().nth(14) == Some('`') {15} else if line.chars().nth(15) == Some('`') {16} else {21};
            let res = line.find(" VALUES (");
            if res == None {
                continue;
            }
            unsafe {
                let body = line.get_unchecked_mut(res.unwrap()..);
                sql.push_str(body);
                println!("{}", sql.get_unchecked_mut(..100));
            }

            //continue;

            let statement = pg.prepare(&sql).await.unwrap();
            let res = pg.execute(&statement, &[]).await;
            if res.is_ok() {
                let rows = res.unwrap();
                rownum += rows as u32;
                println!("{} rows inserted", rows);
            } else {
                println!("{}", res.err().unwrap());
            }
        } else {
            //println!("{}",line);
        }
    }

    println!("total:{}, sql:{}, row:{}", total, sqlnum, rownum);
}

pub async fn run(filename: &str) {
    let mut pool = init();
    let file = std::fs::File::open(filename).unwrap();
    let mut reader = std::io::BufReader::new(file);
    import(&mut pool, &mut reader).await;
}
