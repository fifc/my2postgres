extern crate postgres;

use postgres::{Client, NoTls};
use std::io::BufRead;

fn test_pg(pg: &mut Client) -> Result<u32, String> {
    let id = 5i64;
    let name = "hello";

    let res = pg.execute("insert into t(id,name) values($1, $2)", &[&id, &name]);
    if res.is_ok() {
        println!("insert ok");
    } else {
        println!("{}", res.err().unwrap());
    }

    for row in pg.query("select id, name from t", &[]).unwrap() {
        let id: i64 = row.get(0);
        let name: &str = row.get(1);
        println!("id: {}, name: {}", id, name);
    }
    Ok(0)
}

fn import(pg: &mut Client, reader: &mut std::io::BufReader<std::fs::File>) {
    let mut total: u32 = 0;
    let mut sqlnum: u32 = 0;
    let mut rownum: u32 = 0;
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

            let statement = pg.prepare(&sql).unwrap();
            let res = pg.execute(&statement, &[]);
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

pub fn run(filename: &str, uri: &str) {
    let pgres = Client::connect(uri, NoTls);
    if pgres.is_err() {
        println!("{}", pgres.err().unwrap());
        return;
    }
    let mut pg = &mut pgres.unwrap();
    //test_pg(&mut pg).unwrap();
    let file = std::fs::File::open(filename).unwrap();
    let mut reader = std::io::BufReader::new(file);
    import(&mut pg, &mut reader);
}
