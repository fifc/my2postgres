extern crate postgres;

use std::io::BufRead;
use postgres::{Client, NoTls};

static DEBUG:i32 = 1;

fn test_buf() {
    let mut vc: Vec<u8> = vec!(0; 1<<24);
    vc.insert((1<<24)-1, 1);
    let vb = &mut vc[..(1<<24)];
    vb[(1<<24)-1] = 1;
    vc[2] = 1;
    use std::alloc::{alloc, dealloc, Layout};
    let layout = Layout::new::<u64>();
    unsafe {
        let ptr = alloc(layout);

        let slice = std::slice::from_raw_parts_mut(ptr as *mut i64, 100);
        let val = 123456789012345678i64;
        slice[2] = val;

        assert_eq!(slice[2], val);
        if DEBUG == 0 {
            dealloc(ptr, layout);
        } else {
            let _str = String::from_raw_parts(slice.as_mut_ptr() as *mut u8, 20, 100);
        }
    }
}

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
    let mut total:u32 = 0;
    let mut sqlnum:u32 = 0;
    let mut rownum:u32 = 0;
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
                println!("{}",sql.get_unchecked_mut(..100));
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

    println!("total:{}, sql:{}, row:{}",total, sqlnum, rownum);
}

fn main() {
    let args:Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("usage: {} mysql-dump-file", args[0]);
        return;
    }
    let filename = &args[1];
    test_buf();
    let pgres = Client::connect("host=192.168.56.107 user=y dbname=im", NoTls);
    if pgres.is_err() {
        println!("{}",pgres.err().unwrap());
        return
    }
    let mut pg = &mut pgres.unwrap();
    test_pg(&mut pg).unwrap();
    //let filename = "C:\\Users\\max\\Documents\\dumps\\Dump20191229\\app_s.sql";
    println!("filename: {}", filename);
    let file = std::fs::File::open(filename).unwrap();
    let mut reader = std::io::BufReader::new(file);
    import(&mut pg, &mut reader);
}
