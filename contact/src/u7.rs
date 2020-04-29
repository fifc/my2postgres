use std::io::{BufRead, Write};
use mysql_async::prelude::Queryable;
use chrono::format::ParseError;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};

fn parse_meta(line: &str) -> u64 {
    let mut timestamp = 0u64;
    let yaml_str = "{".to_string() + &line[2..].to_string() + "}";
    let doc = yaml_rust::YamlLoader::load_from_str(yaml_str.as_str()).unwrap();
    let node = &doc[0];

    if ! node.is_badvalue() {
        let opt = &node["timestamp"];
        if !opt.is_badvalue() {
            timestamp = opt.as_i64().unwrap() as u64;
        }
    }

    timestamp
}

const POOL_FILE:&str = "pool";
fn load_pool() -> (Vec<(u64,i32,i32,u64)>,u64) {
    let mut vec = Vec::<(u64,i32,i32,u64)>::new();
    let mut timestamp = 0u64;

    let res = std::fs::File::open(POOL_FILE);

    match res {
        Ok(file) => {
            let reader = std::io::BufReader::new(file);
            for (i, line) in reader.lines().enumerate() {
                let line = line.unwrap();
                if i == 0 && line.starts_with("::") {
                    timestamp = parse_meta(line.as_str());
                    continue;
                }
                let arr:Vec<&str> = line.split(",").collect();
                if arr.len() >= 3 {
                    let mut id = 0u64;
                    let item = arr[0].trim();
                    if item.len() > 0 {
                        id = item.parse::<u64>().unwrap();
                    }
                    let mut num = 0i32;
                    let item = arr[1].trim();
                    if item.len() > 0 {
                        num = item.parse::<i32>().unwrap();
                    }
                    let mut ts = 0u64;
                    let item = arr[if arr.len() == 3 {2} else {3}].trim();
                    if item.len() > 0 {
                        let res = item.parse::<u64>();
                        if res.is_ok() { ts = res.unwrap(); }
                    }
                    let count = if arr.len() == 3 { 0 } else {
                        let item = arr[2].trim();
                        if item.len() < 1 { 0 } else {
                            let res = item.parse::<u64>();
                            if res.is_ok() { res.unwrap() as i32 }
                            else { 0 }
                        }
                    };
                    vec.push((id,num,count,ts));
                }
            }
        }
        Err(e) => {
            println!("{}", e);
            std::process::exit(1);
        }
    }

    if timestamp == 0 {
        for i in 0..vec.len() {
		let ts = vec[i].3;
		if ts > timestamp { timestamp = ts; }
        }
    }
    println!("load: {} @{}", vec.len(), timestamp);
    (vec, timestamp)
}

fn save_pool(pool: &Vec<(u64,i32,i32,u64)>, timestamp: u64) {
    println!("save: {}, @{}", pool.len(), timestamp);
    let mut file = std::fs::File::create(POOL_FILE).unwrap();
    file.write_all(format!(":: timestamp: {}, total: {}\n", timestamp, pool.len()).as_ref()).unwrap();
    for value in pool {
        let res = file.write_all(format!("{},{},{},{}\n", value.0,value.1,value.2,value.3).as_ref());
        match res {
            Err(e) => println!("{}",e),
            Ok(()) => {}
        }
    }
}

async fn get_count(mysql_pool: &mysql_async::Pool, vec: &mut Vec<(u64, i32, i32, u64)>, is_full:bool) {
    let res = mysql_pool.get_conn().await;
    if res.is_err() { println!("{:?}", res.err()); return; }
    let mysql = res.unwrap();
    print!("counting ... "); std::io::stdout().flush().unwrap();

    let mut sql = if is_full {
        format!("select uid,count(0) c from {} where uid in({}", SRC_TABLE, vec[0].0)
    } else {
        format!("select uid,count(0) c from {} where timestamp>now()-interval {} day and uid in({}", SRC_TABLE, super::stream::PULL_DAYS,vec[0].0)
    };
    for i in 1..vec.len() {
        sql += format!(",{}", vec[i].0).as_str();
    }
    sql += ") group by uid";
    let res = mysql.prep_exec(sql, ()).await;
    if res.is_err() { println!("{:?}",res.err()); return; }
    let result = res.unwrap();
    let mut map = std::collections::HashMap::<u64,i32>::new();
    let res = result.map_and_drop(|row| {
        let (id,num):(i64,i64) = mysql_async::from_row(row);
        let en = map.entry(id as u64).or_insert(0);
        *en = num as i32;
    }).await;

    if res.is_err() { println!("{:?}",res.err()); return;}
    for i in 0..vec.len() {
	let id = vec[i].0;
        if map.contains_key(&id) {
           vec[i].2 = *map.get(&id).unwrap();
        }
    }

    println!("{}", map.len());
}

const SRC_TABLE:&str = "relation.contacts";
const PULL_MAX:u32 = 200000;

pub(crate) fn clear_pool() {
    let mut timestamp = 0u64;
    let res = std::fs::File::open(POOL_FILE);
    match res {
        Ok(file) => {
            let reader = std::io::BufReader::new(file);
            for (i, line) in reader.lines().enumerate() {
                let line = line.unwrap();
                if i == 0 && line.starts_with("::") {
                    timestamp = parse_meta(line.as_str());
                    break;
                }
            }
        }
        Err(e) => {
            println!("{}", e);
            std::process::exit(1);
        }
    }

    let res = std::fs::File::create(POOL_FILE);
    match res {
        Ok(mut file) => {
            if timestamp != 0 { file.write_all(format!(":: timestamp: {}\n", timestamp).as_ref()).unwrap() };
        }
        Err(e) => {
            println!("{}", e);
            std::process::exit(4);
        }
    }

}

pub(crate) async fn get_ru_pool(mysql_pool: &mut mysql_async::Pool,is_full: bool) -> Vec<(u64,i32,i32,u64)> {
    let (mut ret_vec, mut timestamp) = load_pool();
    let cond = if timestamp == 0 {
        format!("timestamp>=now()-interval {} day", super::stream::PULL_DAYS)
    } else {
        format!("timestamp>=from_unixtime({})", timestamp)
    };

    let mysql_res = mysql_pool.get_conn().await;
    if mysql_res.is_err() { println!("{:?}", mysql_res.err()); std::process::exit(1) }
    let mysql = mysql_res.unwrap();

    let sql = format!("select uid id,count(0) num,unix_timestamp(max(timestamp)) ts from {}  where {} group by uid order by ts limit {}",SRC_TABLE,cond,PULL_MAX);
    let prep_res = mysql.prep_exec(sql, ()).await;
    if prep_res.is_err() { println!("{:?}",prep_res.err()); std::process::exit(2) }
    let result = prep_res.unwrap();
    let mut vec = Vec::<(u64,i32,i32,u64)>::new();
    let map_res = result.map_and_drop(|row| {
        let (id,num,ts):(i64,i64,i64) = mysql_async::from_row(row);
        //if set.contains(&(id as u64)) { dup += 1; }
        //else { vec.push((id as u64, num as i32, 0i32, ts as u64)); }
        vec.push((id as u64, num as i32, 0i32, ts as u64));
    }).await;

    println!("pull: {}", vec.len());

    if map_res.is_err() {
        println!("{:?}",map_res.err());
        std::process::exit(3);
    }

    drop(map_res);

    if vec.len() > 0 {
	let mut updated = false;
        for i in 0..vec.len() {
            if timestamp < vec[i].3 { updated = true; timestamp = vec[i].3; }
        }
        if ! updated { timestamp += 1; }
        get_count(&mysql_pool, &mut vec, is_full).await;
        merge_pool(&mut ret_vec, &mut vec);
        save_pool(&ret_vec, timestamp);
    }

    ret_vec
}

fn merge_pool(vec: &mut Vec<(u64,i32,i32,u64)>, new: &mut Vec<(u64,i32,i32,u64)>) {
    let mut map = std::collections::HashMap::<u64,(u64,i32,i32,u64)>::new();
    for i in 0..new.len() {
        map.insert(new[i].0, new[i]);
    }
    for i in 0..vec.len() {
        let opt =  map.get(&vec[i].0);
        if opt.is_some() {
            vec[i] = *opt.unwrap();
            map.remove(&vec[i].0);
        }
    }
    for i in 0..new.len() {
        if map.contains_key(&new[i].0) {
            vec.push(new[i]);
        }
    }
    println!("new: {}", map.len());
}

fn _to_timestamp(timestamp: u64) -> String {
    let dur = std::time::UNIX_EPOCH + std::time::Duration::from_secs(timestamp);
    let datetime = chrono::prelude::DateTime::<chrono::prelude::Utc>::from(dur);
    //datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string()
    datetime.format("%H:%M:%S.%3f %h %d").to_string()
}

fn _str_to_time() -> Result<(), ParseError> {
    //let custom = DateTime::parse_from_str("5.8.1994 8:00:07 am +0000", "%d.%m.%Y %H:%M:%S %P %z")?;
    let custom = DateTime::parse_from_str("5.8.1994 18:00:07 +0000", "%d.%m.%Y %H:%M:%S %z")?;
    println!("{}", custom);

    let time_only = NaiveTime::parse_from_str("23:56:04", "%H:%M:%S")?;
    println!("{}", time_only);

    let date_only = NaiveDate::parse_from_str("2015-09-05", "%Y-%m-%d")?;
    println!("{}", date_only);

    let no_timezone = NaiveDateTime::parse_from_str("2015-09-05 23:56:04", "%Y-%m-%d %H:%M:%S")?;
    println!("{}", no_timezone);
    Ok(())
}
