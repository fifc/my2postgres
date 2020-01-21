extern crate yaml_rust;
use std::io::Read;

pub static DEBUG: i32 = 1;

pub struct MySQLConfig {
    pub host_: String,
    pub port_: u16,
    pub user_: String,
    pub passwd_: String
}

pub static mut MYSQL_CONFIG: Option<MySQLConfig> = None;

pub fn init(filename: &str) {
    let file = std::fs::File::open(filename).unwrap();
    let mut reader = std::io::BufReader::new(file);
    let mut tmp_str = String::new();
    reader.read_to_string(&mut tmp_str).unwrap();
    let doc = yaml_rust::YamlLoader::load_from_str(tmp_str.as_str()).unwrap();
    let mut mysql_config = MySQLConfig{
        host_: "localhost".parse().unwrap(),
        port_:3306,
        user_: "root".parse().unwrap(),
        passwd_:"".parse().unwrap()
    };

    let mysql = &doc[0]["mysql"];
    if ! mysql.is_badvalue() {
        let mut opt = &mysql["host"];
        if ! opt.is_badvalue() {
            mysql_config.host_ = opt.as_str().unwrap().to_string();
        }
        opt = &mysql["port"];
        if ! opt.is_badvalue() {
            mysql_config.port_ = opt.as_i64().unwrap() as u16;
        }
        opt = &mysql["user"];
        if ! opt.is_badvalue() {
            mysql_config.user_ = opt.as_str().unwrap().to_string();
        }
        opt = &mysql["passwd"];
        if ! opt.is_badvalue() {
            mysql_config.passwd_ = opt.as_str().unwrap().to_string();
        }
    }

    unsafe {MYSQL_CONFIG = Some(mysql_config)}

    unsafe {
        match &MYSQL_CONFIG {
            Some(config) => {
                println!("mysql config: {}:{}@{}:{}", config.user_,config.passwd_,config.host_,config.port_);
            }
            //_ => { panic("error load config") }
            _ => {}
        }
    }
}