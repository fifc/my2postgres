extern crate yaml_rust;
use std::io::Read;

pub static DEBUG: i32 = 1;

pub struct DatabaseConfig {
    pub host_: String,
    pub ip_: String,
    pub port_: u16,
    pub db_: String,
    pub user_: String,
    pub passwd_: String
}

pub struct MyConfig {
    pub mysql: DatabaseConfig,
    pub postgres: DatabaseConfig
}

pub static mut MY_CONFIG: Option<MyConfig> = None;

pub fn init(filename: &str) {
    let file = std::fs::File::open(filename).unwrap();
    let mut reader = std::io::BufReader::new(file);
    let mut tmp_str = String::new();
    reader.read_to_string(&mut tmp_str).unwrap();
    let doc = yaml_rust::YamlLoader::load_from_str(tmp_str.as_str()).unwrap();
    let mut my_config = MyConfig {
        mysql: DatabaseConfig {
            host_: "localhost".to_string(),
            ip_: "".to_string(),
            port_: 0,
            db_: "".to_string(),
            user_: "root".to_string(),
            passwd_: "".to_string()
        },
        postgres: DatabaseConfig{
            host_: "localhost".to_string(),
            ip_: "".to_string(),
            port_: 0,
            db_: "".to_string(),
            user_: "root".to_string(),
            passwd_: "".to_string()
        },
    };

    let mut config = &doc[0]["mysql"];
    if ! config.is_badvalue() {
        let mut opt = &config["host"];
        if ! opt.is_badvalue() {
            my_config.mysql.host_ = opt.as_str().unwrap().to_string();
        }
        opt = &config["ip"];
        if ! opt.is_badvalue() {
            my_config.mysql.ip_ = opt.as_str().unwrap().to_string();
        }
        opt = &config["port"];
        if ! opt.is_badvalue() {
            my_config.mysql.port_ = opt.as_i64().unwrap() as u16;
        }
        opt = &config["db"];
        if ! opt.is_badvalue() {
            my_config.mysql.db_ = opt.as_str().unwrap().to_string();
        }
        opt = &config["user"];
        if ! opt.is_badvalue() {
            my_config.mysql.user_ = opt.as_str().unwrap().to_string();
        }
        opt = &config["passwd"];
        if ! opt.is_badvalue() {
            my_config.mysql.passwd_ = opt.as_str().unwrap().to_string();
        }
    }


    config = &doc[0]["postgres"];
    if ! config.is_badvalue() {
        let mut opt = &config["host"];
        if ! opt.is_badvalue() {
            my_config.postgres.host_ = opt.as_str().unwrap().to_string();
        }
        opt = &config["ip"];
        if ! opt.is_badvalue() {
            my_config.postgres.ip_ = opt.as_str().unwrap().to_string();
        }
        opt = &config["port"];
        if ! opt.is_badvalue() {
            my_config.postgres.port_ = opt.as_i64().unwrap() as u16;
        }
        opt = &config["db"];
        if ! opt.is_badvalue() {
            my_config.postgres.db_ = opt.as_str().unwrap().to_string();
        }
        opt = &config["user"];
        if ! opt.is_badvalue() {
            my_config.postgres.user_ = opt.as_str().unwrap().to_string();
        }
        opt = &config["passwd"];
        if ! opt.is_badvalue() {
            my_config.postgres.passwd_ = opt.as_str().unwrap().to_string();
        }
    }

    unsafe {MY_CONFIG = Some(my_config)}

    unsafe {
        match &MY_CONFIG {
            Some(_config) => {
                //println!("mysql config: {}:{}@{}[{}]:{}", config.user_,config.passwd_,config.host_,config.ip_,config.port_);
            }
            //_ => { panic("error load config") }
            _ => {}
        }
    }
}