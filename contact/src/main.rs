mod u7;
mod cfg;
mod test;
mod stream;
mod stream_time;

/*
#[tokio::main]
async fn main() {
    let f = async { print!("hello world");}.then(|a| {} );
    f.await;
    //let handle = tokio::spawn(f);
    //let res = handle.await;
    //println!("{:?}", res);
}
*/

#[tokio::main]
async fn main() -> Result<(),std::io::Error>{
    loop {
        let args: Vec<String> = std::env::args().collect();
        if args.len() > 1 && args[1] == "time" {
            stream_time::run().await;
            return Ok(());
        }
        if args.len() > 1 && args[1] == "test" {
            test::run_tests();
        }
        break;
    }

    stream::run().await;

    Ok(())
    //println!("usage: {} [dump-file]", args[0]);
}
