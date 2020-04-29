mod cfg;
mod test;
mod my2pg;
mod stream;

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
    test::run_tests();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        stream::run().await;
    } else if args[1] == "+mn" {
        stream::run_sp().await;
    } else {
        let filename = &args[1];
        my2pg::run(filename).await;
    }
    Ok(())
    //println!("usage: {} [dump-file]", args[0]);
}
