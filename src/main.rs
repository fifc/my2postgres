mod cfg;
mod test;
mod my2pg;
mod my2pg_file;

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
        my2pg::run().await;
    } else {
        let filename = &args[1];
        my2pg_file::run(filename).await;
    }
    Ok(())
    //println!("usage: {} [dump-file]", args[0]);
}
