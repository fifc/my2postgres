use futures::FutureExt;

async fn hello(greet: &str) -> Result<(),()> {
    world()
        .then(|res| {
            say(greet,res.unwrap())
        }).await
}

async fn world() -> Result<&'static str,()> {
    Ok("world")
}

async fn say(greet: &str, whom: &str) -> Result<(),()> {
    println!("{}, {}!", greet, whom);
    Ok(())
}

pub fn run() {
    let _ = futures::executor::block_on(async {
        futures::join!(
            hello("hello"),
            hello("hi"),
            hello("okay"),
            hello("morning"),
            hello("happy"),
            hello("nice"),
            hello("great"),
            hello("thanks"),
            hello("regards"),
            hello("new"),
            hello("wonderful"),
            hello("lovely")
        )
    });
}
