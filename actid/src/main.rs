#![feature(vec_remove_item)]

mod pool;
mod task;
extern crate db;

fn test_buf() {
    let mut vc: Vec<u8> = vec![0; 1 << 24];
    vc.insert((1 << 24) - 1, 1);
    let vb = &mut vc[..(1 << 24)];
    vb[(1 << 24) - 1] = 1;
    vc[2] = 1;
    use std::alloc::{alloc, dealloc, Layout};
    let layout = Layout::new::<u64>();
    unsafe {
        let ptr = alloc(layout);

        let slice = std::slice::from_raw_parts_mut(ptr as *mut i64, 100);
        let val = 123456789012345678i64;
        slice[2] = val;

        assert_eq!(slice[2], val);
	dealloc(ptr, layout);
	//let _str = String::from_raw_parts(slice.as_mut_ptr() as *mut u8, 20, 100);
    }
}

#[actix_rt::main]
async fn main() {
    db::init();
    let now = std::time::SystemTime::now();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2  || args[1] == "-Q" {
        task::render_all().await;
    } else {
        let mode = &args[1];
        if mode != "worker" {
            println!("invalid argument: {}", mode);
        }
        println!("running worker mode");
        task::render_all_worker().await;
    }

    match now.elapsed() {
        Ok(elapsed) => {
            println!("time elapsed: {}s", elapsed.as_secs());
        }
        _ => {}
    }
}
