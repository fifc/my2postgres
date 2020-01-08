mod my2pg;

static DEBUG: i32 = 1;

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
        if DEBUG == 0 {
            dealloc(ptr, layout);
        } else {
            let _str = String::from_raw_parts(slice.as_mut_ptr() as *mut u8, 20, 100);
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("usage: {} mysql-dump-file", args[0]);
        return;
    }
    let filename = &args[1];
    test_buf();

    my2pg::run(filename, &"host=192.168.56.107 user=y dbname=im".to_string());
}
