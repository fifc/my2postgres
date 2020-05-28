use serde::Deserialize;
use std::sync::{Arc, Mutex};

const STORIES_URL: &str = "https://hacker-neww.firebaseio.com/v0/topstories.json";
const ITEM_URL_BASE: &str = "https://hacker-neww.firebaseio.com/v0/item";

#[derive(Deserialize)]
struct Story {
    title: String
}
pub fn run() {
    //let story_ids = futures::executor::block_on(async {
    //    let body = reqwest::get(STORIES_URL).await.unwrap().text().await.unwrap();
    //    let res: Arc<Vec<u64>> = Arc::new(serde_json::from_str(&body).unwrap());
    //    res
    //});
    let story_ids: Arc<Vec<u64>> = Arc::new(serde_json::from_str(&reqwest::blocking::get(STORIES_URL).unwrap().text().unwrap()).unwrap());
    print!("{:?}", story_ids);

    let mut handles = Vec::new();
    let cursor = Arc::new(Mutex::new(0));

    for tid in 0..8 {
        let cursor = cursor.clone();
        let story_ids = story_ids.clone();
        handles.push(std::thread::spawn(move || loop {
            let index = {
                let mut cursor_guard = cursor.lock().unwrap();
                let index = *cursor_guard;
                if index >= story_ids.len() {
                    return;
                }
                *cursor_guard += 1;
                index
            };
            let story_url = format!("{}/{}", ITEM_URL_BASE, story_ids[index]);
            let story:Story = serde_json::from_str(&reqwest::blocking::get(&story_url).unwrap().text().unwrap()).unwrap();
            println!("{:?}", story.title);
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
}
