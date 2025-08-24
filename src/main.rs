use dashmap::DashMap;
use std::hash::Hash;
use std::time::Duration;
use std::time::Instant;
use std::sync::Arc;
use std::io::Write;
use std::fs::OpenOptions;
use std::net::SocketAddr;
use axum::{routing::get, Extension, Router};
use axum::{
    extract::{Path},
};


static DURATION: Duration = Duration::from_secs(60);
static CAPACITY: usize = 100;


#[derive(Clone)]
pub struct CacheEntry<V> {
    value: V,
    expires_at: Instant,
    frequency: u32,
}
struct Cache<K, V> {
    map: DashMap<K, CacheEntry<V>>, //CacheEntry<V> allows us to store multiple values associated with a key
}

impl<K, V> Cache<K, V> where
    K: Eq + Hash + Clone + std::fmt::Display + std::marker::Send + std::marker::Sync + 'static, // defines the type of key and value
    V: Clone + std::marker::Send + std::marker::Sync + std::fmt::Display + 'static,
{
    fn new () -> Self {
        Cache {
            map: DashMap::new(),
        }
    }

    fn clean_lfu(self: Arc<Self>){
        tokio::spawn(async move{
            loop{
                tokio::time::sleep(Duration::from_secs(10)).await;
                self.lfu();
            }

        });
    }
    fn insert(&self, key: K, value: V){
        let entry = CacheEntry {
            value,
            expires_at: Instant::now() + DURATION,
            frequency: 0,
        };
        if self.map.len() >= CAPACITY {
            self.lfu();
        }
        self.map.insert(key, entry);
    }

    fn get_value(&self, key: &K) -> Option<V>{

        if let Some(mut self_ref) = self.map.get_mut(key) {//gets direct object so it can be modified in place

            if Instant::now() >= self_ref.expires_at {
                let cloned_key = key.clone();
                let entry = self_ref.clone();
                drop(self_ref);//drops the reference/lock so it can be removed from the map

                self.map.remove(key);
                println!("Removed expired key: {}", cloned_key);
                self.write_deleted_keys_to_file(cloned_key, entry);
                return None;
            }
            self_ref.frequency += 1;
            Some(self_ref.value.clone())
        }else{
            None
        }
    }

    fn lfu(&self){

        let mut min_freq = std::u32::MAX;
        let mut min_key = None;

        for kv in self.map.iter() {
            let entry = kv.value();
            if entry.frequency < min_freq {
                min_freq = entry.frequency;
                min_key = Some(kv.key().clone());
            }
        }
        if let Some(key) = min_key {
            self.map.remove(&key);
        }

    }

    fn write_deleted_keys_to_file(&self, key: K, value: CacheEntry<V>){
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open("deleted_keys.txt")
            .unwrap();
        writeln!(file, "Key: {}, Value: {}, Expiry: {:?}, Frequency: {}", key, value.value, value.expires_at.elapsed(), value.frequency).unwrap();
    }
}
fn build_http(cache: Arc<Cache<String, String>>) -> Router {
    Router::new()
        .route("/info/{key}", get(get_value_http))
        .layer(Extension(cache))

}
async fn get_value_http(Path(key): Path<String>, Extension(cache): Extension<Arc<Cache<String, String>>>)
                        -> String {
    cache.get_value(&key).unwrap_or_else(|| "Not found".to_string())

}
#[tokio::main]
async fn main(){


    let cache = Arc::new(Cache::<String, String>::new());//arcs allows cache to be shared between threads
    cache.clone().clean_lfu();

    cache.insert("foo".to_string(), "bar".to_string());
    cache.insert("sam".to_string(), "bar".to_string());

    cache.insert("http".to_string(), "lol".to_string());

    let app = build_http(cache.clone());
    let addr = SocketAddr::from(([127, 0, 0, 1], 5000));
    println!("listening on {}", addr);
    let server_handle = tokio::spawn(async move {
        axum_server::bind(addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });


    if let Some(val) = cache.get_value(&"foo".to_string()) {
        println!("Got immediately: {}", val);
    } else {
        println!("Missing immediately!");
    }

    tokio::time::sleep(DURATION + Duration::from_secs(1)).await;

    if let Some(val) = cache.get_value(&"foo".to_string()) {
        println!("Got after wait: {}", val);
    }
    server_handle.await.unwrap();
}

