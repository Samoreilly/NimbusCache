use dashmap::DashMap;
use std::hash::Hash;
use std::time::Duration;
use std::time::Instant;
use std::thread::sleep;
use std::sync::Arc;
use std::io::Write;
use std::fs::OpenOptions;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::error::Error;
use std::net::SocketAddr;

static DURATION: Duration = Duration::from_secs(5);
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
    async fn process_socket(&self, mut socket: TcpStream){
        println!("Got connection!");
        let mut stream = socket;
        stream.write_all(b"Hello, world!").await.unwrap();
    }
    async fn accept_connection(self: Arc<Self>, addr: &str) -> Result<(),Box<dyn Error>>  {
        println!("Listening on: {}", addr);
        let listener = TcpListener::bind(addr).await?;

            loop {
                let (socket, client_addr) : (TcpStream, SocketAddr) = listener.accept().await?;
                println!("Got connection from: {}", client_addr);
                let self_clone = self.clone();

                tokio::spawn(async move {
                    self_clone.process_socket(socket).await;//create new thread to process socket
                });
            }


        Ok(())
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

    fn get(&self, key: &K) -> Option<V>{

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


#[tokio::main]
async fn main(){


    let cache = Arc::new(Cache::<String, String>::new());//arcs allows cache to be shared between threads
    cache.clone().clean_lfu();

    let server_handle = tokio::spawn({
        let cache_clone = cache.clone();
        async move {
            cache_clone.accept_connection("127.0.0.1:8080").await.expect("Server failed to start")
        }
    });



    cache.insert("foo".to_string(), "bar".to_string());

    if let Some(val) = cache.get(&"foo".to_string()) {
        println!("Got immediately: {}", val);
    } else {
        println!("Missing immediately!");
    }

    tokio::time::sleep(DURATION + Duration::from_secs(1)).await;

    if let Some(val) = cache.get(&"foo".to_string()) {
        println!("Got after wait: {}", val);
    }
    server_handle.await.unwrap();
}

