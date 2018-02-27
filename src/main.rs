use std::io::Read;

extern crate yaml_rust;
use yaml_rust::{Yaml, YamlLoader, YamlEmitter};

fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 2 {
        panic!("Must pass config file name");
    }
    let file_name = &args[1];

    let mut file = std::fs::File::open(file_name).expect("\nCould not find config file\n");
    let mut data = String::new();
    file.read_to_string(&mut data).expect("\nFailed to read config file\n");
    let cfg = YamlLoader::load_from_str(&data).expect("\nFailed to parse yaml\n");
    use_mysql(&cfg[0]);
}

fn use_mysql(cfg: &Yaml) {
    let hash = match *cfg {
        Yaml::Hash(ref x) => x,
        _ => panic!("\nConfig top level must be hash containing query and tables\n"),
    };
    let query = &hash[&Yaml::String("query".to_string())];
    let tables = &hash[&Yaml::String("tables".to_string())];
    let query = match *query {
        Yaml::String(ref s) => s,
        _ => panic!("\nQuery must be a String\n"),
    };
    let tables = match *tables {
        Yaml::Array(ref a) => a,
        _ => panic!("\nTables must be an Array\n"),
    };
    println!("{:?}", query);
    println!("{:?}", tables);
}
