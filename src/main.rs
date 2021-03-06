use std::io::Read;

extern crate yaml_rust;
use yaml_rust::{Yaml, YamlLoader, YamlEmitter};
use yaml_rust::yaml::Hash as Doc;

extern crate mysql;

macro_rules! ystring {
    ( $x:expr ) => { Yaml::String($x.to_string()) }
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 2 {
        panic!("\nUsage: <config_file>");
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
    let query = &hash[&ystring!("query")];
    let tables = &hash[&ystring!("tables")];
    let query = match *query {
        Yaml::String(ref s) => s,
        _ => panic!("\nQuery must be a String\n"),
    };
    let tables = match *tables {
        Yaml::Array(ref a) => a,
        _ => panic!("\nTables must be an Array\n"),
    };

    let url = format!("mysql://root@localhost:3306/test");
    let pool = mysql::Pool::new(url).expect("\nCould not open connection to mysql, make sure mysqld is running and has root user\n");
    for ref table in tables {
        populate_table(table, &pool)
    }
    let res = pool.prep_exec(query, ()).unwrap();
    let mut doc: Doc = Doc::new();
    // Insert the query as "sql"
    doc.insert(ystring!("sql"), Yaml::String(format!("'{}'", query)));

    // Insert the expected_names, these are the column names returned.
    doc.insert(ystring!("expected_names"),
        Yaml::String(format!("[{}]",
          res
          .columns_ref()
          .iter()
          .map(|x| format!("'{}'", x.name_str()))
          .fold("".to_string(), |acc, x|
              if acc == "".to_string() { x }
              else { format!("{}, {}", acc, x) }
              )
        )
        )
        );

    // Insert the expected_types, these are the column types returned.
    doc.insert(ystring!("expected_types"),
        Yaml::String(format!("[{}]",
          res
          .columns_ref()
          .iter()
          .map(|x| format!("'{}'", to_test_type(x.column_type())))
          .fold("".to_string(), |acc, x|
              if acc == "".to_string() { x }
              else { format!("{}, {}", acc, x) }
              )
        )
        )
        );

    // Lastly, insert the expected query results. Because we want the leaf
    // nodes formatted as json arrays, we need to serialize them as strings,
    // then remove the double quotes. That is why we use single quotes
    // everywhere for strings. This is super hacky, but it's ok
    // for just generating examples.
    let mut expected: Vec<Yaml> = Vec::new();
    for result_row in res {
        expected.push(
            Yaml::String(format!("[{}]",
            result_row
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|v| {
                use mysql::Value::*;
                match v {
                   NULL => "null".to_string(),
                   Bytes(v) => format!("'{}'", String::from_utf8_lossy(&v).to_string()),
                   Int(i) => i.to_string(),
                   UInt(u) => u.to_string(),
                   Float(f) => f.to_string(),
                   _ => panic!("\nNot handling Date or Time yet, impl later\n"),
                }
            })
            .fold("".to_string(), |acc, x|
               if acc == "".to_string() { x }
               else { format!("{}, {}", acc, x) }
               )
            )
            )
            );
    }
    doc.insert(ystring!("expected"), Yaml::Array(expected));
    let mut out_str = String::new();
    {
        let mut emitter = YamlEmitter::new(&mut out_str);
        emitter.dump(&Yaml::Hash(doc)).unwrap();
    }
    // Remove double quotes, this is super gross.
    println!("{}", out_str.replace("\"", ""));
}

fn populate_table(table: &Yaml, pool: &mysql::Pool) {
    let table = match *table {
        Yaml::Hash(ref h) => h,
        _ => panic!("\nTable definition must be a Map\n")
    };

    let name = &table[&ystring!("name")];
    let name = match *name {
        Yaml::String(ref s) => s,
        _ => panic!("\nTable name must be a String\n"),
    };
    let data = &table[&ystring!("data")];
    let data = match *data {
        Yaml::Array(ref a) => a,
        _ => panic!("\nTable data must be an Array\n"),
    };
    if data.len() < 2 {
        panic!("\nTable data must at least include column names as the first row and one row of data for the types.\n Create your example manually for empty tables.\n");
    }
    let headers = &data[0];
    let headers = match *headers {
        Yaml::Array(ref a) => a,
        _ => panic!("\nTable headers must be an Array\n"),
    };
    let column_names = headers
        .iter()
        .map(|x|
             {
                    match *x {
                        Yaml::String(ref s) => s.clone(),
                        _ => panic!("\nHeader must be a String\n"),
                    }
             }
        )
        .collect::<Vec<_>>();
    let column_types = get_column_types(&data[1]);

    let create_cols = column_names
        .into_iter()
        .zip(column_types)
        .map(|(n, t)| format!("{} {}", n, t))
        .fold("".to_string(), |acc, x|
              if acc == "".to_string() { x }
              else { format!("{}, {}", acc, x) }
              );
    pool.prep_exec(format!("DROP TABLE IF EXISTS {}", name), ())
        .expect(&format!("\nFailed to drop table {}\n", name));
    let create_stmt = format!(r"CREATE TABLE {} ({})", name, create_cols);
    pool.prep_exec(create_stmt, ())
        .expect(&format!("\nFailed to create table {}\n", name));
    for row in data.into_iter().skip(1) {
        let fmt_data = format_row_data(row);
        pool.prep_exec(format!(r"INSERT INTO {} VALUES({})", name, fmt_data), ()).expect(&format!("\nFailed to insert row: {}\n", fmt_data));
    }
    ()
}

fn format_row_data(row: &Yaml) -> String {
    let row = match *row {
        Yaml::Array(ref a) => a,
        _ => panic!("\nTable row must be an Array\n"),
    };
    row
    .into_iter()
    .map(|x| {
        use Yaml::*;
        match *x {
            Real(ref r) => format!("{}", r),
            Integer(ref i) => format!("{}", i),
            String(ref s) => format!("'{}'", s),
            Boolean(ref b) => format!("{}", b),
            Array(_) => panic!("\nData value cannot be Array\n"),
            Hash(_) => panic!("\nData value cannot be Map\n"),
            Alias(_) => panic!("\nData value cannot be an Alias\n"),
            Null => "NULL".to_string(),
            BadValue => panic!("\nBad value in data\n"),
        }
    })
    .fold("".to_string(), |acc, x|
              if acc == "".to_string() { x }
              else { format!("{}, {}", acc, x) }
              )
}

fn get_column_types(row: &Yaml) -> Vec<String> {
    let row = match *row {
        Yaml::Array(ref a) => a,
        _ => panic!("\nRows in table data must be an Arrays\n"),
    };
    row.iter().map(|x| {
        use Yaml::*;
        match *x {
            Real(_) => "double",
            Integer(_) => "int",
            String(_) => "text",
            Boolean(_) => "tinyint(1)",
            Array(_) => panic!("\nData value cannot be Array\n"),
            Hash(_) => panic!("\nData value cannot be Map\n"),
            Alias(_) => panic!("\nData value cannot be an Alias\n"),
            Null => panic!("\nPlease do not use NULL as the first value in a column\n"),
            BadValue => panic!("\nBad value in data\n"),
        }.to_string()
    }).collect()
}

fn to_test_type(ty: mysql::consts::ColumnType) -> String {
    use mysql::consts::ColumnType::*;
    match ty {
    MYSQL_TYPE_DECIMAL => "float64",
    MYSQL_TYPE_TINY => "int",
    MYSQL_TYPE_SHORT => "int",
    MYSQL_TYPE_LONG => "int",
    MYSQL_TYPE_FLOAT => "float64",
    MYSQL_TYPE_DOUBLE => "float64",
    MYSQL_TYPE_NULL => "string",
    MYSQL_TYPE_TIMESTAMP => "string",
    MYSQL_TYPE_LONGLONG => "int",
    MYSQL_TYPE_INT24 => "int",
    MYSQL_TYPE_DATE => "string",
    MYSQL_TYPE_TIME => "string",
    MYSQL_TYPE_DATETIME => "string",
    MYSQL_TYPE_YEAR => "string",
    MYSQL_TYPE_NEWDATE => "string",
    MYSQL_TYPE_VARCHAR => "string",
    MYSQL_TYPE_BIT => "int",
    MYSQL_TYPE_TIMESTAMP2 => "string",
    MYSQL_TYPE_DATETIME2 => "string",
    MYSQL_TYPE_TIME2 => "string",
    MYSQL_TYPE_JSON => "string",
    MYSQL_TYPE_NEWDECIMAL => "float64",
    MYSQL_TYPE_ENUM => "string",
    MYSQL_TYPE_SET => "string",
    MYSQL_TYPE_TINY_BLOB => "string",
    MYSQL_TYPE_MEDIUM_BLOB => "string",
    MYSQL_TYPE_LONG_BLOB => "string",
    MYSQL_TYPE_BLOB => "string",
    MYSQL_TYPE_VAR_STRING => "string",
    MYSQL_TYPE_STRING => "string",
    MYSQL_TYPE_GEOMETRY => "string",
    }.to_string()
}
