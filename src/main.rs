extern crate dotenv;

use dotenv::dotenv;
use std::env;

use tokio::fs;

use chrono::NaiveDateTime;
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> Result<(), ()> {
    // 0. Get the DATABASE_URL environment variable
    dotenv().ok();
    
    let database_host = env::var("DATABASE_HOST").unwrap();
    let database_port = env::var("DATABASE_PORT").unwrap();
    let database_user = env::var("DATABASE_USER").unwrap();
    let database_pass = env::var("DATABASE_PASS").unwrap();
    let database_db = env::var("DATABASE_DB").unwrap();

    // 1. Connect to the database
    let (client, conn) = tokio_postgres::connect(&format!("
        host={database_host} 
        port={database_port}
        user={database_user} 
        password={database_pass}
        dbname={database_db}
    "), NoTls).await.expect("Failed connecting to the database");

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {}", e);
        }
    });

    // 2. Get file name from program arguments & load it
    let file_name: String = env::args().last().expect("Please provide an argument");

    let contents = fs::read(file_name).await.expect("Failed to read file");
    let contents = String::from_utf8(contents).expect("Failed to convert file to string");

    let lines: Vec<&str> = contents.split("\n").collect();

    println!("Loaded file into memory, pushing it to database");

    // 3. Start async processing and putting rows into the database
    for (i , line) in lines.iter().enumerate().skip(1) {
        if i % 1_000 == 0 {
            println!("Finished saving row {i}")
        }

        let cols: Vec<&str> = line.split(",").collect();

        let red = u8::from_str_radix(&cols[2][1..3], 16).unwrap() as i16;
        let green = u8::from_str_radix(&cols[2][3..5], 16).unwrap() as i16;
        let blue = u8::from_str_radix(&cols[2][5..7], 16).unwrap() as i16;

        let x = cols[3].replace("\"", "").parse::<i16>().unwrap();
        let y = cols[4].replace("\"", "").parse::<i16>().unwrap();

        let width = if cols.len() >= 7 { cols[5].parse::<i16>().unwrap() } else { 1 };
        let height = if cols.len() >= 7 { cols[6].replace("\"", "").parse::<i16>().unwrap() } else { 1 };

        match &client.execute(r#"
            INSERT INTO place (time, "user", red, green, blue, x, y, width, height)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9); 
        "#, &[ // 2022-04-03 17:38:20.021 UTC
            &NaiveDateTime::parse_from_str(&cols[0], "%Y-%m-%d %H:%M:%S%.f UTC").unwrap(), 
            &cols[1], 
            &red, &green, &blue, 
            &x, &y, &width, &height
        ]).await {
            Ok(_) => {},
            Err(e) => {
                println!("Failed adding entry into database: {e}");
            }
        }
    }

    Ok(())
}

