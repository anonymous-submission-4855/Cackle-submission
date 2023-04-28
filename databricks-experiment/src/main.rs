use odbc_api::{buffers::TextRowSet, Cursor, Environment, ResultSetMetadata};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::{sleep_until, Instant, Duration};
use tokio::task::spawn_blocking;
use std::collections::HashMap;
use std::sync::{Arc};
use chrono::offset::Local;
use futures::future::join_all;
use std::fs::File;
use std::io::{BufReader, BufRead};
use std::{
    ffi::CStr,
    io::{stdout,stderr, Write},
    path::PathBuf,
};

#[derive(Debug)]
struct QueryExecution {
    start_time : Instant,
    duration : Duration,
    query_id : String,
    is_error : bool,
}


struct QueryRunner{
    env : Environment,
    queries : HashMap::<String, String>,
    executions: Vec::<QueryExecution>,
}

impl QueryRunner {
    fn new() -> Arc::<QueryRunner> {
        let env = Environment::new().unwrap();
        let qmap = Self::parse_queries();
        let n = Arc::new(QueryRunner{
            env: env,
            queries : qmap,
            executions : Vec::new(),
        });
        n
    }

    fn run_query_odbc(self : Arc<QueryRunner>, qid :String, resp : oneshot::Sender<bool>) {
        {
            let conn = self.env.connect_with_connection_string("CONNECTION_STRING_GOES_HERE").unwrap();
            println!("in query {}", qid);
            let query_str = self.queries.get(&qid).unwrap();
            let query_str = "SET use_cached_result = false; ".to_string()+query_str;
            let statements = query_str.split(";");
            // Execute a one of query without any parameters.
            for statement in statements {
                if statement.trim().len() == 0{
                    continue;
                }
                let executed = conn.execute(statement, ());
                let executed =  match executed {
                    Ok(x) => {x},
                    Err(_) => {
                        resp.send(true);
                        return
                    }
                };


                match executed {
                    Some(mut cursor) => {
                        let out  = stderr();
                        let mut writer = csv::Writer::from_writer(out);
                        // Write the column names to stdout
                        let  headline : Result<Vec<String>, _> = cursor.column_names().unwrap().collect::<Result<_,_>>();

                        match headline {
                            Ok(x) => {writer.write_record(x).unwrap()}
                            Err(_) => {},
                        }
                            

                        // Use schema in cursor to initialize a text buffer large enough to hold the largest
                        // possible strings for each column up to an upper limit of 4KiB.
                        let mut buffers = TextRowSet::for_cursor(5000, &mut cursor, Some(4096)).unwrap();
                        // Bind the buffer to the cursor. It is now being filled with every call to fetch.
                        let mut row_set_cursor = cursor.bind_buffer(&mut buffers).unwrap();

                        // Iterate over batches
                        while let Some(batch) = row_set_cursor.fetch().unwrap() {
                            // Within a batch, iterate over every row
                            for row_index in 0..batch.num_rows() {
                                // Within a row iterate over every column
                                let record = (0..batch.num_cols()).map(|col_index| {
                                    batch
                                        .at(col_index, row_index)
                                        .unwrap_or(&[])
                                });
                                // Writes row as csv
                                writer.write_record(record).unwrap();
                            }
                        }
                    }
                    None => {
                    }
                }
            }
        }
        println!("query {} completed", qid);
        resp.send(false);
    }

    async fn run_named_query(self :Arc<QueryRunner>, qid : String) -> QueryExecution{
        let (tx, rx) = oneshot::channel::<bool>();
        let start_time = Instant::now();
        let inner_qid = qid.clone();
        spawn_blocking(move || {self.clone().run_query_odbc(inner_qid.clone(), tx)});
        let resp = rx.await;
        QueryExecution{
            start_time:start_time,
            duration:start_time.elapsed(),
            query_id: qid,
            is_error : resp.unwrap(),
        }
    }


    async fn run_schedule(self: Arc<QueryRunner>,schedule: Vec<(u64, Vec<String>)>) -> Vec<QueryExecution>{
        let start_time = Instant::now();
        let queries_size = schedule.iter().map(|x| x.1.len() as u64).sum::<u64>();
        let mut queries = Vec::with_capacity(queries_size.try_into().unwrap());
        for tup in schedule {
            sleep_until(start_time+Duration::from_secs(tup.0)).await;
            for qname in tup.1{
                println!("starting query {}", qname);
                queries.push(tokio::spawn(self.clone().run_named_query(qname))) ;
            }
        }
        let resp = join_all(queries).await;
        let mut ret = Vec::new();
        for rep in resp{
            match rep {
                Ok(x) => {
                    ret.push(x);
                },
                Err(_) => {},
            }
        }
        ret
    }

    fn parse_queries() -> HashMap<String, String>{
        let mut query_data = HashMap::<String, String>::new(); 
        let dir_path = "./queries/";
        if let Ok(entries) = std::fs::read_dir(dir_path) {
            let mut entries = entries.collect::<Result<Vec<_>, _>>().unwrap();
            for entry in entries {
                let path = entry.path();
                if path.is_file() {
                    let file_name = path.file_stem().unwrap().to_string_lossy().to_string();
                    let file_contents = std::fs::read_to_string(&path).unwrap();
                    query_data.insert(file_name, file_contents);
                }
            }
        }
        query_data
    }

    fn parse_schedule(filename : &str) -> Vec<(u64, Vec<String>)> {
        let file = File::open(filename).unwrap();
        let mut sched = Vec::new();
        let mut curr_time = 0;
        let mut curr_vec = Vec::new();
        for line in BufReader::new(file).lines(){
            let line = line.unwrap();
            let split_line = line.split(",").collect::<Vec<&str>>();
            let time = split_line[0].parse::<u64>().unwrap();
            let query = split_line[1].to_string();
            if time > curr_time {
                if curr_vec.len() > 0 {
                    sched.push((curr_time, curr_vec));
                    curr_vec = Vec::new();
                }
                curr_time = time;
            }
            curr_vec.push(query);
        }
        if curr_vec.len() > 0 {
            sched.push((curr_time, curr_vec));
        }
        sched
    }
}



#[tokio::main]
async fn main() {
    let qr = QueryRunner::new();
    let mut sched = QueryRunner::parse_schedule("test_workload.work");
    println!("{:?}", Local::now());
    let res = qr.run_schedule(sched).await;
    for x in res {
        println!("{:?}", x);
    }
    println!("{:?}", Local::now());
    /*
    for x in 1..23 {
        println!("Query {}", x);
        println!("{:?}", qr.clone().run_named_query(format!("q{}", x)).await);
    }
    */
    //qr.run_named_query("q12".to_string()).await;

}
