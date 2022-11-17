use tokio::signal;
use tracing:: { info, error, subscriber::SetGlobalDefaultError };
use tracing_subscriber::EnvFilter;
use unibus::rabbit;

#[tokio::main]
async fn main() -> Result<(), SetGlobalDefaultError> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info,lapin=off,unibus=trace");
    }
    let fmt = tracing_subscriber::fmt::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_file(false)
        .finish();

    tracing::subscriber::set_global_default(fmt)?;

    info!("started");
    let rabbit_client = rabbit::start().await;


    
    let addr = std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f" .into());

    let options = unibus::rabbit::ConnectionOptions::new(&addr, "main");
    let con = rabbit_client.connect(options).await.unwrap();
    
    let mut watcher = con.state_watcher().await.unwrap();

    info!(
        "current connection state: {:?}",
        *watcher.borrow_and_update()
    );
    tokio::spawn(async move {
        while let Ok(_) = watcher.changed().await {
            info!("current connection state: {:?}", *watcher.borrow());
        }
    });
    


    
    match signal::ctrl_c().await {
        Ok(()) => {
            info!("shutting down");
        }
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
        }
    };
    Ok(())
}
