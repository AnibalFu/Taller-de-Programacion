use crate::llm_error::LLMError;
use crate::microservice::RealService;
use std::sync::Arc;

mod call_llm;
mod configuration;
mod constantes;
mod llm_error;
mod microservice;
mod redis_connect;

pub const CHANNEL_REQUEST: &str = "llm:request";
pub const CHANNEL_GLOBAL_REQUEST: &str = "documents:utils"; // "canal_de_prueba_es_el_que_nos_lleva_al_microservicio_de_control";
pub const LOGGER_PATH: &str = "llm_logger.log";
pub const WORKER_AMOUNT: usize = 6;

fn main() -> Result<(), LLMError> {
    let args = std::env::args().collect::<Vec<String>>();

    let config = configuration::Configuration::from_args(&args);

    let micro_llm = RealService::new(
        WORKER_AMOUNT,
        CHANNEL_REQUEST.into(),
        CHANNEL_GLOBAL_REQUEST.into(),
        config.redis_addr,
        LOGGER_PATH,
        config.api_key.clone(),
    )
    .unwrap();

    let backend_addr = config.redis_addr.to_string();
    println!("Init llm microservice on {backend_addr}");
    Arc::new(micro_llm).run()?;
    println!("Llm status = {:?}", ());
    Ok(())
}
