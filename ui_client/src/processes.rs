use crate::{
    redis_command_process::RedisCommandProcess,
    redis_ia_subscribe_process::RedisSubscribeProcessIA,
    redis_subscribe_process::RedisSubscribeProcess, ui_error::UIResult,
};

#[derive(Debug, Default)]
pub struct Processes {
    pub self_updates_process: Option<RedisSubscribeProcess>,
    pub file_updates_process: Option<RedisSubscribeProcess>,
    pub commands_process: Option<RedisCommandProcess>,
    pub ia_llm_process: Option<RedisSubscribeProcessIA>,
}

impl Processes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn start_commands_process(
        &mut self,
        user: &str,
        password: &str,
        redis_host: &str,
        redis_port: u16,
    ) -> UIResult<()> {
        let process = RedisCommandProcess::new(user, password, redis_host, redis_port)?;
        self.commands_process = Some(process);
        Ok(())
    }

    pub fn start_self_updates_process(
        &mut self,
        user: &str,
        password: &str,
        redis_host: &str,
        redis_port: u16,
    ) {
        let channel_name = format!("users:{user}");
        let process = RedisSubscribeProcess::new(
            &channel_name,
            user.to_string(),
            password.to_string(),
            redis_host.to_string(),
            redis_port,
        );
        if let Ok(process) = process{
            self.self_updates_process = Some(process);
        }
    }

    pub fn start_file_updates_process(
        &mut self,
        channel_id: &str,
        user: &str,
        password: &str,
        redis_host: &str,
        redis_port: u16,
    ) {
        let process = RedisSubscribeProcess::new(
            channel_id,
            user.to_string(),
            password.to_string(),
            redis_host.to_string(),
            redis_port,
        );
        if let Ok(process) = process{
            self.file_updates_process = Some(process);
        }
    }

    pub fn start_ia_response_process(
        &mut self,
        user: &str,
        password: &str,
        host: &str,
        port: u16,
        channel_name: String,
    ) {
        // me subscribo al de la respuesta de la IA
        // el canal es llm:req_id
        let channel = format!("llm:{channel_name}");
        let process = RedisSubscribeProcessIA::new(
            &channel,
            user.to_string(),
            password.to_string(),
            host.to_string(),
            port,
        );
        if let Ok(process) = process{
            self.ia_llm_process = Some(process);
        }
    }
}
