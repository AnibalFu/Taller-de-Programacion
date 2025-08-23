//! Este módulo contiene las funciones de procesamiento de comandos del nodo

use super::{
    const_cmd::{
        CMD_GET, CMD_LINDEX, CMD_LLEN, CMD_LRANGE, CMD_PUBLISH, CMD_SISMEMBER, CMD_SMEMBERS,
        CMD_STRLEN,
    },
    pub_sub_struct::{BrokerCommand, PubSubBroker},
    utils::{do_handshake, leer_comando, send_msj, send_msj_to_logger},
};
use crate::comandos::const_cmd::CMD_SPUBLISH;
use crate::internal_protocol::moved::Moved;
use crate::{
    client_struct::client::Client,
    internal_protocol::redis_cmd::RedisCMD,
    log_msj::log_mensajes::log_cli_send_cmd_info,
    node_role::NodeRole,
    persistence::persistencia::guardar_operacion,
    utils::utils_functions::{obtener_fn_normal, obtener_stream},
};
use crate::{
    cluster::node_message::{InnerMensajeNode, TipoMensajeNode},
    node::Node,
};
use redis_client::tipos_datos::{simple_error::SimpleError, traits::DatoRedis};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    sync::{Arc, RwLock, atomic::Ordering, mpsc::Sender},
};

impl Node {
    /// Procesa los comandos de un cliente mientras los haya
    ///
    /// # Parametros
    /// * `client`: cliente cuya conexion se administra
    /// * `storage`: storage del nodo
    /// * `channels`: canales del nodo
    /// * `path`: path al aofile de persistencia
    /// * `logger`: logger donde enviar mensajes
    pub(crate) fn procesar_comandos(
        &self,
        client: Arc<RwLock<Client>>,
        aof_file: Option<Arc<RwLock<File>>>,
        tx_connect: Sender<TipoMensajeNode>,
        users: HashMap<String, String>,
    ) {
        let mut reader = match obtener_stream(&client, &self.logger) {
            Some(stream) => stream,
            None => return,
        };

        while let Some(comando_tokens) = leer_comando(&mut reader) {
            self.procesar_un_comando(
                &aof_file,
                &comando_tokens,
                &client,
                tx_connect.clone(),
                &users,
            );
        }
    }

    /// Procesa un comando de un cliente
    ///
    /// # Parametros
    /// * `handshake_functions`: diccionario de funciones de handshake
    /// * `aofile`: file de persistencia
    /// * `comando_tokens`: secuencia de tokens que conforman unb comando de redis
    /// * `client`: cliente cuya comando se procesa
    /// * `storage`: storage del nodo
    /// * `logger`: logger donde enviar mensajes
    /// * `channels`: canales del nodo
    fn procesar_un_comando(
        &self,
        aofile: &Option<Arc<RwLock<File>>>,
        comando_tokens: &[String],
        client: &Arc<RwLock<Client>>,
        tx_connect: Sender<TipoMensajeNode>,
        users: &HashMap<String, String>,
    ) {
        if self.verificar_y_enviar_fail(client) {
            return;
        }

        if comando_tokens.is_empty() {
            send_msj_to_logger(client, &self.logger);
            return;
        }

        if !self.handshake_realizado(client) {
            do_handshake(client, comando_tokens, &self.logger, users);
            return;
        }

        self.log_comando_cliente(client, comando_tokens);

        if self.procesar_pubsub_y_reenviar_si_es_publish(comando_tokens, client, &tx_connect) {
            return;
        }

        if self.cliente_en_modo_pubsub(client) {
            return;
        }

        let comando = comando_tokens[0].to_uppercase();
        self.procesar_comando_general(&comando, comando_tokens, client, aofile, tx_connect);
    }

    fn handshake_realizado(&self, client: &Arc<RwLock<Client>>) -> bool {
        client.read().map(|c| c.get_handshake()).unwrap_or(false)
    }

    fn cliente_en_modo_pubsub(&self, client: &Arc<RwLock<Client>>) -> bool {
        client.read().map(|c| c.get_modo_pub_sub()).unwrap_or(false)
    }

    fn log_comando_cliente(&self, client: &Arc<RwLock<Client>>, comando_tokens: &[String]) {
        if let Ok(cli) = client.read() {
            log_cli_send_cmd_info(&self.logger, cli, comando_tokens);
        }
    }

    fn procesar_pubsub_y_reenviar_si_es_publish(
        &self,
        comando_tokens: &[String],
        client: &Arc<RwLock<Client>>,
        tx_connect: &Sender<TipoMensajeNode>,
    ) -> bool {
        if !self.procesar_pub_sub(comando_tokens, client, tx_connect.clone()) {
            return false;
        }

        if let Some(cmd) = comando_tokens.first().map(|s| s.to_uppercase()) {
            if cmd == CMD_PUBLISH || cmd == CMD_SPUBLISH {
                let mensaje = InnerMensajeNode::PubSub(RedisCMD::new(comando_tokens.to_vec()));
                let _ = tx_connect.send(TipoMensajeNode::InnerNode(mensaje));
            }
        }

        true
    }

    /// Procesa y llama a la ejecucion de un comando de tipo pub/sub
    ///
    /// # Parametros
    /// * `comando`: nombre del comando
    /// * `tokens`: secuencia de strings que conforman el comando
    /// * `channels`: canales del nodo
    /// * `client`: cliente que escribe el comando
    /// * `logger`: logger donde enviar mensajes
    ///
    /// # Retorna
    /// - Verdadero si se encuentra el comando, falso en otro caso
    fn procesar_pub_sub(
        &self,
        tokens: &[String],
        client: &Arc<RwLock<Client>>,
        tx_connect: Sender<TipoMensajeNode>,
    ) -> bool {
        let comando = tokens[0].to_uppercase();
        if !PubSubBroker::es_comando_pub_sub(&comando) {
            return false;
        }
        let dummy =
            BrokerCommand::new_client_cmd(tokens.to_vec(), client.clone(), Some(tx_connect));
        self.pub_sub.send_cmd(dummy);

        true
    }

    /// Procesa y ejecuta un comando de tipo general, escribe en el logger
    /// los mensajes pertinentes
    ///
    /// # Parametros
    /// * `comando`: nombre del comando
    /// * `tokens`: secuencia de strings que conforman el comando
    /// * `client`: cliente que escribe el comando
    /// * `storage`: almacenamiento del nodo
    /// * `aofile`
    /// * `logger`: logger donde enviar mensajes
    /// * `rol`: rol del nodo
    fn procesar_comando_general(
        &self,
        comando: &str,
        tokens: &[String],
        client: &Arc<RwLock<Client>>,
        aofile: &Option<Arc<RwLock<File>>>,
        tx_connect: Sender<TipoMensajeNode>,
    ) {
        let logger = &self.logger;
        if *self.role.read().unwrap() == NodeRole::Replica
            && !es_operacion_no_mutable(&comando.to_uppercase())
        {
            send_msj(
                client.clone(),
                DatoRedis::SimpleError(SimpleError::new(
                    "INVALID".to_string(),
                    "invalid command for redis replica".to_string(),
                )),
                logger,
            );
            return;
        }
        match obtener_fn_normal(comando) {
            Some(funcion) => match funcion(tokens, &self.storage) {
                Ok(respuesta) => {
                    send_msj(client.clone(), respuesta, logger);

                    if let Some(aofile) = aofile {
                        guardar_operacion(aofile, tokens.to_vec())
                            .map_err(|e| logger.error(&e.to_string(), "AOF"))
                            .ok();

                        logger.info(&format!("Operación {tokens:?} guardada en AOF"), "AOF");
                    }

                    self.actualizar_replication_offset(&comando.to_uppercase());
                    let msj = InnerMensajeNode::RedisCommand(RedisCMD::new(tokens.to_vec()));
                    let _ = tx_connect.send(TipoMensajeNode::InnerNode(msj));
                }
                Err(e) => {
                    if let DatoRedis::MovedError(e) = e.clone() {
                        let moved = Moved::new(e.get_slot(), client.clone());
                        let msj = InnerMensajeNode::Moved(moved);
                        let _ = tx_connect.send(TipoMensajeNode::InnerNode(msj));
                    } else {
                        send_msj(client.clone(), e, logger);
                    }
                }
            },
            None => {
                send_msj(client.clone(), DatoRedis::new_null(), logger);
            }
        }
    }

    /// Procesa y ejecuta un comando de tipo general, escribe en el logger
    /// los mensajes pertinentes para una réplica
    ///
    /// # Parametros
    /// * `comando`: nombre del comando
    /// * `tokens`: secuencia de strings que conforman el comando
    /// * `storage`: almacenamiento del nodo
    /// * `aofile`
    /// * `logger`: logger donde enviar mensajes
    pub fn ejecutar_comando_general_replica(
        &self,
        comando: &str,
        tokens: &[String],
        aofile: &Option<Arc<RwLock<File>>>,
    ) {
        if let Some(funcion) = obtener_fn_normal(&comando.to_uppercase()) {
            if funcion(tokens, &self.storage).is_ok() {
                if let Some(aof) = aofile {
                    match guardar_operacion(aof, tokens.to_vec()) {
                        Ok(_) => self
                            .logger
                            .info(&format!("Operación {tokens:?} guardada en AOF"), "AOF"),
                        Err(e) => self.logger.error(&e.to_string(), "AOF"),
                    }
                }
            }
        }
    }

    pub fn actualizar_replication_offset(&self, comando: &String) {
        if !es_operacion_no_mutable(comando) {
            self.replication_offset.fetch_add(1, Ordering::SeqCst);
        }
    }
}

fn es_operacion_no_mutable(cmd: &String) -> bool {
    let operaciones_no_mutables = HashSet::from([
        CMD_LRANGE.to_string(),
        CMD_LINDEX.to_string(),
        CMD_LLEN.to_string(),
        CMD_GET.to_string(),
        CMD_STRLEN.to_string(),
        CMD_SISMEMBER.to_string(),
        CMD_SMEMBERS.to_string(),
    ]);

    operaciones_no_mutables.contains(cmd)
}
