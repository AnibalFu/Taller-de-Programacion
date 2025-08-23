//! Este modulo contiene los errores usados internamente al
//! operar en el cluster

use std::io::Error;

use crate::node_id::NodeId;

#[derive(Debug)]
pub struct ClusterError {
    pub error_type: ClusterErrorType,
    pub description: String,
    pub module: String,
}

#[derive(Debug)]
pub enum ClusterErrorType {
    Lock,
    SetNewMaster,
    SendMeetNewMaster,
    SendMessage,
    RequestVote,
    AcceptConnection,
    ClusterValidation,
    AcceptReplica,
    PromotingReplica,
    StartNode,
}

impl ClusterError {
    /// Crea un ClusterError correspondiente al fallo al obtener un lock
    ///
    /// # Parámetros
    /// * `lock`: lock que no se pudo adquirir
    /// * `module`: módulo lógico del flujo donde se produjo el error
    ///
    /// # Retorna
    /// - ClusterError
    pub fn new_lock_error(lock: &'static str, module: &'static str) -> Self {
        let descripcion = format!("(error) ERR error en lock '{lock}'");
        ClusterError {
            error_type: ClusterErrorType::Lock,
            description: descripcion,
            module: module.to_string(),
        }
    }

    /// Crea un ClusterError correspondiente al fallo al actualizar el master
    ///
    /// # Parámetros
    /// * `module`: módulo lógico del flujo donde se produjo el error
    ///
    /// # Retorna
    /// - ClusterError
    pub fn new_set_new_master_error(module: &'static str) -> Self {
        let descripcion = "(error) ERR al actualizar master".to_string();
        ClusterError {
            error_type: ClusterErrorType::SetNewMaster,
            description: descripcion,
            module: module.to_string(),
        }
    }

    /// Crea un ClusterError correspondiente al enviar mensaje de promocion efectuada
    /// a las nuevas réplicas
    ///
    /// # Parámetros
    /// * `module`: módulo lógico del flujo donde se produjo el error
    ///
    /// # Retorna
    /// - ClusterError
    pub fn new_send_meet_new_master_error(module: &'static str) -> Self {
        let descripcion = "(error) ERR al enviar meet new master a replica(s)".to_string();
        ClusterError {
            error_type: ClusterErrorType::SendMeetNewMaster,
            description: descripcion,
            module: module.to_string(),
        }
    }

    /// Crea un ClusterError correspondiente al fallo de envío de un
    /// mensaje a otro nodo
    ///
    /// # Parámetros
    /// * `lock`: el tipo de mensaje que no se pudo enviar
    /// * `module`: módulo lógico del flujo donde se produjo el error
    ///
    /// # Retorna
    /// - ClusterError
    pub fn new_send_message_error(message: &'static str, module: &'static str) -> Self {
        let descripcion = format!("(error) ERR al enviar mensaje '{message}'");
        ClusterError {
            error_type: ClusterErrorType::SendMessage,
            description: descripcion,
            module: module.to_string(),
        }
    }

    /// Crea un ClusterError correspondiente al pedir votos de masters
    /// durante la replica promotion
    ///
    /// # Parámetros
    /// * `module`: módulo lógico del flujo donde se produjo el error
    ///
    /// # Retorna
    /// - ClusterError
    pub fn new_req_vote_error(module: &'static str) -> Self {
        let descripcion = "(error) ERR al pedir votos".to_string();
        ClusterError {
            error_type: ClusterErrorType::RequestVote,
            description: descripcion,
            module: module.to_string(),
        }
    }

    /// Crea un ClusterError correspondiente al fallo al aceptar una conexión
    ///
    /// # Parámetros
    /// * `e`: error obtenido
    /// * `module`: módulo lógico del flujo donde se produjo el error
    ///
    /// # Retorna
    /// - ClusterError
    pub fn new_accept_connection_error(e: Error, module: &'static str) -> Self {
        let descripcion = format!("(error) ERR accept connection '{e}'");
        ClusterError {
            error_type: ClusterErrorType::AcceptConnection,
            description: descripcion,
            module: module.to_string(),
        }
    }

    /// Crea un ClusterError correspondiente al fallo al intentar determinar
    /// el estado del cluster
    ///
    /// # Parámetros
    /// * `e`: error obtenido
    /// * `module`: módulo lógico del flujo donde se produjo el error
    ///
    /// # Retorna
    /// - ClusterError
    pub fn new_cluster_validation_error(e: ClusterError, module: &'static str) -> Self {
        let descripcion = format!("(error) ERR validate cluster state: '{}'", e.description);
        ClusterError {
            error_type: ClusterErrorType::ClusterValidation,
            description: descripcion,
            module: module.to_string(),
        }
    }

    /// Crea un ClusterError correspondiente al aceptar una réplica como propia
    ///
    /// # Parámetros
    /// * `id`: id de la réplica
    /// * `module`: módulo lógico del flujo donde se produjo el error
    ///
    /// # Retorna
    /// - ClusterError
    pub fn new_accept_replica_error(id: &NodeId, module: &'static str) -> Self {
        let descripcion = format!("(error) ERR al aceptar replica: '{id}'");
        ClusterError {
            error_type: ClusterErrorType::AcceptReplica,
            description: descripcion,
            module: module.to_string(),
        }
    }

    /// Crea un ClusterError correspondiente al fallo al promover una réplica
    ///
    /// # Parámetros
    /// * `module`: módulo lógico del flujo donde se produjo el error
    ///
    /// # Retorna
    /// - ClusterError
    pub fn new_promoting_replica_error(module: &'static str) -> Self {
        let descripcion = "(error) ERR al promover replica".to_string();
        ClusterError {
            error_type: ClusterErrorType::PromotingReplica,
            description: descripcion,
            module: module.to_string(),
        }
    }

    /// Crea un ClusterError correspondiente al fallo al iniciar el nodo
    ///
    /// # Parámetros
    /// * `module`: módulo lógico del flujo donde se produjo el error
    ///
    /// # Retorna
    /// - ClusterError
    pub fn new_start_node_error(module: &'static str) -> Self {
        let descripcion = "(error) ERR al iniciar nodo".to_string();
        ClusterError {
            error_type: ClusterErrorType::StartNode,
            description: descripcion,
            module: module.to_string(),
        }
    }
}
