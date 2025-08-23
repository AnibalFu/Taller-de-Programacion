use redis_node::config::config_parser::Config;
use redis_node::node::Node;
use redis_node::persistence::persistencia::restaurar_nodo;
use std::sync::Arc;

/// Punto de entrada del nodo en el clúster Redis.
///
/// Este `main` realiza las siguientes tareas:
///
/// 1. **Carga de argumentos de línea de comandos**:
///    - Requiere que se pase la ruta a un archivo de configuración `redis.conf`.
///    - Si no se proporciona, se imprime un mensaje de error y finaliza la ejecución.
///
/// 2. **Carga del archivo de configuración**:
///    - Usa `Config::from_file(path)` para obtener los parámetros de configuración.
///    - Si ocurre un error durante la carga, se imprime el error y se termina.
///
/// 3. **Inicialización o restauración del nodo**:
///    - Intenta restaurar un nodo previamente existente mediante `restaurar_nodo(...)`.
///    - Si falla la restauración (por ejemplo, en el primer arranque), crea un nuevo nodo maestro
///      utilizando `Node::new_master(...)`, con los datos extraídos del archivo de configuración.
///
/// 4. **Inicio del nodo**:
///    - El nodo se envuelve en un `Arc` para que pueda ser compartido de forma segura entre hilos.
///    - Se llama a `start_node(...)`, que inicia los servicios del nodo:
///         - Módulo AOF (Append Only File).
///         - Módulo RBD (snapshot del estado).
///         - Conexiones con nodos *seed* o configuración como réplica.
///         - Carga de usuarios autenticados.
///
/// 5. **Mensajes informativos**:
///    - Se imprime en consola la dirección en la que el nodo queda escuchando,
///      útil para depuración e información del operador.
///
/// # Uso
/// ```bash
/// ./mi_nodo /ruta/al/redis.conf
/// ```
///
/// # Ejemplo de archivo redis.conf (parcial)
/// ```text
/// address = 127.0.0.1:7000
/// slot_range = 0-5460
/// aof = ./aof.log
/// rbd = ./rbd.snapshot
/// metadata = ./metadata.json
/// ```
fn main() {
    let args = std::env::args().collect::<Vec<String>>();
    if args.len() < 2 {
        eprintln!("Uso: {} /ruta/al/redis.conf", args[0]);
        return;
    }

    let config_path = &args[1];
    let config = match Config::from_file(config_path) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Error al cargar la configuración: {e}");
            return;
        }
    };

    // Crear o restaurar un nodo
    let node = match restaurar_nodo(
        config.get_node_metadata(),
        config.get_node_rbd_path(),
        config.get_node_aof(),
        config.get_node_address(),
    ) {
        Ok(node) => node,
        Err(_e) => Node::new_master(&config),
    };

    println!("Start listening on {:?}", config.get_node_address());
    println!("-----------------------------------");
    let arc_node = Arc::new(node);
    let _ = arc_node.start_node(
        config.get_node_aof(),
        config.get_node_rbd_path(),
        config.get_node_seed(),
        config.get_replica_of(),
        config.get_node_users(),
    );
    println!("-----------------------------------");
}
