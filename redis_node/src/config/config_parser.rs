//! Este módulo contiene la estructura que parsea los datos de configuración
//! a partir del archivo .conf
use std::collections::HashMap;
use std::fs;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::ops::Range;

/// Estructura para almacenar la configuración del nodo
#[derive(Debug)]
pub struct Config {
    address: SocketAddr,
    cluster_address: SocketAddr,
    public_address: SocketAddr,
    slot_range: Range<u16>,
    max_clients: usize,

    aof_file: String,
    metadata_file: String,
    storage_file: String,
    log_file: String,
    users: HashMap<String, String>,

    appendonly: bool,
    save_interval_ms: u64,

    node_seed: Option<SocketAddr>,
    replicaof: Option<SocketAddr>,
    node_timeout: u64,
}

impl Config {
    /// Crea una nueva instancia de Config a partir de un archivo de configuración
    ///     Recibe:
    /// - `path`: Ruta al archivo de configuración
    ///   Retorna:
    /// - `Ok(Config)`: Si la configuración se carga correctamente
    pub fn from_file(path: &str) -> Result<Self, String> {
        let lines = Self::read_lines_from_file(path)?;
        let map = Self::parse_key_value_lines(lines, '=', "config")?;

        let address = Self::get_address(&map)?;
        let public_address = Self::get_socket_addr(&map, "public_address")?.unwrap_or(address); // Si no se especifica, usamos la misma dirección del nodo
        let (appendonly, aof_file) = Self::get_append_only(&map)?;
        let cluster_address = Self::parse_cluster_address(&map)?;

        Ok(Config {
            address,
            cluster_address,
            public_address,
            slot_range: Self::get_slot_range(&map)?,
            max_clients: Self::get_max_clients(&map)?,
            aof_file,
            metadata_file: Self::get(&map, "metadata_file")?,
            storage_file: Self::get(&map, "storage_file")?,
            log_file: Self::get(&map, "log_file")?,
            users: Self::get_users(&map)?,
            appendonly,
            save_interval_ms: Self::get_save_info(&map)?,
            node_seed: Self::get_socket_addr(&map, "node_id_seed")?,
            replicaof: Self::get_socket_addr(&map, "replicaof")?,
            node_timeout: Self::get_node_timeout(&map)?,
        })
    }

    // funciones publicas :: getters

    pub fn get_node_address(&self) -> SocketAddr {
        self.address
    }

    pub fn get_node_slot_range(&self) -> Range<u16> {
        self.slot_range.clone()
    }

    pub fn get_node_max_clients(&self) -> usize {
        self.max_clients
    }

    pub fn get_node_aof(&self) -> (bool, String) {
        (self.appendonly, self.aof_file.to_string())
    }

    pub fn get_node_metadata(&self) -> String {
        self.metadata_file.to_string()
    }

    pub fn get_node_rbd_path(&self) -> String {
        self.storage_file.to_string()
    }

    pub fn get_node_log_file(&self) -> String {
        self.log_file.to_string()
    }

    pub fn get_node_save_interval(&self) -> u64 {
        self.save_interval_ms
    }

    pub fn get_node_seed(&self) -> Option<SocketAddr> {
        self.node_seed
    }

    pub fn get_replica_of(&self) -> Option<SocketAddr> {
        self.replicaof
    }

    pub fn get_node_users(&self) -> HashMap<String, String> {
        self.users.clone()
    }

    pub fn get_node_time_out(&self) -> u64 {
        self.node_timeout
    }

    pub fn get_cluster_address(&self) -> SocketAddr {
        self.cluster_address
    }

    pub fn get_public_address(&self) -> SocketAddr {
        self.public_address
    }

    // Funciones privadas para el manejo de la configuración

    /// Convierte un vector de líneas en un HashMap
    fn parse_key_value_lines(
        lines: Vec<String>,
        delimiter: char,
        context: &str,
    ) -> Result<HashMap<String, String>, String> {
        let mut map = HashMap::new();
        for line in lines {
            if let Some((key, value)) = line.split_once(delimiter) {
                map.insert(key.trim().to_string(), value.trim().to_string());
            } else {
                return Err(format!("Línea mal formada en '{context}': {line}"));
            }
        }
        Ok(map)
    }

    /// Lee un archivo de configuración y devuelve un vector de líneas
    fn read_lines_from_file(path: &str) -> Result<Vec<String>, String> {
        let contents = fs::read_to_string(path)
            .map_err(|e| format!("Error leyendo archivo de configuración '{path:?}': {e}"))?;
        let lines: Vec<String> = contents
            .lines()
            .filter(|line| !line.trim().is_empty() && !line.trim_start().starts_with('#'))
            .map(|line| line.to_string())
            .collect();
        Ok(lines)
    }

    // Funcion de acceso a los campos del HashMap
    fn get(map: &HashMap<String, String>, key: &str) -> Result<String, String> {
        map.get(key)
            .cloned()
            .ok_or_else(|| format!("Falta el campo obligatorio en el config: '{key}'"))
    }

    /// Funcion para parsear los valores de la configuración
    fn parse_u16(map: &HashMap<String, String>, key: &str) -> Result<u16, String> {
        let value_str = Self::get(map, key)?;
        value_str
            .parse::<u16>()
            .map_err(|_| format!("No se pudo parsear '{key}' (\"{value_str}\") como u16"))
    }

    /// Función para obtener y parsear una dirección IP
    fn get_ip(map: &HashMap<String, String>, key: &str) -> Result<IpAddr, String> {
        let ip_str = Self::get(map, key)?;
        // Try to parse as IpAddr first
        if let Ok(parsed_ip) = ip_str.parse::<IpAddr>() {
            return Ok(parsed_ip);
        }
        // Try to resolve as hostname (with dummy port)
        let addr = format!("{ip_str}:4000");
        let mut addrs_iter = addr.to_socket_addrs().map_err(|_| {
            format!("El valor para 'ip' (\"{ip_str}\") no es una dirección IP válida.")
        })?;

        if let Some(socket_addr) = addrs_iter.next() {
            return Ok(socket_addr.ip());
        }
        Err(format!(
            "No se pudo resolver '{key}' como dirección IP o hostname: {ip_str}"
        ))
    }

    /// Función para obtener y parsear un puerto
    fn get_port(map: &HashMap<String, String>, key: &str) -> Result<u16, String> {
        let port_val = Self::parse_u16(map, key)?;
        if port_val == 0 {
            return Err("El valor de 'port' no puede ser 0.".to_string());
        }
        Ok(port_val)
    }

    /// Función para obtener la dirección y el puerto
    fn get_address(map: &HashMap<String, String>) -> Result<SocketAddr, String> {
        let parsed_ip: IpAddr = Self::get_ip(map, "ip")?;
        let port_val = Self::get_port(map, "port")?;
        let address = SocketAddr::new(parsed_ip, port_val);
        if address.port() == 0 {
            return Err("El valor de 'port' no puede ser 0.".to_string());
        }
        Ok(address)
    }

    fn parse_cluster_address(map: &HashMap<String, String>) -> Result<SocketAddr, String> {
        let cluster_address_str = Self::get(map, "cluster_ip")?;
        let port = Self::get_port(map, "port")? + 10_000;
        let addr = format!("{cluster_address_str}:{port}");
        let mut addrs = addr.to_socket_addrs().map_err(|_| {
            format!(
                "El valor de 'cluster_address' (\"{cluster_address_str}\") no es una dirección IP o hostname válido."
            )
        })?;
        addrs
            .next()
            .ok_or_else(|| format!("No se pudo resolver 'cluster_address' ({addr})"))
            .map_err(|e| e.to_string())
    }

    /// Función para obtener el rango de slots
    fn get_slot_range(map: &HashMap<String, String>) -> Result<Range<u16>, String> {
        let slot_range_start = Self::parse_u16(map, "slot_range_start")?;
        let slot_range_end = Self::parse_u16(map, "slot_range_end")?;

        if slot_range_start > slot_range_end || slot_range_end > 16384 {
            return Err("Configuración de slots inválida".to_string());
        }

        let slot_range = Range {
            start: slot_range_start,
            end: slot_range_end,
        };
        Ok(slot_range)
    }

    /// Función para obtener el número máximo de clientes
    fn get_max_clients(map: &HashMap<String, String>) -> Result<usize, String> {
        let max_clients_str = Self::get(map, "max_clients")?;
        let max_clients = max_clients_str.parse::<usize>().map_err(|_| {
            format!("No se pudo parsear 'max_clients' (\"{max_clients_str}\") como usize")
        })?;
        Ok(max_clients)
    }

    /// Función para obtener el valor de appendonly y el nombre del archivo AOF
    fn get_append_only(map: &HashMap<String, String>) -> Result<(bool, String), String> {
        let appendonly = match map.get("appendonly").map(|s| s.as_str()) {
            Some("yes") => true,
            Some("no") => false,
            Some(value) => {
                return Err(format!(
                    "Valor inválido para 'appendonly': '{value}'. Debe ser 'yes' o 'no'."
                ));
            }
            None => false,
        };

        let aof_file = Self::get(map, "aof_file")?;
        let trimmed_path = aof_file.trim();
        if trimmed_path.is_empty() {
            return Err(format!(
                "El campo 'aof_file', si se especifica, no puede estar vacío. Valor recibido: '{trimmed_path}'"
            ));
        }
        let aof_file = trimmed_path.to_string();

        Ok((appendonly, aof_file))
    }

    /// Función para obtener el intervalo de guardado
    fn get_save_info(map: &HashMap<String, String>) -> Result<u64, String> {
        let save_str = Self::get(map, "save")?;
        let save_interval_ms = save_str.parse::<u64>().map_err(|_| {
            format!("No se pudo parsear 'save' (\"{save_str}\") como u64 (milisegundos)")
        })?;

        if save_interval_ms == 0 {
            return Err("El valor de 'save' (milisegundos) debe ser mayor que 0.".to_string());
        }
        Ok(save_interval_ms)
    }

    fn get_socket_addr(
        map: &HashMap<String, String>,
        key: &str,
    ) -> Result<Option<SocketAddr>, String> {
        if let Some(value) = map.get(key) {
            let mut addrs = value.to_socket_addrs().map_err(|_| {
                format!("El valor de '{key}' (\"{value}\") no es una dirección IP:PORT válida.")
            })?;

            addrs
                .next()
                .map(Some)
                .ok_or_else(|| format!("No se pudo resolver '{value}'"))
        } else {
            Ok(None)
        }
    }

    fn get_users(map: &HashMap<String, String>) -> Result<HashMap<String, String>, String> {
        let file_path = Self::get(map, "users_file")?;
        let lines = Self::read_lines_from_file(&file_path)?;
        let users = Self::parse_key_value_lines(lines, ':', "users_file")?;
        Ok(users)
    }

    fn get_node_timeout(map: &HashMap<String, String>) -> Result<u64, String> {
        let duration = Self::get(map, "node_timeout")?;
        let ms = duration.parse::<u64>().map_err(|_| {
            format!("No se pudo parsear 'node_timeout' (\"{duration}\") como u64 (milisegundos)")
        })?;
        if ms <= 1000 {
            return Err("node_timeout debe ser mayor a 1 segundo".to_string());
        }
        Ok(ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::net::SocketAddr;

    fn create_temp_config_file(path: &str, content: &str) {
        let mut file = fs::File::create(path).unwrap_or_else(|e| {
            panic!("No se pudo crear el archivo de config temporal '{path}': {e}")
        });
        file.write_all(content.as_bytes()).unwrap_or_else(|e| {
            panic!("No se pudo escribir en el archivo de config temporal '{path}': {e}")
        });
    }

    fn remove_temp_config_file(path: &str) {
        fs::remove_file(path).unwrap_or_else(|e| {
            panic!("No se pudo eliminar el archivo de config temporal '{path}': {e}")
        });
    }

    #[test]
    fn test_01_todos_los_campos() {
        let lines = [
            "ip=127.0.0.1",
            "port=8089",
            "slot_range_start=0",
            "slot_range_end=16378",
            "max_clients=100",
            "aof_file=aof.log",
            "metadata_file=metadata.bin",
            "storage_file=storage.bin",
            "log_file=node_1.log",
            "users_file=src/config/users_test.txt",
            "appendonly=yes",
            "save=900",
            "node_timeout=5000",
            "cluster_ip=127.0.0.1",
            "public_address=127.0.0.1:8089",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "temp_valid_config_full.txt";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(
            config_result.is_ok(),
            "Parser fallo con config valida: {:?}",
            config_result.err()
        );

        let config = config_result.unwrap();
        let expected_address: SocketAddr = "127.0.0.1:8089".parse().unwrap();
        assert_eq!(config.address, expected_address);
        assert_eq!(config.slot_range, 0..16378);
        assert_eq!(config.max_clients, 100);
        assert_eq!(config.aof_file, "aof.log".to_string());
        assert_eq!(config.metadata_file, "metadata.bin");
        assert_eq!(config.storage_file, "storage.bin");
        assert_eq!(config.log_file, "node_1.log");
        assert!(config.appendonly);
        assert_eq!(config.save_interval_ms, 900);

        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_02_aof() {
        let lines = [
            "ip=10.0.0.1",
            "port=7000",
            "slot_range_start=10",
            "slot_range_end=20",
            "max_clients=5",
            "aof_file=optional.aof",
            "metadata_file=meta.db",
            "storage_file=store.db",
            "log_file=node_gamma.log",
            "users_file=src/config/users_test.txt",
            "appendonly=no",
            "save=1000",
            "node_timeout=5000",
            "cluster_ip=127.0.0.1",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "temp_valid_an_af_present.txt";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(
            config_result.is_ok(),
            "Parser fallo con config valida (appendonly=no): {:?}",
            config_result.err()
        );
        let config = config_result.unwrap();
        assert!(!config.appendonly);
        assert_eq!(config.aof_file, "optional.aof".to_string());
        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_03_no_aof() {
        let lines = [
            "ip=192.168.1.1",
            "port=7004",
            "slot_range_start=300",
            "slot_range_end=400",
            "max_clients=3",
            "aof_file=my_aof.aof",
            "metadata_file=another_meta.dat",
            "storage_file=another_store.dat",
            "log_file=another_log.log",
            "users_file=src/config/users_test.txt",
            "save=1000",
            "node_timeout=5000",
            "cluster_ip=127.0.0.1",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "temp_valid_appmiss_afpres.txt";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(
            config_result.is_ok(),
            "Parser falló (appendonly ausente): {:?}",
            config_result.err()
        );
        let config = config_result.unwrap();
        assert!(!config.appendonly);
        assert_eq!(config.aof_file, "my_aof.aof".to_string());
        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_04_falta_aof_file() {
        let lines = [
            "ip=127.0.0.1",
            "port=8089",
            "slot_range_start=0",
            "slot_range_end=16378",
            "max_clients=100",
            "metadata_file=metadata.bin",
            "storage_file=storage.bin",
            "log_file=node_1.log",
            "users_file=src/config/users_test.txt",
            "appendonly=yes",
            "save=900",
            "node_timeout=5000",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "temp_error_af_missing.txt";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(config_result.is_err(),);
        assert_eq!(
            config_result.err().unwrap(),
            "Falta el campo obligatorio en el config: 'aof_file'"
        );
        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_05_aof_file_vacio() {
        let lines = [
            "ip=127.0.0.1",
            "port=8089",
            "slot_range_start=0",
            "slot_range_end=100",
            "max_clients=10",
            "aof_file=",
            "metadata_file=m.bin",
            "storage_file=s.bin",
            "log_file=l.log",
            "users_file=src/config/users_test.txt",
            "appendonly=no",
            "save=900",
            "node_timeout=5000",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "temp_error_af_empty_literal.txt";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(config_result.is_err());
        assert_eq!(
            config_result.err().unwrap(),
            "El campo 'aof_file', si se especifica, no puede estar vacío. Valor recibido: ''"
        );
        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_06_aof_file_es_whitespace() {
        let lines = [
            "ip=127.0.0.1",
            "port=8089",
            "slot_range_start=0",
            "slot_range_end=100",
            "max_clients=10",
            "aof_file=   ",
            "metadata_file=m.bin",
            "storage_file=s.bin",
            "log_file=l.log",
            "users_file=src/config/users_test.txt",
            "appendonly=yes",
            "save=900",
            "node_timeout=5000",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "temp_error_af_whitespace.txt";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(config_result.is_err());
        assert_eq!(
            config_result.err().unwrap(),
            "El campo 'aof_file', si se especifica, no puede estar vacío. Valor recibido: ''"
        );
        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_07_aof_invalido() {
        let lines = [
            "ip=127.0.0.1",
            "port=8080",
            "slot_range_start=0",
            "slot_range_end=100",
            "max_clients=1",
            "aof_file=some.aof",
            "metadata_file=m.bin",
            "storage_file=s.bin",
            "log_file=l.log",
            "users_file=src/config/users_test.txt",
            "appendonly=maybe",
            "save=1000",
            "node_timeout=5000",
            "cluster_ip=127.0.0.1",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "temp_error_invalid_appendonly.txt";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(config_result.is_err());
        assert_eq!(
            config_result.err().unwrap(),
            "Valor inválido para 'appendonly': 'maybe'. Debe ser 'yes' o 'no'."
        );
        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_08_falta_port() {
        let lines = [
            "ip=127.0.0.1",
            "slot_range_start=0",
            "slot_range_end=16378",
            "max_clients=100",
            "aof_file=aof.log",
            "metadata_file=metadata.bin",
            "storage_file=storage.bin",
            "log_file=node_1.log",
            "users_file=src/config/users_test.txt",
            "appendonly=yes",
            "save=900",
            "node_timeout=5000",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "temp_error_missing_port.txt";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(config_result.is_err());
        assert_eq!(
            config_result.err().unwrap(),
            "Falta el campo obligatorio en el config: 'port'"
        );
        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_09_invalid_ip() {
        let lines = [
            "ip=127.0.0.256",
            "port=8080",
            "slot_range_start=0",
            "slot_range_end=100",
            "max_clients=1",
            "aof_file=some.aof",
            "metadata_file=m.bin",
            "storage_file=s.bin",
            "log_file=l.log",
            "users_file=src/config/users_test.txt",
            "appendonly=no",
            "save=1000",
            "node_timeout=5000",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "temp_error_invalid_ip.txt";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(config_result.is_err());
        assert_eq!(
            config_result.err().unwrap(),
            "El valor para 'ip' (\"127.0.0.256\") no es una dirección IP válida."
        );
        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_10_error_port_zero() {
        let lines = [
            "ip=127.0.0.1",
            "port=0",
            "tcp_bus_port=18080",
            "slot_range_start=0",
            "slot_range_end=100",
            "max_clients=1",
            "aof_file=some.aof",
            "metadata_file=m.bin",
            "storage_file=s.bin",
            "log_file=l.log",
            "users_file=src/config/users_test.txt",
            "appendonly=no",
            "save=1000",
            "node_timeout=5000",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "temp_error_port_zero.txt";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(config_result.is_err());
        assert_eq!(
            config_result.err().unwrap(),
            "El valor de 'port' no puede ser 0."
        );
        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_11_slot_range_start_greater_than_end() {
        let lines = [
            "ip=127.0.0.1",
            "port=8080",
            "slot_range_start=1000",
            "slot_range_end=500",
            "max_clients=1",
            "aof_file=some.aof",
            "metadata_file=m.bin",
            "storage_file=s.bin",
            "log_file=l.log",
            "users_file=src/config/users_test.txt",
            "appendonly=no",
            "save=1000",
            "node_timeout=5000",
            "cluster_ip=127.0.0.1",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "temp_error_slot_start_gt_end.txt";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(config_result.is_err());
        assert_eq!(
            config_result.err().unwrap(),
            "Configuración de slots inválida"
        );
        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_12_error_slot_range_end_excede_max() {
        let lines = [
            "ip=127.0.0.1",
            "port=8080",
            "slot_range_start=0",
            "slot_range_end=20000",
            "max_clients=1",
            "aof_file=some.aof",
            "metadata_file=m.bin",
            "storage_file=s.bin",
            "log_file=l.log",
            "users_file=src/config/users_test.txt",
            "appendonly=no",
            "save=1000",
            "node_timeout=5000",
            "cluster_ip=127.0.0.1",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "temp_error_slot_end_exceeds.txt";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(config_result.is_err());
        assert_eq!(
            config_result.err().unwrap(),
            "Configuración de slots inválida"
        );
        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_13_valid_slot_range() {
        let lines = [
            "ip=127.0.0.1",
            "port=8080",
            "slot_range_start=555",
            "slot_range_end=555",
            "max_clients=1",
            "aof_file=some.aof",
            "metadata_file=m.bin",
            "storage_file=s.bin",
            "log_file=l.log",
            "users_file=src/config/users_test.txt",
            "appendonly=no",
            "save=1000",
            "node_timeout=5000",
            "cluster_ip=127.0.0.1",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "temp_valid_slot_start_eq_end.txt";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(config_result.is_ok(),);
        let config = config_result.unwrap();
        assert_eq!(config.slot_range, 555..555);
        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_14_master_seed_node_should_parse() {
        let lines = [
            "ip=127.0.0.1",
            "port=8000",
            "slot_range_start=0",
            "slot_range_end=16383",
            "max_clients=100",
            "aof_file=aof.log",
            "metadata_file=meta.bin",
            "storage_file=store.bin",
            "log_file=node.log",
            "users_file=src/config/users_test.txt",
            "appendonly=yes",
            "save=5000",
            "node_timeout=5000",
            "cluster_ip=127.0.0.1",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "test_master_seed.conf";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(config_result.is_ok());
        let config = config_result.unwrap();
        assert_eq!(config.replicaof, None);
        assert_eq!(config.node_seed, None);

        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_15_master_node_with_seed_should_parse() {
        let lines = [
            "ip=127.0.0.1",
            "port=8001",
            "slot_range_start=0",
            "slot_range_end=1000",
            "max_clients=10",
            "aof_file=aof.log",
            "metadata_file=meta.bin",
            "storage_file=store.bin",
            "log_file=node.log",
            "users_file=src/config/users_test.txt",
            "appendonly=no",
            "save=1000",
            "node_timeout=5000",
            "node_id_seed=127.0.0.1:8000",
            "cluster_ip=127.0.0.1",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "test_master_with_seed.conf";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(config_result.is_ok());
        let config = config_result.unwrap();
        assert_eq!(config.node_seed, Some("127.0.0.1:8000".parse().unwrap()));
        assert_eq!(config.replicaof, None);

        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_16_replica_seed_node_should_parse() {
        let lines = [
            "ip=127.0.0.1",
            "port=8002",
            "slot_range_start=1001",
            "slot_range_end=2000",
            "max_clients=10",
            "aof_file=aof.log",
            "metadata_file=meta.bin",
            "storage_file=store.bin",
            "log_file=node.log",
            "users_file=src/config/users_test.txt",
            "appendonly=yes",
            "save=3000",
            "node_timeout=5000",
            "replicaof=127.0.0.1:8000",
            "cluster_ip=127.0.0.1",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "test_replica_seed.conf";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(config_result.is_ok());
        let config = config_result.unwrap();
        assert_eq!(config.replicaof, Some("127.0.0.1:8000".parse().unwrap()));
        assert_eq!(config.node_seed, None);

        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_17_replica_with_seed_should_parse() {
        let lines = [
            "ip=127.0.0.1",
            "port=8003",
            "slot_range_start=2001",
            "slot_range_end=3000",
            "max_clients=10",
            "aof_file=aof.log",
            "metadata_file=meta.bin",
            "storage_file=store.bin",
            "log_file=node.log",
            "users_file=src/config/users_test.txt",
            "appendonly=no",
            "save=6000",
            "replicaof=127.0.0.1:8001",
            "node_timeout=5000",
            "node_id_seed=127.0.0.1:8000",
            "cluster_ip=127.0.0.1",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "test_replica_with_seed.conf";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(config_result.is_ok());
        let config = config_result.unwrap();
        assert_eq!(config.replicaof, Some("127.0.0.1:8001".parse().unwrap()));
        assert_eq!(config.node_seed, Some("127.0.0.1:8000".parse().unwrap()));

        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_18_users_map_success() {
        let lines = [
            "ip=127.0.0.1",
            "port=8089",
            "slot_range_start=0",
            "slot_range_end=16378",
            "max_clients=100",
            "aof_file=aof.log",
            "metadata_file=metadata.bin",
            "storage_file=storage.bin",
            "log_file=node_1.log",
            "users_file=src/config/users_test.txt",
            "appendonly=yes",
            "save=900",
            "node_timeout=5000",
            "cluster_ip=127.0.0.1",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "users_map_success.txt";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(config_result.is_ok(),);

        let config = config_result.unwrap();
        let expected_users = HashMap::from([
            ("user1".to_string(), "123456".to_string()),
            ("user2".to_string(), "123123".to_string()),
            ("user3".to_string(), "147258".to_string()),
            ("user4".to_string(), "258369".to_string()),
        ]);

        let users = config.get_node_users();
        assert!(!users.is_empty(),);
        assert_eq!(users.len(), expected_users.len(),);
        for (key, value) in expected_users.iter() {
            assert!(users.contains_key(key),);
            assert_eq!(users.get(key).unwrap(), value,);
        }
        remove_temp_config_file(test_file_path);
    }

    #[test]
    fn test_19_node_timeout_success() {
        let lines = [
            "ip=127.0.0.1",
            "port=8089",
            "slot_range_start=0",
            "slot_range_end=16378",
            "max_clients=100",
            "aof_file=aof.log",
            "metadata_file=metadata.bin",
            "storage_file=storage.bin",
            "log_file=node_1.log",
            "users_file=src/config/users_test.txt",
            "appendonly=yes",
            "save=900",
            "node_timeout=5000",
            "cluster_ip=127.0.0.1",
        ];
        let config_content = lines.join("\n") + "\n";
        let test_file_path = "temp_valid_config_full.txt";
        create_temp_config_file(test_file_path, &config_content);

        let config_result = Config::from_file(test_file_path);
        assert!(config_result.is_ok(),);

        let config = config_result.unwrap();
        assert_eq!(config.node_timeout, 5000);

        remove_temp_config_file(test_file_path);
    }
}
