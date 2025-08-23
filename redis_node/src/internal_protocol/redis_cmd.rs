use super::utils::read_exact;
use crate::internal_protocol::protocol_trait::{DeserializeRIP, SerializeRIP};
use std::io::Read;

/// Representa un comanbdo de Redus utilizado en la comunicación
/// entre nodos del clúster Redis. Esta estructura es parte del payload de mensajes
/// tipo `RedisCommand` enviados entre nodos.
///
///
/// # Campos
/// - `command`: Vector de tokens (strings) que representan el comando
/// - `len_command`: cantidad de tokens del comando
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct RedisCMD {
    command: Vec<String>,
    len_command: u32,
}

impl RedisCMD {
    /// Crea un nuevo redis cmd a enviar a otro nodo.
    ///
    /// # Argumentos
    /// - `command`: comando a enviar, tomado como vec de strings
    ///
    /// # Retorna
    /// Una nueva instancia de `RedisCMD`.
    pub fn new(command: Vec<String>) -> Self {
        let len_command = command.len() as u32;
        Self {
            command,
            len_command,
        }
    }

    pub fn get_command(&self) -> Vec<String> {
        self.command.clone()
    }
}

impl SerializeRIP for RedisCMD {
    /// Serializa el comando en formato binario según el protocolo interno
    /// de Redis Cluster. La serialización incluye:
    ///
    /// 1. Cantidad de strings que conforman el comando (4 bytes).
    /// 2. Vector de strings, cada uno precedido por su largo en bytes
    ///
    /// # Retorna
    /// Un `Vec<u8>` con los bytes serializados.
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(&self.len_command.to_be_bytes());
        let command = &self.command;

        for string in command {
            let bytes_string = string.as_bytes();
            let len = bytes_string.len() as u32;
            bytes.extend(&len.to_be_bytes());
            bytes.extend(bytes_string);
        }
        bytes
    }
}

impl DeserializeRIP for RedisCMD {
    /// Deserializa el comando desde un stream de bytes, leyendo
    /// los campos en el mismo orden en que fueron serializados.
    ///
    /// # Formato esperado
    /// 1. Cantidad de strings que conforman el comando (4 bytes).
    /// 2. Vector de strings, cada uno precedido por su largo en bytes
    ///
    /// # Argumentos
    /// - `stream`: Stream binario desde el que se lee.
    ///
    /// # Retorna
    /// Un `Result` con la entrada `RedisCMD` o un error de I/O.
    fn deserialize<T: Read>(stream: &mut T) -> std::io::Result<Self> {
        let len_command = u32::from_be_bytes(read_exact::<4, _>(stream)?);
        let mut command = Vec::with_capacity(len_command as usize);
        for _ in 0..len_command {
            command.push(deserialize_string(stream)?);
        }

        Ok(Self::new(command))
    }
}

fn deserialize_string<T: Read>(stream: &mut T) -> std::io::Result<String> {
    let len_string = u32::from_be_bytes(read_exact::<4, _>(stream)?);
    let mut buf = vec![0u8; len_string as usize];
    stream.read_exact(&mut buf)?;
    String::from_utf8(buf)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "ID no válido UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_01_redis_cmd_struct() {
        let command = vec!["set".to_string(), "hola".to_string(), "mundo".to_string()];
        let entry = RedisCMD::new(command.clone());

        assert_eq!(entry.command, command);
        assert_eq!(entry.len_command, 3u32);
    }

    #[test]
    fn test_02_redis_cmd_serialization_deserialization() {
        let command = vec!["set".to_string(), "hola".to_string(), "mundo".to_string()];
        let entry = RedisCMD::new(command);

        let bytes = entry.serialize();
        let mut cursor = Cursor::new(bytes);
        let deserialized = RedisCMD::deserialize(&mut cursor).unwrap();

        assert_eq!(entry.command, deserialized.command);
        assert_eq!(entry.len_command, deserialized.len_command);
    }

    #[test]
    fn test_03_redis_cmd_serialization_deserialization_mult() {
        let command = vec!["set".to_string(), "hola".to_string(), "mundo".to_string()];
        let entry = RedisCMD::new(command.clone());

        let command2 = vec!["get".to_string(), "hola".to_string()];
        let entry2 = RedisCMD::new(command2.clone());

        let mut bytes = entry.serialize();
        bytes.extend(entry2.serialize());
        let mut cursor = Cursor::new(bytes);
        let deserialized = RedisCMD::deserialize(&mut cursor).unwrap();
        let deserialized2 = RedisCMD::deserialize(&mut cursor).unwrap();

        assert_eq!(entry.get_command(), command);
        assert_eq!(entry.command, deserialized.command);
        assert_eq!(entry.len_command, deserialized.len_command);
        assert_eq!(entry2.get_command(), command2);
        assert_eq!(entry2.command, deserialized2.command);
        assert_eq!(entry2.len_command, deserialized2.len_command);
    }
}
