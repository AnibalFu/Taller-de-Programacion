use std::io::Read;

/// Trait para serializar estructuras de datos al protocolo interno de Redis (RIP).
///
/// Proporciona una función para convertir una instancia en una secuencia de bytes
/// que representa el objeto según el formato del protocolo interno.
pub trait SerializeRIP {
    /// Serializa la instancia en un vector de bytes conforme al protocolo interno de Redis.
    fn serialize(&self) -> Vec<u8>;
}

/// Trait para deserializar estructuras desde el protocolo interno de Redis (RIP).
///
/// Proporciona una función para leer datos desde un stream de bytes y construir
/// una instancia del tipo correspondiente según el formato del protocolo interno.
pub trait DeserializeRIP {
    /// Deserializa una instancia del tipo desde el stream de bytes que sigue el protocolo interno de Redis.
    ///
    /// # Errores
    /// Retorna un error `std::io::Error` si la lectura falla o los datos son inválidos.
    fn deserialize<T: Read>(stream: &mut T) -> std::io::Result<Self>
    where
        Self: Sized;
}
