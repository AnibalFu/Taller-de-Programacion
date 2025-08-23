//! Este módulo implementa el storage del nodo
use std::collections::HashMap;
use std::ops::Range;

use crate::constantes::TOTAL_SLOTS;
use common::cr16::crc16;
use redis_client::tipos_datos::traits::DatoRedis;

/// Estructura que representa el almacenamiento interno del sistema.
///
/// Cada `Storage` contiene un rango de slots que define qué y cuantas claves puede manejar.
/// Cada slot mantiene un hashmap de claves (String) y valores `DatoRedis`.
#[derive(Debug, PartialEq)]
pub struct Storage {
    slot_range: Range<u16>,
    hashes_slots: HashMap<u16, HashMap<String, DatoRedis>>,
}

impl Storage {
    /// Crea una nueva instancia de `Storage` vacía para un rango de slots determinado.
    ///
    /// # Parámetros
    /// - `slot_range`: Rango de slots (inclusive inferior, exclusivo superior) asignado al almacenamiento.
    ///
    /// # Retorna
    /// Una instancia vacía de `Storage`.
    pub fn new(slot_range: Range<u16>) -> Self {
        Storage {
            slot_range,
            hashes_slots: HashMap::new(),
        }
    }

    /// Crea un `Storage` con datos iniciales predefinidos.
    ///
    /// # Parámetros
    /// - `slot_range`: Rango de slots válido.
    /// - `hashes_slots`: Mapa que asocia slots a sus datos (`HashMap<String, DatoRedis>`).
    ///
    /// # Retorna
    /// Un `Storage` inicializado con contenido.
    pub fn new_with_content(
        slot_range: Range<u16>,
        hashes_slots: HashMap<u16, HashMap<String, DatoRedis>>,
    ) -> Self {
        Storage {
            slot_range,
            hashes_slots,
        }
    }

    /// Obtiene una copia del valor asociado a una clave.
    ///
    /// # Parámetros
    /// - `key`: Clave a consultar.
    ///
    /// # Retorna
    /// - `Ok(DatoRedis)`: Si la clave existe y está en el rango del slot.
    /// - `Err(DatoRedis::Null)`: Si no se encuentra.
    pub fn get(&self, key: String) -> Result<DatoRedis, DatoRedis> {
        let slot = self.calculate_slot_for_key(&key);
        self.verificar_key_in_range(slot)?;
        let slot_hash = self.hashes_slots.get(&slot).ok_or(DatoRedis::new_null())?;
        let value = slot_hash.get(&key).ok_or(DatoRedis::new_null())?;
        Ok(value.clone())
    }

    /// Obtiene una referencia mutable al valor de una clave.
    ///
    /// # Parámetros
    /// - `key`: Clave a consultar.
    ///
    /// # Retorna
    /// - `Ok(&mut DatoRedis)`: Si la clave existe.
    /// - `Err(DatoRedis::Null)`: Si la clave no se encuentra o está fuera del rango.
    pub fn get_mutable(&mut self, key: String) -> Result<&mut DatoRedis, DatoRedis> {
        let slot = self.calculate_slot_for_key(&key);
        self.verificar_key_in_range(slot)?;

        let slot_hash = self
            .hashes_slots
            .get_mut(&slot)
            .ok_or(DatoRedis::new_null())?;

        let value = slot_hash.get_mut(&key).ok_or(DatoRedis::new_null())?;
        Ok(value)
    }

    /// Inserta o actualiza una clave con un valor.
    ///
    /// # Parámetros
    /// - `key`: Clave a insertar.
    /// - `value`: Valor a asociar.
    ///
    /// # Retorna
    /// - `Ok(())`: Si se guardó correctamente.
    /// - `Err`: Si la clave no pertenece al rango del `Storage`.
    pub fn set(&mut self, key: String, value: DatoRedis) -> Result<(), DatoRedis> {
        let slot = self.calculate_slot_for_key(&key);
        self.verificar_key_in_range(slot)?;
        let slot_to_save = self.hashes_slots.entry(slot).or_default();
        slot_to_save.insert(key.to_string(), value);
        Ok(())
    }

    /// Elimina una clave del almacenamiento.
    ///
    /// # Parámetros
    /// - `key`: Clave a eliminar.
    ///
    /// # Retorna
    /// - `Ok(DatoRedis)`: Valor eliminado.
    /// - `Err(DatoRedis::Integer(0))`: Si no existía.
    pub fn remove(&mut self, key: String) -> Result<DatoRedis, DatoRedis> {
        let slot = self.calculate_slot_for_key(&key);
        self.verificar_key_in_range(slot)?;

        let slot_hash = self
            .hashes_slots
            .get_mut(&slot)
            .ok_or(DatoRedis::new_integer(0))?;

        if let Some(result) = slot_hash.remove(&key) {
            return Ok(result);
        }
        Err(DatoRedis::new_integer(0))
    }

    /// Verifica si un slot se encuentra dentro del rango asignado al `Storage`.
    ///
    /// # Parámetros
    /// - `slot`: Slot a validar.
    ///
    /// # Retorna
    /// - `Ok(())`: Si está dentro del rango.
    /// - `Err(DatoRedis::Error("MOVED", slot))`: Si no lo está.
    fn verificar_key_in_range(&self, slot: u16) -> Result<(), DatoRedis> {
        if !self.slot_range.contains(&slot) {
            return Err(DatoRedis::new_moved_error(slot));
        }
        Ok(())
    }

    /// Calcula el slot correspondiente a una clave usando CRC16.
    ///
    /// # Parámetros
    /// - `slot`: Clave de la cual se desea obtener el slot.
    ///
    /// # Retorna
    /// El número de slot correspondiente.
    fn calculate_slot_for_key(&self, slot: &str) -> u16 {
        crc16(slot.as_bytes()) % TOTAL_SLOTS
    }

    /// Iterador sobre los slots y sus hashmaps de claves/valores.
    ///
    /// # Retorna
    /// Un iterador sobre `(&u16, &HashMap<String, DatoRedis>)`.
    pub fn iter(&self) -> impl Iterator<Item = (&u16, &HashMap<String, DatoRedis>)> {
        self.hashes_slots.iter()
    }
    /// Obtiene una copia del rango de slots del `Storage`.
    pub fn get_slot_range(&self) -> Range<u16> {
        self.slot_range.clone()
    }

    pub fn in_memory(slot_range: Range<u16>) -> Self {
        Self {
            slot_range,
            hashes_slots: HashMap::new(),
        }
    }
}

#[cfg(test)]
impl Storage {
    pub fn slot_range(&self) -> &Range<u16> {
        &self.slot_range
    }

    pub fn hashes_slots(&self) -> &HashMap<u16, HashMap<String, DatoRedis>> {
        &self.hashes_slots
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use redis_client::tipos_datos::traits::{DatoRedis, TipoDatoRedis};

    use crate::storage::Storage;
    const RANGE: Range<u16> = Range {
        start: 0,
        end: 16378,
    };

    #[test]
    fn create_storage_initializes_correctly() {
        let stg = Storage::new(RANGE);
        assert_eq!(stg.slot_range(), &RANGE);
        assert!(stg.hashes_slots().is_empty());
    }

    #[test]
    fn storage_sets_key_value_correctly() {
        let mut stg = Storage::new(RANGE);
        let value = DatoRedis::new_bulk_string("Mundo".to_string()).unwrap();
        let _ = stg.set("Hola".to_string(), value);
        assert!(!stg.hashes_slots().is_empty());
    }

    #[test]
    fn storage_gets_value_correctly() {
        let mut stg = Storage::new(RANGE);
        let value_original = DatoRedis::new_bulk_string("Mundo".to_string()).unwrap();
        let _ = stg.set("Hola".to_string(), value_original.clone());
        let value_getted = stg.get("Hola".to_string()).unwrap();

        assert_eq!(
            value_getted.convertir_a_protocolo_resp(),
            value_original.convertir_a_protocolo_resp()
        );
    }

    #[test]
    fn storage_removes_value_correctly() {
        let mut stg = Storage::new(RANGE);
        let value_original = DatoRedis::new_bulk_string("Mundo".to_string()).unwrap();
        let _ = stg.set("Hola".to_string(), value_original.clone());
        assert_eq!(stg.remove("Hola".to_string()).unwrap(), value_original);
        assert!(matches!(
            stg.get("Hola".to_string()),
            Err(err) if err == DatoRedis::new_null()
        ));
    }
}
