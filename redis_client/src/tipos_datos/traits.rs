use super::map_reply::MapReply;
use super::moved_error::MovedError;
use crate::protocol::dataencryption::decrypt_resp;
use crate::protocol::dataencryption::encrypt_resp;
use crate::tipos_datos::arrays::Arrays;
use crate::tipos_datos::bulk_string::BulkString;
use crate::tipos_datos::constantes::*;
use crate::tipos_datos::integer::Integer;
use crate::tipos_datos::nulls::Null;
use crate::tipos_datos::set::Set;
use crate::tipos_datos::simple_error::SimpleError;
use crate::tipos_datos::simple_string::SimpleString;
use crate::tipos_datos::verbatim_string::VerbatimString;
use std::fmt::Debug;
use std::io::Cursor;

/// Se define a los tipos de datos Redis a todos aquellos tipos que implementen el trait TipoDatoRedis
pub trait TipoDatoRedis {
    fn convertir_a_protocolo_resp(&self) -> String;
    fn convertir_resp_a_string(&self) -> String;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DatoRedis {
    BulkString(BulkString),
    Arrays(Arrays),
    Integer(Integer),
    Set(Set),
    SimpleString(SimpleString),
    VerbatimString(VerbatimString),
    Null(Null),
    SimpleError(SimpleError),
    Map(MapReply),
    MovedError(MovedError),
}

impl DatoRedis {
    pub fn convertir_a_protocolo(&self) -> String {
        self.convertir_a_protocolo_resp()
    }

    pub fn new_bulk_string(contenido: String) -> Result<Self, DatoRedis> {
        Ok(DatoRedis::BulkString(BulkString::new(contenido)?))
    }

    pub fn new_array() -> Self {
        DatoRedis::Arrays(Arrays::new())
    }
    pub fn new_array_con_contenido(contenido: Arrays) -> Self {
        DatoRedis::Arrays(contenido)
    }

    pub fn new_integer(valor: i64) -> Self {
        DatoRedis::Integer(Integer::new(valor))
    }

    pub fn new_set() -> Self {
        DatoRedis::Set(Set::new())
    }

    pub fn new_set_con_contenido(set: Set) -> Self {
        DatoRedis::Set(set)
    }

    pub fn new_simple_string(contenido: String) -> Result<Self, DatoRedis> {
        Ok(DatoRedis::SimpleString(SimpleString::new(contenido)?))
    }

    pub fn new_verbatim_string(contenido: String, tipo: String) -> Result<Self, DatoRedis> {
        Ok(DatoRedis::VerbatimString(VerbatimString::new(
            contenido, tipo,
        )?))
    }

    pub fn new_null() -> Self {
        DatoRedis::Null(Null::new())
    }

    pub fn new_simple_error(tipo: String, mensaje: String) -> Self {
        DatoRedis::SimpleError(SimpleError::new(tipo, mensaje))
    }

    pub fn new_map_reply_with_content(map: MapReply) -> Self {
        DatoRedis::Map(map)
    }

    pub fn new_moved_error(slot: u16) -> Self {
        DatoRedis::MovedError(MovedError::new(slot))
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let resp: String = self.convertir_a_protocolo_resp();
        encrypt_resp(&resp).unwrap_or_else(|_| vec![])
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DatoRedis> {
        let contenido_resp =
            String::from_utf8(bytes.to_vec()).map_err(|_e| DatoRedis::new_null())?;
        let tipo_dato = bytes
            .first()
            .ok_or_else(DatoRedis::new_null)
            .map(|b| *b as char)?;
        let tipo_dato = tipo_dato.to_string();
        Self::new_dato_redis(&tipo_dato, contenido_resp)
    }

    pub fn from_encrypted_bytes(bytes: &[u8]) -> Result<Self, DatoRedis> {
        let mut cursor = Cursor::new(bytes);

        let contenido_resp = decrypt_resp(&mut cursor)?;

        let tipo_dato = contenido_resp
            .chars()
            .next()
            .ok_or_else(DatoRedis::new_null)?
            .to_string();

        Self::new_dato_redis(&tipo_dato, contenido_resp)
    }

    fn new_dato_redis(tipo: &str, contenido_resp: String) -> Result<Self, DatoRedis> {
        match tipo {
            SIMPLE_STRING_SIMBOL => Ok(DatoRedis::SimpleString(SimpleString::new_desde_resp(
                contenido_resp,
            )?)),
            BULK_STRING_SIMBOL => Ok(DatoRedis::BulkString(BulkString::new_desde_resp(
                contenido_resp,
            )?)),
            INTEGER_SIMBOL => Ok(DatoRedis::Integer(Integer::new_desde_resp(contenido_resp)?)),
            ARRAY_SIMBOL => Ok(DatoRedis::Arrays(Arrays::new_desde_resp(contenido_resp)?)),
            SETS_SIMBOL => Ok(DatoRedis::Set(Set::new_desde_resp(contenido_resp)?)),
            NULL_SIMBOL => Ok(DatoRedis::Null(Null::new_desde_resp(contenido_resp)?)),
            ERROR_SIMBOL => Ok(DatoRedis::SimpleError(SimpleError::new_desde_resp(
                contenido_resp,
            )?)),
            MAP_SYMBOL => Ok(DatoRedis::Map(MapReply::new_desde_resp(contenido_resp)?)),
            //VERBATIM_STRING => leer_y_convertir(&mut reader, interpretar_verbatim_string, VerbatimString::new_desde_resp),
            _ => Err(DatoRedis::new_null()),
        }
    }
}

impl TipoDatoRedis for DatoRedis {
    /// Convierte un struct de tipo Dato Redis a su representacion resp
    fn convertir_a_protocolo_resp(&self) -> String {
        match self {
            DatoRedis::BulkString(bulk_string) => bulk_string.convertir_a_protocolo_resp(),
            DatoRedis::Arrays(arrays) => arrays.convertir_a_protocolo_resp(),
            DatoRedis::Integer(integer) => integer.convertir_a_protocolo_resp(),
            DatoRedis::Set(set) => set.convertir_a_protocolo_resp(),
            DatoRedis::SimpleString(simple_string) => simple_string.convertir_a_protocolo_resp(),
            DatoRedis::VerbatimString(verbatim_string) => {
                verbatim_string.convertir_a_protocolo_resp()
            }
            DatoRedis::Null(null) => null.convertir_a_protocolo_resp(),
            DatoRedis::SimpleError(simple_error) => simple_error.convertir_a_protocolo_resp(),
            DatoRedis::Map(map_reply) => map_reply.convertir_a_protocolo_resp(),
            DatoRedis::MovedError(moved_error) => moved_error.convertir_a_protocolo_resp(),
        }
    }

    /// Convierte un struct de tipo Dato Redis a un String legible para el cliente
    fn convertir_resp_a_string(&self) -> String {
        match self {
            DatoRedis::BulkString(bulk_string) => bulk_string.convertir_resp_a_string(),
            DatoRedis::Arrays(arrays) => arrays.convertir_resp_a_string(),
            DatoRedis::Integer(integer) => integer.convertir_resp_a_string(),
            DatoRedis::Set(set) => set.convertir_resp_a_string(),
            DatoRedis::SimpleString(simple_string) => simple_string.convertir_resp_a_string(),
            DatoRedis::VerbatimString(verbatim_string) => verbatim_string.convertir_resp_a_string(),
            DatoRedis::Null(null) => null.convertir_resp_a_string(),
            DatoRedis::SimpleError(simple_error) => simple_error.convertir_resp_a_string(),
            DatoRedis::Map(map_reply) => map_reply.convertir_resp_a_string(),
            DatoRedis::MovedError(moved_error) => moved_error.convertir_resp_a_string(),
        }
    }
}
