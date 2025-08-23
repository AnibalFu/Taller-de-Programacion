//! Este modulo contiene los errores usados internamente al
//! operar con tipos de datos redis
use crate::tipos_datos::traits::DatoRedis;
use std::fmt;

#[derive(Debug)]
pub enum Error {
    ClaveNoEncontrada(DatoRedis),
    DatoIngresadoEsInvalido,
    CantidadIncorrectaDeArgumentos(String),
    RedireccionarCliente(String),
    ClaveNoEliminada(i8),

    // Nuevos errores
    ComandoDesconocido(String),
    TipoIncorrectoDeDato,
    IndiceFueraDeRango,
    SimpleStringInvalido,
    Otro(String), // Error genérico
    ParametrosInvalidos,
}

impl Error {
    pub fn new_error_null() -> Self {
        Error::ClaveNoEncontrada(DatoRedis::new_null())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::CantidadIncorrectaDeArgumentos(cmd) => {
                write!(
                    f,
                    "(error) ERR wrong number of arguments for '{cmd}' command"
                )
            }
            Error::DatoIngresadoEsInvalido => {
                write!(f, "(error) ERR invalid input data")
            }
            Error::RedireccionarCliente(slot) => {
                write!(f, "MOVED {slot} ")
            }
            Error::ClaveNoEliminada(value) => {
                write!(f, "{value}")
            }
            Error::ComandoDesconocido(cmd) => {
                write!(f, "(error) ERR unknown command '{cmd}'")
            }
            Error::TipoIncorrectoDeDato => {
                write!(
                    f,
                    "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
                )
            }
            Error::IndiceFueraDeRango => {
                write!(f, "(error) ERR index out of range")
            }
            Error::SimpleStringInvalido => {
                write!(
                    f,
                    "(error) ERR SimpleString contiene caracteres no permitidos"
                )
            }
            Error::Otro(mensaje) => {
                write!(f, "(error) ERR {mensaje}")
            }

            Error::ParametrosInvalidos => {
                write!(f, "(error) PARAMETROS_INVALIDOS")
            }

            Error::ClaveNoEncontrada(_) => {
                write!(f, "(error) ")
            }
        }
    }
}
