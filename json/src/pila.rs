//! Este módulo contiene la estructura pila utilizada
//! para el parseo de estructuras Json
use common::pila::PilaGenerica;

use crate::json::ExpresionJson;

pub struct Pila<T> {
    pila: PilaGenerica<T>,
}

impl<T> Pila<T> {
    /// Crea una pila vacía con la capacidad pedida
    ///
    /// # Parámetros
    /// - `capacidad`: capacidad de la pila
    ///
    /// # Retorna
    /// - Pila vacía
    pub fn crear(capacidad: usize) -> Pila<T> {
        let pila = PilaGenerica::crear(capacidad);
        Pila { pila }
    }

    /// Apila un elemento, si la capacidad lo permite
    ///
    /// # Parámetros
    /// - `elemento`: elemento a apilar
    ///
    /// # Retorna
    /// - None en caso de éxito, Error de ExpresionJson en otro caso
    pub fn apilar(&mut self, elemento: T) -> Result<Option<T>, ExpresionJson> {
        self.pila
            .apilar(elemento)
            .map_err(|_| ExpresionJson::new_invalid_json_err())
    }

    /// Desapila un elemento, si la pila no está vacía
    ///
    /// # Retorna
    /// - Referencia al elemento desapilado en caso de éxito, Error de
    ///   ExpresionJson en otro caso
    pub fn desapilar(&mut self) -> Result<&T, ExpresionJson> {
        self.pila
            .desapilar()
            .map_err(|_| ExpresionJson::new_invalid_json_err())
    }

    /// Determina si la pila está vacía
    ///
    /// # Retorna
    /// - Verdadero si esta vacía, falso en otro caso
    pub fn esta_vacia(&mut self) -> bool {
        self.pila.esta_vacia()
    }

    /// Muestra el tope, si la pila no está vacía
    ///
    /// # Retorna
    /// - Referencia al tope en caso de éxito, Error de
    ///   ExpresionJson en otro caso
    pub fn ver_tope(&mut self) -> Result<&T, ExpresionJson> {
        self.pila
            .ver_tope()
            .map_err(|_| ExpresionJson::new_invalid_json_err())
    }
}

#[test]
pub fn test_apilar() {
    let mut pila = Pila::crear(2);
    let _ = pila.apilar(3);
    assert_eq!(pila.ver_tope().unwrap(), &3);

    let _ = pila.apilar(7);
    assert_eq!(pila.ver_tope().unwrap(), &7);

    assert!(pila.apilar(4).is_err());
}

#[test]
pub fn test_desapilar() {
    let mut pila = Pila::crear(5);
    let _ = pila.apilar(3);
    let _ = pila.apilar(7);

    assert_eq!(pila.desapilar().unwrap(), &7);
    assert_eq!(pila.desapilar().unwrap(), &3);
    assert!(pila.desapilar().is_err());
}

#[test]
pub fn test_estavacia() {
    let mut pila = Pila::crear(5);
    assert!(pila.esta_vacia());

    let _ = pila.apilar("s1".to_string());
    let _ = pila.apilar("s2".to_string());
    assert!(!pila.esta_vacia());

    let _ = pila.desapilar();
    let _ = pila.desapilar();
    assert!(pila.esta_vacia());
}

#[test]
pub fn test_integracion() {
    let mut pila = Pila::crear(5);
    assert!(pila.esta_vacia());

    let _ = pila.apilar("s1".to_string());
    let _ = pila.apilar("s2".to_string());
    let _ = pila.apilar("s3".to_string());
    assert_eq!(pila.desapilar().unwrap(), &("s3".to_string()));
    assert!(!pila.esta_vacia());
    assert_eq!(pila.ver_tope().unwrap(), &("s2".to_string()));

    assert_eq!(pila.desapilar().unwrap(), &("s2".to_string()));
    assert_eq!(pila.desapilar().unwrap(), &("s1".to_string()));
    assert!(pila.desapilar().is_err());
    assert!(pila.esta_vacia());
}
