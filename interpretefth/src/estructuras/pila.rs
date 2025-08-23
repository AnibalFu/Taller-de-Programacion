use super::errores::Error;
use common::pila::PilaGenerica;

/// Representa una pila de tipo generico
pub struct Pila<T> {
    pila: PilaGenerica<T>,
}

impl<T> Pila<T> {
    /// Crea una pila
    ///
    /// # Parametros:
    /// - 'capacidad': cantidad maxima de elementos a almacenar
    pub fn crear(capacidad: usize) -> Pila<T> {
        let pila = PilaGenerica::crear(capacidad);
        Pila { pila }
    }

    /// Apila un elemento
    ///
    /// # Parametros:
    /// - 'elemento': elemento a apilar
    ///
    /// De superar la capacidad definida, lanza error
    pub fn apilar(&mut self, elemento: T) -> Result<Option<T>, Error> {
        self.pila.apilar(elemento).map_err(|_| Error::StackOverflow)
    }

    /// Desapila un elemento
    ///
    /// De ser exitoso, devuelve el elemento desapilado, de estar vacia, devuelve error
    pub fn desapilar(&mut self) -> Result<&T, Error> {
        self.pila.desapilar().map_err(|_| Error::StackUnderflow)
    }

    /// Determina si la pila esta vacia, devolviendo verdadero en ese caso, falso en los otros
    pub fn esta_vacia(&mut self) -> bool {
        self.pila.esta_vacia()
    }

    /// Devuelve el ultimo elemento apilado, sin desapilarlo, de estar vacia, lanza error
    pub fn ver_tope(&mut self) -> Result<&T, Error> {
        self.pila.ver_tope().map_err(|_| Error::StackUnderflow)
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
