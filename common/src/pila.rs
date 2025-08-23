use std::fmt::Error;

/// Representa una pila de tipo generico
pub struct PilaGenerica<T> {
    capacidad: usize,
    largo: usize,
    elementos: Vec<T>,
}

impl<T> PilaGenerica<T> {
    /// Crea una pila
    ///
    /// # Parametros:
    /// - 'capacidad': cantidad maxima de elementos a almacenar
    pub fn crear(capacidad: usize) -> PilaGenerica<T> {
        PilaGenerica {
            capacidad,
            largo: 0,
            elementos: Vec::with_capacity(capacidad),
        }
    }

    /// Apila un elemento
    ///
    /// # Parametros:
    /// - 'elemento': elemento a apilar
    ///
    /// De superar la capacidad definida, lanza error
    pub fn apilar(&mut self, elemento: T) -> Result<Option<T>, Error> {
        if self.largo == self.capacidad {
            return Err(Error);
        }
        if self.largo < self.elementos.len() {
            self.elementos[self.largo] = elemento;
        } else {
            self.elementos.push(elemento);
        }

        self.largo += 1;
        Ok(None)
    }

    /// Desapila un elemento
    ///
    /// De ser exitoso, devuelve el elemento desapilado, de estar vacia, devuelve error
    pub fn desapilar(&mut self) -> Result<&T, Error> {
        if self.esta_vacia() {
            return Err(Error);
        }
        self.largo -= 1;
        Ok(&(self.elementos[self.largo]))
    }

    /// Determina si la pila esta vacia, devolviendo verdadero en ese caso, falso en los otros
    pub fn esta_vacia(&mut self) -> bool {
        self.largo == 0
    }

    /// Devuelve el ultimo elemento apilado, sin desapilarlo, de estar vacia, lanza error
    pub fn ver_tope(&mut self) -> Result<&T, Error> {
        if self.esta_vacia() {
            return Err(Error);
        }
        Ok(&(self.elementos[self.largo - 1]))
    }
}

#[test]
pub fn test_apilar() {
    let mut pila = PilaGenerica::crear(2);
    let _ = pila.apilar(3);
    assert_eq!(pila.ver_tope().unwrap(), &3);

    let _ = pila.apilar(7);
    assert_eq!(pila.ver_tope().unwrap(), &7);

    assert!(pila.apilar(4).is_err());
}

#[test]
pub fn test_desapilar() {
    let mut pila = PilaGenerica::crear(5);
    let _ = pila.apilar(3);
    let _ = pila.apilar(7);

    assert_eq!(pila.desapilar().unwrap(), &7);
    assert_eq!(pila.desapilar().unwrap(), &3);
    assert!(pila.desapilar().is_err());
}

#[test]
pub fn test_estavacia() {
    let mut pila = PilaGenerica::crear(5);
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
    let mut pila = PilaGenerica::crear(5);
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
