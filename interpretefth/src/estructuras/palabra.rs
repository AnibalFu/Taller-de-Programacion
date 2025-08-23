use std::collections::HashMap;

/// Representa una word/palabra
///
/// # Atributos publicos
/// - 'versiones': cantidad de definiciones disponibles de la palabra
/// - 'definiciones': vector de definiciones (vectores de string) disponibles
pub struct Palabra {
    pub versiones: isize,
    referencias: HashMap<String, isize>,
    ultima_referencia: isize,
    pub definiciones: Vec<Vec<String>>,
}

impl Palabra {
    /// Crea una palabra
    ///
    /// # Parametros:
    /// - 'definicion': primera definicion de la palabra
    /// - 'referencias': palabras a las que referencia, y a que version
    pub fn new(definicion: Vec<String>, referencias: HashMap<String, isize>) -> Palabra {
        Palabra {
            versiones: 1,
            referencias,
            ultima_referencia: -1,
            definiciones: vec![definicion],
        }
    }

    /// Agrega una definicion a la lista de definiciones
    ///
    /// # Parametros:
    /// - 'definicion': primera definicion de la palabra
    pub fn agregar_definicion(&mut self, definicion: Vec<String>) {
        if self.versiones == self.ultima_referencia + 1 {
            self.versiones += 1;
        } else {
            self.definiciones.pop();
        }
        self.definiciones.push(definicion);
        self.ultima_referencia += 1;
    }

    /// Agrega una referencia a una version de una palabra
    ///
    /// # Parametros:
    /// - 'nombre_ref': nombre de la palabra que referencia
    /// - 'version_ref': version de la palabra que referencia
    pub fn agregar_referencia(&mut self, nombre_ref: String, version_ref: isize) {
        self.referencias.insert(nombre_ref, version_ref);
    }

    /// Devuelve que version referencia de una palabra, None si la misma no es referenciada por self
    ///
    /// # Parametros:
    /// - 'nombre_ref': nombre de la palabra que referencia
    pub fn version_referencia(&self, nombre_ref: String) -> Option<isize> {
        if let Some(version) = self.referencias.get(&nombre_ref) {
            return Some(*version);
        }
        None
    }

    /// Obtiene la definicion correspondiente a una version de la palabra de ser una version valida
    ///
    /// # Parametros:
    /// - 'n': numero de version deseado. De ser -1, devuelve la ultima version
    pub fn obtener_version(&self, n: isize) -> Option<&Vec<String>> {
        if n > self.versiones {
            return None;
        } else if n == -1 {
            return self.definiciones.get((self.versiones - 1) as usize);
        }
        self.definiciones.get((n - 1) as usize)
    }

    /// Indica que la palabra fue referenciada en su version actual
    pub fn es_referenciada(&mut self) {
        self.ultima_referencia = self.versiones - 1;
    }
}
