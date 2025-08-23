//! Este modulo contiene la implementacion del tipo de dato redis Array

use super::utils::obtener_elemento;
use crate::tipos_datos::traits::{DatoRedis, TipoDatoRedis};
use std::mem::discriminant;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Arrays {
    contenido: Vec<DatoRedis>,
    es_nulo: bool,
}

impl Arrays {
    pub fn new() -> Self {
        Arrays {
            contenido: Vec::new(),
            es_nulo: false,
        }
    }

    pub fn new_con_contenido(contenido: Vec<DatoRedis>) -> Self {
        Arrays {
            contenido,
            es_nulo: false,
        }
    }

    /// Crea un dato de redis Array a partir de un String en
    /// formato RESP
    ///
    /// # Parametros
    /// * `array_resp`: String resp a interpretar
    ///
    /// # Retorna
    /// - Array en caso de exito, error simple de redis en otro caso
    pub fn new_desde_resp(array_resp: String) -> Result<Self, DatoRedis> {
        let elementos = Self::obtener_arreglo(array_resp)?;
        let largo = elementos.len();
        Ok(Arrays {
            contenido: elementos,
            es_nulo: largo == 0,
        })
    }

    /// Determina si es un arreglo nulo
    pub fn es_nulo(&self) -> bool {
        self.es_nulo
    }

    /// Agrega un elemento al final del arreglo
    ///
    /// # Parametros
    /// * `dato`: Dato Redis a agregar
    pub fn append(&mut self, dato: DatoRedis) {
        self.contenido.push(dato);
    }

    /// Obtiene el elemento en una posicion del arreglo
    ///
    /// # Parametros
    /// * `index`: indice del elemento
    ///
    /// # Retorna
    /// - option del elemento, de existir
    pub fn get(&self, index: usize) -> Option<DatoRedis> {
        self.contenido.get(index).cloned()
    }

    /// Modifica el valor de un elemento del arreglo
    ///
    /// # Parametros
    /// * `index`: indice del elemento a modificar
    /// * `dato`: Dato Redis, nuevo valor
    ///
    /// # Retorna
    /// - () en caso de exit, error simple de redis en otro caso
    pub fn set(&mut self, index: usize, dato: DatoRedis) -> Result<(), DatoRedis> {
        if index >= self.contenido.len() {
            return Err(DatoRedis::new_simple_error(
                "ERR".to_string(),
                "index out of range".to_string(),
            ));
        }
        self.contenido[index] = dato;
        Ok(())
    }

    /// Inserta un elemento en una posicion del arreglo, moviendo
    /// los elementos siguientes a la derecha
    ///
    /// # Parametros
    /// * `index`: indice donde agregar el elemento
    /// * `dato`: Dato Redis a agregar
    pub fn insert(&mut self, index: usize, dato: DatoRedis) -> Result<(), DatoRedis> {
        if index > self.contenido.len() {
            return Err(DatoRedis::new_simple_error(
                "ERR".to_string(),
                "index out of range".to_string(),
            ));
        }
        self.contenido.insert(index, dato);
        Ok(())
    }

    /// Remueve un elemento en una posicion del arreglo, moviendo
    /// los elementos siguientes a la izquierda
    ///
    /// # Parametros
    /// * `index`: indice donde agregar el elemento
    ///
    /// # Retorna
    /// - El elemento removido en caso de exito, error de redis en otro caso
    pub fn remove(&mut self, index: usize) -> Result<DatoRedis, DatoRedis> {
        if index >= self.contenido.len() {
            return Err(DatoRedis::new_simple_error(
                "ERR".to_string(),
                "index out of range".to_string(),
            ));
        }
        Ok(self.contenido.remove(index))
    }

    /// Remueve el ultimo elemento del arreglo
    ///
    /// # Retorna
    /// - El elemento removido en caso de exito, error de redis en otro caso
    pub fn pop(&mut self) -> Result<DatoRedis, DatoRedis> {
        if self.contenido.is_empty() {
            Err(DatoRedis::new_simple_error(
                "ERR".to_string(),
                "index out of range".to_string(),
            ))
        } else {
            if let Some(dato) = self.contenido.pop() {
                return Ok(dato);
            }
            Err(DatoRedis::new_null())
        }
    }

    /// Vacia el arreglo
    pub fn clear(&mut self) {
        self.contenido.clear();
    }

    /// Determina si el arreglo esta vacio
    pub fn is_empty(&self) -> bool {
        self.contenido.is_empty()
    }

    /// Retorna la cardinalidad del arreglo
    pub fn len(&self) -> usize {
        self.contenido.len()
    }

    /// Retorna el iterador del arreglo
    pub fn iter(&self) -> std::slice::Iter<DatoRedis> {
        self.contenido.iter()
    }

    /// Obtiene un rango del arreglo
    ///
    /// # Parametros
    /// * `start`: indice de inicio
    /// * `end`: indice de fin
    ///
    /// # Retorna
    /// - El elemento removido en caso de exito, error de redis en otro caso
    pub fn range(&self, start: usize, end: usize) -> Arrays {
        let contenido: Vec<DatoRedis> = self
            .contenido
            .iter()
            .enumerate()
            .filter_map(|(i, elem)| {
                if i >= start && i <= end {
                    Some(elem.clone())
                } else {
                    None
                }
            })
            .collect();

        Arrays::new_con_contenido(contenido)
    }

    /// Devuelve `true` si en todo el árbol existe **algún**
    /// `DatoRedis` de la misma variante que `target`.
    pub fn contains_dato(&self, target: &DatoRedis) -> bool {
        self.contains_where(&|d| discriminant(d) == discriminant(target))
    }

    /// Reutilizamos el método genérico recursivo
    fn contains_where<F>(&self, pred: &F) -> bool
    where
        F: Fn(&DatoRedis) -> bool,
    {
        if self.es_nulo {
            return false;
        }
        self.contenido
            .iter()
            .any(|d| matches!(d, dato if pred(dato)))
    }

    /// A partir de un string en formato resp, sin el caracter inicial *,
    /// retorna un vector de datos redis
    ///
    /// # Parametros:
    /// * `arreglo_resp`: Representacion resp del arreglo
    ///
    /// # Retorna
    /// - un vector de datos redis en caso de exito, error simple en otro caso
    fn obtener_arreglo(arreglo_resp: String) -> Result<Vec<DatoRedis>, DatoRedis> {
        let largo_arreglo_str = Self::obtener_largo(&arreglo_resp, &mut String::new())?;
        let digitos_largo = largo_arreglo_str.len();
        if let Ok(largo) = largo_arreglo_str.parse::<usize>() {
            let mut elementos: Vec<DatoRedis> = Vec::new();
            let mut indice_fin = digitos_largo + 3;
            for _ in 0..largo {
                let resto = &arreglo_resp[indice_fin..].to_string();
                if let Ok((elemento, indice_final)) = obtener_elemento(resto) {
                    elementos.push(elemento);
                    indice_fin += indice_final;
                } else if arreglo_resp.chars().nth(indice_fin) == Some('*') {
                    let arr_anidado = Self::obtener_arreglo(resto.to_string())?;
                    let largo_arr_anidado = arr_anidado.len();
                    let arr = Arrays {
                        contenido: arr_anidado,
                        es_nulo: largo_arr_anidado == 0,
                    };
                    let mut largo = arr.convertir_a_protocolo_resp().len();
                    if resto.chars().nth(1) == Some('0') && resto.chars().nth(2) == Some('\r') {
                        largo = 0;
                    }
                    indice_fin += largo;
                    elementos.push(DatoRedis::Arrays(arr));
                } else {
                    return Err(DatoRedis::new_simple_error(
                        "txt".to_string(),
                        "Array invalido\n".to_string(),
                    ));
                }
            }
            if elementos.len() == largo {
                return Ok(elementos);
            }
        }
        Err(DatoRedis::new_simple_error(
            "txt".to_string(),
            "Array invalido\n".to_string(),
        ))
    }

    /// Obtiene el largo de un arreglo a partir de una cadena que
    /// lo representa
    ///
    /// # Parametros
    /// * `arreglo_resp`: String resp a interpretar
    /// * `largo_entrada`: largo en chars de la entrada
    ///
    /// # Retorna
    /// - string indicando el largo del arreglo en caso de exito,
    ///   error simple de redis en otro caso
    fn obtener_largo(
        arreglo_resp: &str,
        largo_arreglo_str: &mut String,
    ) -> Result<String, DatoRedis> {
        if arreglo_resp.is_empty() {
            return Err(DatoRedis::new_simple_error(
                "txt".to_string(),
                "Array invalido\n".to_string(),
            ));
        }
        if arreglo_resp.chars().nth(0) != Some('*') {
            return Err(DatoRedis::new_simple_error(
                "txt".to_string(),
                "Array invalido\n".to_string(),
            ));
        }
        let caracteres = &arreglo_resp[1..];
        for caracter in caracteres.chars() {
            if caracter.is_ascii_digit() {
                largo_arreglo_str.push(caracter);
            } else {
                break;
            }
        }
        Ok(largo_arreglo_str.to_string())
    }
}

impl TipoDatoRedis for Arrays {
    fn convertir_a_protocolo_resp(&self) -> String {
        if self.es_nulo {
            return "*-1\r\n".to_string();
        }
        let mut resultado = format!("*{}\r\n", self.contenido.len());
        for dato in &self.contenido {
            resultado.push_str(&dato.convertir_a_protocolo_resp());
        }
        resultado
    }

    fn convertir_resp_a_string(&self) -> String {
        self.convertir_con_indentacion(0)
    }
}

impl Arrays {
    /// Transforma un Array en una representacion de String
    /// enumerada
    ///
    /// # Parametros:
    /// * `nivel`: nivel de anidamiento en el arreglo
    ///
    /// # Retorna
    /// - Representacion String del arreglo
    fn convertir_con_indentacion(&self, nivel: usize) -> String {
        let mut resultado = String::new();

        if self.contenido.is_empty() {
            resultado.push_str("(empty array)\r\n")
        }

        for (i, dato) in self.contenido.iter().enumerate() {
            resultado.push_str(&"\t".repeat(nivel));
            resultado.push_str(format!("{}) ", i + 1).as_str());

            match &dato {
                DatoRedis::Arrays(array) => {
                    resultado.push_str(&array.convertir_con_indentacion(nivel + 1));
                }
                _ => {
                    resultado.push_str(&dato.convertir_resp_a_string());
                }
            }
        }

        resultado
    }
}

impl Default for Arrays {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::tipos_datos::arrays::Arrays;
    use crate::tipos_datos::traits::{DatoRedis, TipoDatoRedis};

    #[test]
    fn test_01_arreglo_vacio_formato_valido() {
        let arreglo = Arrays::new();
        let resultado_esperado = "*0\r\n".to_string();
        let resultado_obtenido = arreglo.convertir_a_protocolo_resp();

        assert_eq!(resultado_esperado, resultado_obtenido)
    }

    #[test]
    fn test_02_arreglo_nulo_formato_valido() {
        let arreglo = Arrays {
            contenido: Vec::new(),
            es_nulo: true,
        };
        let resultado_esperado = "*-1\r\n".to_string();
        let resultado_obtenido = arreglo.convertir_a_protocolo_resp();

        assert_eq!(resultado_esperado, resultado_obtenido)
    }

    #[test]
    fn test_02_arreglo_de_cadena_formato_valido() {
        let mut arreglo = Arrays::new();

        let elem1 = DatoRedis::new_bulk_string("Hola mundo".to_string()).unwrap();
        let elem2 = DatoRedis::new_bulk_string("Chau mundo".to_string()).unwrap();
        let elem3 = DatoRedis::new_bulk_string(" ".to_string()).unwrap();

        arreglo.append(elem1);
        arreglo.append(elem2);
        arreglo.append(elem3);

        let resultado_esperado =
            "*3\r\n$10\r\nHola mundo\r\n$10\r\nChau mundo\r\n$1\r\n \r\n".to_string();
        let resultado_obtenido = arreglo.convertir_a_protocolo_resp();

        assert_eq!(resultado_esperado, resultado_obtenido)
    }

    #[test]
    fn test_03_arreglo_nulo_no_permite_append() {
        let mut arreglo = Arrays {
            contenido: Vec::new(),
            es_nulo: true,
        };
        let elem = DatoRedis::new_bulk_string("dato".to_string()).unwrap();
        arreglo.append(elem);

        // Aún después de hacer append, si es_nulo sigue siendo true, el RESP es *-1\r\n
        assert!(arreglo.es_nulo());
        assert_eq!(arreglo.convertir_a_protocolo_resp(), "*-1\r\n");
    }

    #[test]
    fn test_04_get_set_y_remove_funcionan() {
        let mut arreglo = Arrays::new();
        let elem = DatoRedis::new_bulk_string("dato".to_string()).unwrap();
        arreglo.append(elem);

        assert_eq!(arreglo.len(), 1);
        assert!(arreglo.get(0).is_some());

        let nuevo = DatoRedis::new_bulk_string("nuevo".to_string()).unwrap();
        assert!(arreglo.set(0, nuevo).is_ok());

        let eliminado = arreglo.remove(0);
        assert!(eliminado.is_ok());
        assert!(arreglo.is_empty());
    }

    #[test]
    fn test_05_set_falla_con_indice_invalido() {
        let mut arreglo = Arrays::new();
        let elem = DatoRedis::new_bulk_string("dato".to_string()).unwrap();
        let resultado = arreglo.set(5, elem);
        assert!(resultado.is_err());
    }

    #[test]
    fn test_06_remove_falla_con_indice_invalido() {
        let mut arreglo = Arrays::new();
        let resultado = arreglo.remove(0);
        assert!(resultado.is_err());
    }

    #[test]
    fn test_07_clear_vacia_el_arreglo() {
        let mut arreglo = Arrays::new();
        let elem1 = DatoRedis::new_bulk_string("uno".to_string()).unwrap();
        let elem2 = DatoRedis::new_bulk_string("dos".to_string()).unwrap();

        arreglo.append(elem1);
        arreglo.append(elem2);

        assert_eq!(arreglo.len(), 2);
        arreglo.clear();
        assert_eq!(arreglo.len(), 0);
        assert!(arreglo.is_empty());
    }

    #[test]
    fn test_08_arreglo_de_arreglos_formato_valido() {
        // Arreglo interior vacio: *0\r\n
        let arreglo_vacio = DatoRedis::new_array();

        // Arreglo interior con un BulkString: *1\r\n$5\r\nhello\r\n
        let mut arreglo_con_dato = DatoRedis::new_array();
        let string = DatoRedis::new_bulk_string("hello".to_string()).unwrap();
        if let DatoRedis::Arrays(ref mut arr) = arreglo_con_dato {
            arr.append(string);
        } else {
            panic!("Error: arreglo_con_dato no es un arreglo");
        }

        // Arreglo principal con dos arreglos: *2\r\n... ...
        let mut arreglo_principal = Arrays::new();
        arreglo_principal.append(arreglo_vacio);
        arreglo_principal.append(arreglo_con_dato);

        let resultado_esperado = "*2\r\n*0\r\n*1\r\n$5\r\nhello\r\n".to_string();
        let resultado_obtenido = arreglo_principal.convertir_a_protocolo_resp();

        assert_eq!(resultado_esperado, resultado_obtenido);
    }

    #[test]
    fn test_09_arreglo_de_enteros_formato_valido() {
        let mut arreglo = Arrays::new();

        let int1 = DatoRedis::new_integer(42);
        let int2 = DatoRedis::new_integer(-7);
        let int3 = DatoRedis::new_integer(0);

        arreglo.append(int1);
        arreglo.append(int2);
        arreglo.append(int3);

        let resultado_esperado = "*3\r\n:42\r\n:-7\r\n:0\r\n".to_string();
        let resultado_obtenido = arreglo.convertir_a_protocolo_resp();

        assert_eq!(resultado_esperado, resultado_obtenido);
    }

    #[test]
    fn test_10_arreglo_mixto_formato_valido() {
        let mut arreglo = Arrays::new();

        let str1 = DatoRedis::new_bulk_string("Hola".to_string()).unwrap();
        let num = DatoRedis::new_integer(100);
        let str2 = DatoRedis::new_bulk_string("Chau".to_string()).unwrap();

        arreglo.append(str1);
        arreglo.append(num);
        arreglo.append(str2);

        let resultado_esperado = "*3\r\n$4\r\nHola\r\n:100\r\n$4\r\nChau\r\n".to_string();
        let resultado_obtenido = arreglo.convertir_a_protocolo_resp();

        assert_eq!(resultado_esperado, resultado_obtenido);
    }

    #[test]
    fn test_11_arreglo_int_a_partir_de_resp() {
        let arreglo = Arrays::new_desde_resp("*3\r\n:1\r\n:3\r\n:334\r\n".to_string()).unwrap();
        assert_eq!(arreglo.len(), 3);
        let resultado_esperado = "*3\r\n:1\r\n:3\r\n:334\r\n".to_string();
        let resultado_obtenido = arreglo.convertir_a_protocolo_resp();
        assert_eq!(resultado_esperado, resultado_obtenido);
    }

    #[test]
    fn test_12_arreglo_bulk_str_a_partir_de_resp() {
        let arreglo = Arrays::new_desde_resp("*2\r\n$3\r\nabc\r\n$1\r\nd\r\n".to_string()).unwrap();
        assert_eq!(arreglo.len(), 2);
        let resultado_esperado = "*2\r\n$3\r\nabc\r\n$1\r\nd\r\n".to_string();
        let resultado_obtenido = arreglo.convertir_a_protocolo_resp();
        assert_eq!(resultado_esperado, resultado_obtenido);
    }

    #[test]
    fn test_13_arreglo_simple_str_a_partir_de_resp() {
        let arreglo =
            Arrays::new_desde_resp("*4\r\n+abc\r\n+123\r\n+qwert\r\n+aa\r\n".to_string()).unwrap();
        assert_eq!(arreglo.len(), 4);
        let resultado_esperado = "*4\r\n+abc\r\n+123\r\n+qwert\r\n+aa\r\n".to_string();
        let resultado_obtenido = arreglo.convertir_a_protocolo_resp();
        assert_eq!(resultado_esperado, resultado_obtenido);
    }

    #[test]
    fn test_14_arreglo_mixto_a_partir_de_resp() {
        let arreglo =
            Arrays::new_desde_resp("*4\r\n+abc\r\n:-123\r\n+qwert\r\n$2\r\nab\r\n".to_string())
                .unwrap();
        assert_eq!(arreglo.len(), 4);
        let resultado_esperado = "*4\r\n+abc\r\n:-123\r\n+qwert\r\n$2\r\nab\r\n".to_string();
        let resultado_obtenido = arreglo.convertir_a_protocolo_resp();
        assert_eq!(resultado_esperado, resultado_obtenido);
    }

    #[test]
    fn test_15_arreglo_a_string() {
        let arreglo =
            Arrays::new_desde_resp("*4\r\n+abc\r\n:-123\r\n+qwert\r\n$2\r\nab\r\n".to_string())
                .unwrap();
        let contenido = arreglo.convertir_resp_a_string();
        assert_eq!(
            contenido,
            "1) abc\r\n2) (integer) -123\r\n3) qwert\r\n4) \"ab\"\r\n"
        )
    }

    #[test]
    fn test_16_arreglo_a_string1() {
        // let arreglo = Arrays::new_desde_resp("*2\r\n*0\r\n*1\r\n$5\r\nhello\r\n".to_string()).unwrap();

        let mut arreglo1 = Arrays::new();

        let elem1 = DatoRedis::new_bulk_string("uno".to_string()).unwrap();
        let elem2 = DatoRedis::new_bulk_string("dos".to_string()).unwrap();
        arreglo1.append(elem1);
        arreglo1.append(elem2);

        let mut arreglo2 = Arrays::new();
        let elem1 = DatoRedis::new_bulk_string("tres".to_string()).unwrap();
        let elem2 = DatoRedis::new_bulk_string("cuatro".to_string()).unwrap();
        arreglo2.append(elem1);
        arreglo2.append(elem2);

        let mut arreglo3 = Arrays::new();
        arreglo3.append(DatoRedis::Arrays(arreglo1));
        arreglo3.append(DatoRedis::Arrays(arreglo2));
        let elem3 = DatoRedis::new_bulk_string("cinco".to_string()).unwrap();
        arreglo3.append(elem3);

        let contenido = arreglo3.convertir_resp_a_string();
        assert_eq!(
            contenido,
            "1) \t1) \"uno\"\r\n\t2) \"dos\"\r\n2) \t1) \"tres\"\r\n\t2) \"cuatro\"\r\n3) \"cinco\"\r\n"
        )
    }

    #[test]
    fn test_17_arreglo_anidado_resp_a_array() {
        let arreglo =
            Arrays::new_desde_resp("*2\r\n*0\r\n*1\r\n$5\r\nhello\r\n".to_string()).unwrap();
        assert_eq!(arreglo.len(), 2);
        let arreglo = Arrays::new_desde_resp(
            "*2\r\n*3\r\n$2\r\nab\r\n+abc\r\n:25\r\n*1\r\n$5\r\nhello\r\n".to_string(),
        )
        .unwrap();
        assert_eq!(
            arreglo.convertir_a_protocolo_resp(),
            "*2\r\n*3\r\n$2\r\nab\r\n+abc\r\n:25\r\n*1\r\n$5\r\nhello\r\n".to_string()
        );
    }

    #[test]
    fn test_18_arreglo_resp_a_array() {
        let arreglo =
            Arrays::new_desde_resp("*4\r\n+hola\r\n$7\r\n¡hello\r\n+¡a\r\n:2\r\n".to_string())
                .unwrap();
        assert_eq!(arreglo.len(), 4);
        if let Some(bs) = arreglo.get(1) {
            assert_eq!(bs.convertir_a_protocolo(), "$7\r\n¡hello\r\n");
        } else {
            panic!();
        }
        if let Some(ss) = arreglo.get(2) {
            assert_eq!(ss.convertir_a_protocolo(), "+¡a\r\n");
        } else {
            panic!();
        }
        if let Some(int) = arreglo.get(3) {
            assert_eq!(int.convertir_a_protocolo(), ":2\r\n");
        } else {
            panic!();
        }
    }

    #[test]
    fn test_19_arreglo_con_simple_error() {
        let arreglo =
            Arrays::new_desde_resp("*2\r\n$4\r\nhola\r\n-MOVED 6539\r\n".to_string()).unwrap();
        let result = arreglo.convertir_a_protocolo_resp();
        assert_eq!(result, "*2\r\n$4\r\nhola\r\n-MOVED 6539\r\n");
    }

    #[test]
    fn test_20_arreglo_contiene_simple_error() {
        let arreglo =
            Arrays::new_desde_resp("*2\r\n$4\r\nhola\r\n-MOVED 6539\r\n".to_string()).unwrap();
        let result = arreglo.contains_dato(&DatoRedis::new_simple_error(
            "-MOVED".to_string(),
            "6539".into(),
        ));
        assert!(result);
    }
}
