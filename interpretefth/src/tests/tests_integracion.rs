#![cfg(test)]

mod tests {

    use std::{
        fs::File,
        io::{BufRead, BufReader},
    };

    use crate::{estructuras::errores::Error, interpretar_texto};

    fn interpretar_archivo(f: File) -> Vec<String> {
        let buffer = BufReader::new(f);
        let mut lineas: Vec<String> = Vec::new();

        for linea in buffer.lines() {
            match linea {
                Ok(comando) => {
                    lineas.push(comando);
                }
                Err(_) => lineas.push(format!("{}", Error::OperationFail)),
            }
        }

        lineas
    }

    fn ejecutar_test(ruta: &str, out_esperado: String, stack_esperado: String) -> bool {
        let f = File::open(ruta).unwrap();
        let texto = interpretar_archivo(f);
        let (output, stack) = interpretar_texto(texto);
        output == out_esperado && stack == stack_esperado
    }

    #[test]

    fn test_integracion_1() {
        let ruta = "src/tests/integracion/integracion_1.fth";
        let out = "".to_string();
        let stack = "2 3 2 ".to_string();
        assert!(ejecutar_test(ruta, out, stack));
    }

    #[test]

    fn test_integracion_2() {
        let ruta = "src/tests/integracion/integracion_2.fth";
        let out = "division-by-zero";
        let stack = "".to_string();
        assert!(ejecutar_test(ruta, out.to_string(), stack));
    }

    #[test]
    fn test_integracion_3() {
        let ruta = "src/tests/integracion/integracion_3.fth";
        let out = "9 2 ";
        let stack = "".to_string();
        assert!(ejecutar_test(ruta, out.to_string(), stack));
    }

    #[test]
    fn test_integracion_4() {
        let ruta = "src/tests/integracion/integracion_4.fth";
        let out = "5 5 \n8 ";
        let stack = "".to_string();
        assert!(ejecutar_test(ruta, out.to_string(), stack));
    }

    #[test]
    fn test_integracion_5() {
        let ruta = "src/tests/integracion/integracion_5.fth";
        let out = "3 4 2 ";
        let stack = "2 ".to_string();
        assert!(ejecutar_test(ruta, out.to_string(), stack));
    }

    #[test]
    fn test_integracion_6() {
        let ruta = "src/tests/integracion/integracion_6.fth";
        let out: &str = "falso ";
        let stack = "1 ".to_string();
        assert!(ejecutar_test(ruta, out.to_string(), stack));
    }

    #[test]
    fn test_integracion_7() {
        let ruta = "src/tests/integracion/integracion_7.fth";
        let out: &str = "D C B A ";
        let stack = "4 5 ".to_string();
        assert!(ejecutar_test(ruta, out.to_string(), stack));
    }
}
