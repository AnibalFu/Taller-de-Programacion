use std::cmp::max;

/// Representa una edición mínima a nivel de carácter entre dos cadenas.
///
/// Estas ediciones se generan al comparar dos versiones de una misma línea,
/// utilizando el algoritmo de LCS para determinar la menor cantidad de cambios requeridos.
enum CharEdit {
    /// Inserta el carácter `ch` en la posición `pos` (en la cadena original).
    Ins { pos: usize, ch: char },
    /// Elimina el carácter `ch` que se encuentra en la posición `pos` de la cadena original.
    Del { pos: usize, ch: char },
}

/// Representa una operación agrupada a nivel de línea o palabra.
///
/// Las ediciones individuales (`CharEdit`) se agrupan en operaciones más comprensibles
/// para formar comandos de inserción o eliminación de texto.
enum Op {
    /// Inserta una secuencia de caracteres en una posición específica.
    Insert {
        /// Posición en la línea original donde se debe insertar el contenido.
        pos: usize,
        /// Texto completo que debe insertarse.
        text: String,
    },
    /// Elimina una secuencia contigua de caracteres en un rango `[start, end]`.
    Delete {
        /// Índice de inicio del rango a eliminar (inclusive).
        start: usize,
        /// Índice final del rango a eliminar (inclusive).
        end: usize,
        /// Texto completo que debe eliminar.
        text: String,
    },
}

/// Construye la tabla de programación dinámica utilizada por el algoritmo de LCS.
///
/// Cada celda `opt[i][j]` representa la longitud de la subsecuencia común más larga
/// entre los prefijos `a[..i]` y `b[..j]`.
///
/// # Argumentos
///
/// - `a`: Primera secuencia de caracteres.
/// - `b`: Segunda secuencia de caracteres.
///
/// # Retorno
///
/// Una matriz `m+1 x n+1` con los valores del LCS.
///
/// # Complejidad
///
/// `O(m * n)`
fn lcs_table(a: &[char], b: &[char]) -> Vec<Vec<usize>> {
    let m = a.len();
    let n = b.len();
    let mut opt = vec![vec![0usize; n + 1]; m + 1];
    for i in 0..m {
        for (j, b_j) in b.iter().enumerate().take(n) {
            if a[i] == *b_j {
                opt[i + 1][j + 1] = opt[i][j] + 1;
            } else {
                opt[i + 1][j + 1] = max(opt[i + 1][j], opt[i][j + 1]);
            }
        }
    }
    opt
}

/// Determina la secuencia de ediciones atómicas (insertar/eliminar caracteres)
/// requeridas para transformar `old` en `new`.
///
/// # Argumentos
///
/// - `old`: Versión original de la línea.
/// - `new`: Versión modificada de la línea.
///
/// # Retorno
///
/// Un vector de `CharEdit` que representa las diferencias carácter por carácter.
fn raw_edits(old: &str, new: &str) -> Vec<CharEdit> {
    let a: Vec<char> = old.chars().collect();
    let b: Vec<char> = new.chars().collect();
    let t = lcs_table(&a, &b);
    let mut i = a.len();
    let mut j = b.len();
    let mut v = Vec::<CharEdit>::new();
    while i > 0 || j > 0 {
        if i > 0 && j > 0 && a[i - 1] == b[j - 1] {
            i -= 1;
            j -= 1;
        } else if j > 0 && (i == 0 || t[i][j - 1] >= t[i - 1][j]) {
            v.push(CharEdit::Ins {
                pos: i,
                ch: b[j - 1],
            });
            j -= 1;
        } else {
            v.push(CharEdit::Del {
                pos: i - 1,
                ch: a[i - 1],
            });
            i -= 1;
        }
    }
    v.reverse();
    v
}

/// Agrupa ediciones individuales (`CharEdit`) en operaciones legibles (`Op`).
///
/// Agrupa inserciones contiguas en la misma posición en un único `Insert`
/// y eliminaciones contiguas en rangos en un único `Delete`.
///
/// # Argumentos
///
/// - `raw`: Lista de ediciones atómicas generadas por `raw_edits`.
///
/// # Retorno
///
/// Vector de operaciones compuestas (`Op`).
fn group_ops(raw: &[CharEdit]) -> Vec<Op> {
    let mut ops = Vec::<Op>::new();
    let mut k = 0;
    while k < raw.len() {
        match &raw[k] {
            CharEdit::Ins { pos, .. } => {
                let p = *pos;
                let mut txt = String::new();
                let mut kk = k;
                while kk < raw.len() {
                    if let CharEdit::Ins { pos: q, ch } = raw[kk] {
                        if q == p {
                            txt.push(ch);
                            kk += 1;
                            continue;
                        }
                    }
                    break;
                }
                ops.push(Op::Insert { pos: p, text: txt });
                k = kk;
            }
            CharEdit::Del { pos: start, ch } => {
                let s = *start;
                let mut e = s;
                let mut txt = String::new();
                txt.push(*ch);
                let mut kk = k + 1;
                while kk < raw.len() {
                    if let CharEdit::Del { pos: q, ch } = raw[kk] {
                        if q == e + 1 {
                            txt.push(ch);
                            e += 1;
                            kk += 1;
                            continue;
                        }
                    }
                    break;
                }
                ops.push(Op::Delete {
                    start: s,
                    end: e,
                    text: txt,
                });
                k = kk;
            }
        }
    }
    ops
}

/// Recibe dos versiones de una línea y produce comandos atómicos de edición en formato string.
///
/// Cada comando sigue el formato:
/// - `"op:insert;pos:<pos>;content:<text>"`
/// - `"op:delete;start:<start>;end:<end>"`
///
/// # Argumentos
///
/// - `old`: Contenido original de la línea (sin `\n`).
/// - `new`: Contenido actualizado de la línea.
///
/// # Retorno
///
/// Un vector de strings, donde cada string es un comando que describe un cambio.
pub fn atomic_ops(old: &str, new: &str) -> Vec<String> {
    let edits = raw_edits(old, new);
    let ops = group_ops(&edits);
    ops.into_iter()
        .map(|op| match op {
            Op::Insert { pos, text } => {
                format!("op\\:insert\\;pos\\:{pos}\\;content\\:{text}")
            }
            Op::Delete { start, end, text } => {
                format!("op\\:delete\\;start\\:{start}\\;end\\:{end}\\;content\\:{text}")
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_only() {
        let result = atomic_ops("abc", "abxyzc");
        assert_eq!(result, vec!["op:insert;pos:2;content:xyz"]);
    }

    #[test]
    fn test_delete_only() {
        let result = atomic_ops("abcdef", "abf");
        assert_eq!(result, vec!["op:delete;start:2;end:4;content:cde"]);
    }

    #[test]
    fn test_no_change() {
        let result = atomic_ops("hello", "hello");
        assert_eq!(result, Vec::<String>::new());
    }

    #[test]
    fn test_full_replace() {
        let result = atomic_ops("abc", "xyz");
        assert_eq!(
            result,
            vec![
                "op:delete;start:0;end:2;content:abc",
                "op:insert;pos:3;content:xyz",
            ]
        );
    }

    #[test]
    fn test_insert_beginning1() {
        let result = atomic_ops("def", "abcde");
        assert_eq!(
            result,
            vec![
                "op:insert;pos:0;content:abc",
                "op:delete;start:2;end:2;content:f"
            ]
        );
    }

    #[test]
    fn test_insert_end() {
        let result = atomic_ops("123", "123abc");
        assert_eq!(result, vec!["op:insert;pos:3;content:abc"]);
    }

    #[test]
    fn test_delete_beginning() {
        let result = atomic_ops("abcxyz", "xyz");
        assert_eq!(result, vec!["op:delete;start:0;end:2;content:abc"]);
    }

    #[test]
    fn test_delete_end() {
        let result = atomic_ops("abcxyz", "abc");
        assert_eq!(result, vec!["op:delete;start:3;end:5;content:xyz"]);
    }

    #[test]
    fn test_empty_old() {
        let result = atomic_ops("", "hello");
        assert_eq!(result, vec!["op:insert;pos:0;content:hello"]);
    }

    #[test]
    fn test_empty_new() {
        let result = atomic_ops("goodbye", "");
        assert_eq!(result, vec!["op:delete;start:0;end:6;content:goodbye"]);
    }

    fn fmt(ops: Vec<String>) -> Vec<String> {
        ops.into_iter().map(|s| s.replace('\n', "\\n")).collect()
    }

    #[test]
    fn insert_spaces_symbols() {
        let ops = atomic_ops("Hello world!", "Hello world! :)");
        assert_eq!(fmt(ops), vec!["op:insert;pos:12;content: :)"]);
    }

    #[test]
    fn delete_with_punctuation() {
        let ops = atomic_ops("rust-lang", "rust");
        assert_eq!(fmt(ops), vec!["op:delete;start:4;end:8;content:-lang"]);
    }

    #[test]
    fn combined_insert_delete_middle() {
        let ops = atomic_ops("let mut x = 5;", "let x = 10;");
        assert_eq!(
            fmt(ops),
            vec![
                "op:delete;start:2;end:5;content:t mu",
                "op:delete;start:12;end:12;content:5",
                "op:insert;pos:13;content:10"
            ]
        );
    }

    #[test]
    fn insert_beginning_and_end() {
        let ops = atomic_ops("core", ">>core<<");
        assert_eq!(
            fmt(ops),
            vec!["op:insert;pos:0;content:>>", "op:insert;pos:4;content:<<"]
        );
    }

    #[test]
    fn delete_spaces_between_words() {
        let ops = atomic_ops("good bye world", "goodworld");
        assert_eq!(fmt(ops), vec!["op:delete;start:4;end:8;content: bye "]);
    }

    #[test]
    fn full_line_replace_with_spaces() {
        let ops = atomic_ops("the old line", "a new line");
        assert_eq!(
            fmt(ops),
            vec![
                "op:delete;start:0;end:2;content:the",
                "op:insert;pos:3;content:a",
                "op:delete;start:4;end:6;content:old",
                "op:insert;pos:7;content:new",
            ]
        );
    }

    #[test]
    fn insert_multibyte_unicode() {
        let ops = atomic_ops("hola", "hola🌎");
        assert_eq!(fmt(ops), vec!["op:insert;pos:4;content:🌎"]);
    }

    #[test]
    fn delete_unicode_char() {
        let ops = atomic_ops("smile🙂", "smile");
        assert_eq!(fmt(ops), vec!["op:delete;start:5;end:5;content:🙂"]);
    }

    #[test]
    fn insert_and_delete_in_words() {
        let ops = atomic_ops("color", "colour");
        assert_eq!(fmt(ops), vec!["op:insert;pos:4;content:u"]);
    }

    #[test]
    fn identical_lines() {
        let ops = atomic_ops("unchanged", "unchanged");
        assert!(ops.is_empty());
    }

    #[test]
    fn with_newline_characters() {
        let ops = atomic_ops("line1\nline2", "line1\nline2\nline3");
        assert_eq!(fmt(ops), vec!["op:insert;pos:11;content:\\nline3"]);
    }

    #[test]
    fn delete_newline_characters() {
        let ops = atomic_ops("line1\nline2", "line1");
        assert_eq!(fmt(ops), vec!["op:delete;start:5;end:10;content:\\nline2"]);
    }
}
