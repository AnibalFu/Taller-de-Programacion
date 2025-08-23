use crate::protocol::dataencryption::decrypt_resp;
use crate::tipos_datos::traits::DatoRedis;
use crate::{protocol::dataencryption::encrypt_resp, tipos_datos::constantes::*};
use std::io::{BufRead, BufReader, Cursor, Read, Write};

/// Serializa un comando en formato RESP y lo envía por el stream al servidor.
///
/// El protocolo RESP representa comandos como un array de bulk strings.
/// Por ejemplo, para el comando `"SET foo bar"`, se enviará:
///
/// ```text
/// *3\r\n
/// $3\r\n
/// SET\r\n
/// $3\r\n
/// foo\r\n
/// $3\r\n
/// bar\r\n
/// ```
///
/// # Parámetros
/// - `comando`: el comando en texto plano ingresado por el cliente.
/// - `stream`: el stream donde se escribe el comando serializado.
///
/// # Retorna
/// - `Ok(())` si el comando fue enviado correctamente.
/// - `Err(DatoRedis::SimpleError)` si hubo un error en la construcción del comando o al escribir en el stream.
pub fn resp_client_command_write(comando: String, stream: &mut dyn Write) -> Result<(), DatoRedis> {
    let entrada_split: Vec<String> = parsear_comando(comando);

    if entrada_split.is_empty() {
        return Err(DatoRedis::new_simple_error(
            "ERR".to_string(),
            "Comando vacío o inválido".to_string(),
        ));
    }

    resp_api_command_write(entrada_split, stream)
}

/// Falta documentar
pub fn resp_api_command_write(
    comando: Vec<String>,
    stream: &mut dyn Write,
) -> Result<(), DatoRedis> {
    let mut comando_resp = format!("*{}\r\n", comando.len());
    for token in comando {
        comando_resp.push_str(&format!("${}\r\n{}\r\n", token.len(), token));
    }

    let encrypted: Vec<u8> = encrypt_resp(&comando_resp)?;

    if stream.write_all(&encrypted).is_err() {
        return Err(DatoRedis::new_simple_error(
            "ERR".to_string(),
            "Error al escribir en el stream".to_string(),
        ));
    };

    Ok(())
}

/// Parsea un comando en una serie de tokens separados por espacio,
/// respetando agrupaciones entre comillas dobles.
///
/// Esta función divide un `String` que representa un comando en múltiples
/// partes (tokens). Si una parte del comando está entre comillas dobles (`"`),
/// se tratará como un solo token, incluso si contiene espacios. Las comillas
/// se conservan en el resultado, tal como aparecen en el texto original.
///
/// # Parámetros
/// * `comandos` - Un `String` que contiene el comando a parsear.
///
/// # Retorna
/// - Un `Vec<String>` que contiene cada componente del comando, en orden.
///
/// # Comportamiento especial
/// - Si un token comienza con `"` pero no termina con `"`, se considera el
///   inicio de una secuencia entre comillas.
/// - Se sigue agregando texto a ese token hasta encontrar otro con `"` al final.
/// - Las comillas no se eliminan ni escapan; se preservan tal como están.
/// - Si hay un token entre comillas no cerradas, se incluye de todas formas.
///
/// # Ejemplos
/// ```
/// use redis_client::protocol::protocol_resp::parsear_comando;
/// let entrada = String::from(r#"SET "mi clave" "mi valor con espacios""#);
/// let tokens = parsear_comando(entrada);
/// assert_eq!(tokens, vec![
///     "SET",
///     "mi clave",
///     "mi valor con espacios",
/// ]);
/// ```
pub fn parsear_comando(line: String) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quote: Option<char> = None;
    let mut escape = false;

    for ch in line.chars() {
        if escape {
            match ch {
                'n' => current.push('\n'),
                't' => current.push('\t'),
                'r' => current.push('\r'),
                '\\' => current.push('\\'),
                '\'' => current.push('\''),
                '"' => current.push('"'),
                _ => current.push(ch),
            }
            escape = false;
            continue;
        }

        if ch == '\\' {
            escape = true;
            continue;
        }

        if let Some(quote_char) = in_quote {
            if ch == quote_char {
                in_quote = None;
            } else {
                current.push(ch);
            }
        } else if ch == '\'' || ch == '"' {
            in_quote = Some(ch);
        } else if ch.is_whitespace() {
            if !current.is_empty() {
                tokens.push(current.clone());
                current.clear();
            }
        } else {
            current.push(ch);
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

/// Lee desde el stream un comando RESP enviado por el cliente y lo parsea como un vector de strings.
///
/// Espera que el formato recibido siga la especificación RESP con un array de bulk strings.
///
/// # Ejemplo de entrada esperada:
/// ```text
/// *3\r\n
/// $3\r\n
/// SET\r\n
/// $3\r\n
/// foo\r\n
/// $3\r\n
/// bar\r\n
/// ```
///
/// # Parámetros
/// - `stream`: stream de lectura desde donde se recibe el comando.
///
/// # Retorna
/// - `Ok(Vec<String>)`: cada palabra del comando como un string.
/// - `Err(DatoRedis::SimpleError)`: si el formato es inválido o hay errores de lectura.
pub fn resp_server_command_read(stream: &mut dyn Read) -> Result<Vec<String>, DatoRedis> {
    let desencrypted_resp = decrypt_resp(stream)?; // aca ya tengo el resp
    let mut new_stream = Cursor::new(desencrypted_resp.into_bytes());

    // aca tengo que ver como usar el string...
    let header = read_line_custom(&mut new_stream)?;
    let cantidad = parse_array_len(&header)?;
    let mut resultado = Vec::with_capacity(cantidad);

    for _ in 0..cantidad {
        let bulk_header = read_line_custom(&mut new_stream)?;
        let bulk_len = parse_bulk_len(&bulk_header)?;
        resultado.push(read_exact_bulk(&mut new_stream, bulk_len)?);
    }
    Ok(resultado)
}

/// Extrae la cantidad de elementos de un encabezado de array RESP (`*<n>\r\n`).
fn parse_array_len(header: &str) -> Result<usize, DatoRedis> {
    if !header.starts_with(ARRAY_SIMBOL) {
        return Err(DatoRedis::new_simple_error(
            "PROTOCOLO".into(),
            "Se esperaba un array RESP.".into(),
        ));
    }
    header[1..].trim().parse().map_err(|_| {
        DatoRedis::new_simple_error("PROTOCOLO".into(), "Cantidad de elementos inválida.".into())
    })
}

/// Extrae la longitud declarada de un bulk string (`$<len>\r\n`).
fn parse_bulk_len(header: &str) -> Result<usize, DatoRedis> {
    if !header.starts_with(BULK_STRING_SIMBOL) {
        return Err(DatoRedis::new_simple_error(
            "PROTOCOLO".into(),
            "Se esperaba un bulk string.".into(),
        ));
    }
    header[1..].trim().parse().map_err(|_| {
        DatoRedis::new_simple_error("PROTOCOLO".into(), "Largo del bulk string inválido.".into())
    })
}

/// Lee `len` bytes + `\r\n` del stream y los devuelve como `String`.
fn read_exact_bulk(stream: &mut dyn Read, len: usize) -> Result<String, DatoRedis> {
    let mut contenido = vec![0u8; len + 2]; // +2 por \r\n
    stream.read_exact(&mut contenido).map_err(|_| {
        DatoRedis::new_simple_error(
            "READ".into(),
            "No se pudo leer el contenido del bulk string.".into(),
        )
    })?;
    Ok(String::from_utf8_lossy(&contenido[..len]).to_string())
}

/// Lee del stream hasta encontrar una secuencia `\r\n` y devuelve la línea
/// **incluyendo** esos dos bytes finales.
///
/// *Bloqueante* – la llamada se detiene hasta recibir la secuencia completa.
///
/// # Errores
/// * `"READ"` fallo de E/S al intentar leer del stream.
/// * `"PROTOCOLO"` bytes recibidos no constituyen UTF‑8 válido.
fn read_line_custom(stream: &mut dyn Read) -> Result<String, DatoRedis> {
    let mut buf = [0u8; 1];
    let mut line = Vec::new();

    while stream.read(&mut buf).map_err(|_| {
        DatoRedis::new_simple_error("READ".into(), "Error leyendo del stream.".into())
    })? == 1
    {
        line.push(buf[0]);
        if line.ends_with(b"\r\n") {
            break;
        }
    }

    String::from_utf8(line).map_err(|_| {
        DatoRedis::new_simple_error("PROTOCOLO".into(), "Error decodificando UTF‑8.".into())
    })
}

/// Envía una respuesta RESP desde el servidor al cliente.
///
/// Esta función serializa una respuesta (por ejemplo, resultado de un comando como `GET`)
/// en formato RESP y la escribe en el stream de salida proporcionado.
///
/// # Parámetros
/// - `retorno`: la respuesta ya serializada en formato RESP (como string).
/// - `stream`: stream de salida donde se escribe la respuesta.
/// # Retorna
/// - `Ok(())`: si la respuesta fue enviada correctamente.
/// - `Err(DatoRedis::SimpleError)`: si hubo un error de escritura o flush.
pub fn resp_server_command_write(retorno: &str, stream: &mut dyn Write) -> Result<(), DatoRedis> {
    let encrypted = encrypt_resp(retorno)?;

    stream.write_all(&encrypted).map_err(|_| {
        DatoRedis::new_simple_error(
            "WRITE".to_string(),
            "No se pudo escribir la respuesta al cliente.".to_string(),
        )
    })?;
    Ok(())
}

/// Lee la respuesta del servidor desde el stream (lado cliente).
///
/// Esta función interpreta una respuesta RESP recibida desde el servidor,
/// determinando el tipo de dato según el primer byte del mensaje y luego
/// delegando la lectura y conversión correspondiente.
///
/// Soporta los siguientes tipos RESP:
/// - Simple Strings (`+`)
/// - Bulk Strings (`$`)
/// - Integers (`:`)
/// - Arrays (`*`)
/// - Sets (`~`)
/// - Nulls (`_`)
/// - Simple Errors (`-`)
/// - Verbatim Strings (`=`)
///
/// # Parámetros
/// - `stream`: stream desde el cual se lee la respuesta RESP del servidor.
///
/// # Retorna
/// - `Ok(String)`: representación textual del dato RESP recibido.
/// - `Err(DatoRedis)`: si ocurre un error de lectura, decodificación o el tipo RESP es desconocido.
///
/// # Errores posibles
/// - El stream no contiene suficientes bytes.
/// - El primer byte no representa un tipo RESP válido.
/// - Error de codificación UTF-8 en los datos recibidos.
/// - Formato RESP inválido o no soportado.
/// # Ejemplo de uso
/// ```rust
/// use redis_client::protocol::protocol_resp::resp_client_command_read;
/// use redis_client::tipos_datos::traits::DatoRedis;
/// use redis_client::protocol::protocol_resp::resp_client_command_write;
/// use redis_client::tipos_datos::traits::TipoDatoRedis;
/// use std::io::BufReader;
/// use std::io::Cursor;
///
/// fn main() -> Result<(), DatoRedis> {
///      let mut buffer = Vec::new();
///      let respuesta = resp_client_command_write("SET mykey hello".to_string(), &mut buffer)?;
///      let mut cursor = Cursor::new(buffer);
///      let respuesta = resp_client_command_read(&mut cursor)?;
///      println!("Respuesta: {:?}", respuesta);
///      Ok(())
/// }
/// ```
pub fn resp_client_command_read(stream: &mut dyn Read) -> Result<DatoRedis, DatoRedis> {
    let desencrypted_resp = decrypt_resp(stream)?; // aca ya tengo el resp
    let new_stream = Cursor::new(desencrypted_resp.into_bytes());

    let mut reader = BufReader::new(new_stream);

    let mut tipo_dato_buf = vec![0u8; 1];

    reader.read_exact(&mut tipo_dato_buf).map_err(|_e| {
        DatoRedis::new_simple_error(
            "ERR Protocol".to_string(),
            "stream did not contain enough bytes".to_string(),
        )
    })?;

    let tipo_dato = String::from_utf8(tipo_dato_buf).map_err(|_e| {
        DatoRedis::new_simple_error("ERR".to_string(), "invalid utf-8 sequence".to_string())
    })?;

    match tipo_dato.as_str() {
        SIMPLE_STRING_SIMBOL => {
            read_and_convert_to_dato_redis(&mut reader, interpretar_simple_string)
        }
        BULK_STRING_SIMBOL => read_and_convert_to_dato_redis(&mut reader, interpretar_bulk_string),
        INTEGER_SIMBOL => read_and_convert_to_dato_redis(&mut reader, interpretar_integer),
        ARRAY_SIMBOL => read_and_convert_to_dato_redis(&mut reader, interpretar_arrays),
        SETS_SIMBOL => read_and_convert_to_dato_redis(&mut reader, interpretar_sets),
        NULL_SIMBOL => read_and_convert_to_dato_redis(&mut reader, interpretar_nulls),
        ERROR_SIMBOL => read_and_convert_to_dato_redis(&mut reader, interpretar_simple_error),
        MAP_SYMBOL => read_and_convert_to_dato_redis(&mut reader, interpretar_map_reply),
        _ => Err(DatoRedis::new_simple_error(
            "ERR Protocol".to_string(),
            "unknown data type".to_string(),
        )),
    }
}

/// Lee un dato a interpretar como simple string del reader recibido
/// y lo devuelve String listo para convertir a dato redis
///
/// # Parámetros
/// * `reader`: de donde se lee el string a interpretar
///
/// # Retorna
/// - Un String en caso de exito, error simple de redis en otro caso
fn interpretar_simple_string<R: BufRead>(reader: &mut R) -> Result<String, DatoRedis> {
    let mut simple_string = String::new();
    reader.read_line(&mut simple_string).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR protocol".to_string(),
            "reading simple string".to_string(),
        )
    })?;

    Ok(SIMPLE_STRING_SIMBOL.to_string() + &simple_string)
}

/// Lee un dato del reader recibido a interpretar como bulk string y lo
/// devuelve String listo para convertir a dato redis
///
/// # Parámetros
/// * `reader`: de donde se lee el string a interpretar
///
/// # Retorna
/// - Un String en caso de exito, error simple de redis en otro caso
fn interpretar_bulk_string<R: BufRead>(reader: &mut R) -> Result<String, DatoRedis> {
    let mut largo = String::new();
    reader.read_line(&mut largo).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR protocol".to_string(),
            "invalid bulk length".to_string(),
        )
    })?;
    let long_int = largo.trim().parse::<usize>().map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR protocol".to_string(),
            "invalid bulk length".to_string(),
        )
    })?;

    let mut contenido = vec![0u8; long_int + 2]; // solo contenido + "\r\n"
    reader.read_exact(&mut contenido).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR protocol".to_string(),
            "reading bulk string".to_string(),
        )
    })?;
    let contenido = String::from_utf8(contenido).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR protocol".to_string(),
            "parsing bulk string".to_string(),
        )
    })?;

    Ok(BULK_STRING_SIMBOL.to_string() + &largo + &contenido)
}

/// Lee un dato a interpretar como integer del reader recibido y lo
/// devuelve String listo para convertir a dato redis
///
/// # Parámetros
/// * `reader`: de donde se lee el string a interpretar
///
/// # Retorna
/// - Un String en caso de exito, error simple de redis en otro caso
fn interpretar_integer<R: BufRead>(reader: &mut R) -> Result<String, DatoRedis> {
    let mut integer = String::new();
    reader.read_line(&mut integer).map_err(|_| {
        DatoRedis::new_simple_error("ERR protocol".to_string(), "reading integer".to_string())
    })?;

    Ok(INTEGER_SIMBOL.to_string() + &integer)
}

/// Lee un dato a interpretar como array del reader recibido y lo
/// devuelve String listo para convertir a dato redis
///
/// # Parámetros
/// * `reader`: de donde se lee el string a interpretar
///
/// # Retorna
/// - Un String en caso de exito, error simple de redis en otro caso
fn interpretar_arrays<R: BufRead>(reader: &mut R) -> Result<String, DatoRedis> {
    let mut cantidad_elementos = String::new();
    reader.read_line(&mut cantidad_elementos).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR protocol".to_string(),
            "reading array length".to_string(),
        )
    })?;

    let mut resp = ARRAY_SIMBOL.to_string() + &cantidad_elementos;

    let cantidad_elementos = cantidad_elementos.trim().parse::<usize>().map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR protocol".to_string(),
            "parsing array length".to_string(),
        )
    })?;
    for _ in 0..cantidad_elementos {
        let mut tipo_buf = vec![0u8; 1];
        reader.read_exact(&mut tipo_buf).map_err(|_| {
            DatoRedis::new_simple_error(
                "ERR protocol".to_string(),
                "reading array content".to_string(),
            )
        })?;
        let tipo_dato = String::from_utf8(tipo_buf).map_err(|_| {
            DatoRedis::new_simple_error("ERR protocol".to_string(), "parsing array".to_string())
        })?;
        let elemento_resp = match tipo_dato.as_str() {
            SIMPLE_STRING_SIMBOL => interpretar_simple_string(reader)?,
            BULK_STRING_SIMBOL => interpretar_bulk_string(reader)?,
            INTEGER_SIMBOL => interpretar_integer(reader)?,
            ARRAY_SIMBOL => interpretar_arrays(reader)?,
            ERROR_SIMBOL => interpretar_simple_error(reader)?,
            _ => {
                return Err(DatoRedis::new_simple_error(
                    "ERR protocol".to_string(),
                    "unknown data type".to_string(),
                ));
            }
        };

        resp.push_str(&elemento_resp);
    }

    Ok(resp)
}

/// Lee un dato a interpretar como set del reader recibido y lo
/// devuelve String listo para convertir a dato redis
///
/// # Parámetros
/// * `reader`: de donde se lee el string a interpretar
///
/// # Retorna
/// - Un String en caso de exito, error simple de redis en otro caso
fn interpretar_sets<R: BufRead>(reader: &mut R) -> Result<String, DatoRedis> {
    let mut cantidad_elementos = String::new();
    reader.read_line(&mut cantidad_elementos).map_err(|_| {
        DatoRedis::new_simple_error("ERR protocol".to_string(), "reading set length".to_string())
    })?;
    let mut resp = SETS_SIMBOL.to_string() + &cantidad_elementos;

    let cantidad_elementos = cantidad_elementos.trim().parse::<usize>().map_err(|_| {
        DatoRedis::new_simple_error("ERR protocol".to_string(), "parsing set length".to_string())
    })?;
    for _ in 0..cantidad_elementos {
        let mut tipo_buf = vec![0u8; 1];
        reader.read_exact(&mut tipo_buf).map_err(|_| {
            DatoRedis::new_simple_error("ERR protocol".to_string(), "reading set data".to_string())
        })?;
        let tipo_dato = String::from_utf8(tipo_buf).map_err(|_| {
            DatoRedis::new_simple_error("ERR protocol".to_string(), "parsing set data".to_string())
        })?;

        let elemento_resp = match tipo_dato.as_str() {
            SIMPLE_STRING_SIMBOL => interpretar_simple_string(reader)?,
            BULK_STRING_SIMBOL => interpretar_bulk_string(reader)?,
            INTEGER_SIMBOL => interpretar_integer(reader)?,
            _ => {
                return Err(DatoRedis::new_simple_error(
                    "ERR protocol".to_string(),
                    "unknown data type".to_string(),
                ));
            }
        };

        resp.push_str(&elemento_resp);
    }

    Ok(resp)
}

/// Lee un dato a interpretar como null del reader recibido y lo
/// devuelve String listo para convertir a dato redis
///
/// # Parámetros
/// * `reader`: de donde se lee el string a interpretar
///
/// # Retorna
/// - Un String en caso de exito, error simple de redis en otro caso
fn interpretar_nulls<R: BufRead>(reader: &mut R) -> Result<String, DatoRedis> {
    let mut string_null = String::new();
    reader.read_line(&mut string_null).map_err(|_| {
        DatoRedis::new_simple_error("ERR protocol".to_string(), "reading null".to_string())
    })?;

    Ok(NULL_SIMBOL.to_string() + &string_null)
}

/// Lee un dato a interpretar como simple error del reader recibido y lo
/// devuelve String listo para convertir a dato redis
///
/// # Parámetros
/// * `reader`: de donde se lee el string a interpretar
///
/// # Retorna
/// - Un String en caso de exito, error simple de redis en otro caso
fn interpretar_simple_error<R: BufRead>(reader: &mut R) -> Result<String, DatoRedis> {
    let mut simple_error = String::new();
    reader.read_line(&mut simple_error).map_err(|_| {
        DatoRedis::new_simple_error(
            "ERR protocol".to_string(),
            "reading simple error".to_string(),
        )
    })?;
    Ok(ERROR_SIMBOL.to_string() + &simple_error)
}

/// Lee un dato a interpretar como map del reader recibido y lo
/// devuelve String listo para convertir a dato redis
///
/// # Parámetros
/// * `reader`: de donde se lee el string a interpretar
///
/// # Retorna
/// - Un String en caso de exito, error simple de redis en otro caso
fn interpretar_map_reply<R: BufRead>(reader: &mut R) -> Result<String, DatoRedis> {
    let mut cantidad_elementos = String::new();
    reader.read_line(&mut cantidad_elementos).map_err(|_| {
        DatoRedis::new_simple_error("ERR protocol".to_string(), "reading map length".to_string())
    })?;
    let mut resp = MAP_SYMBOL.to_string() + &cantidad_elementos;

    let cantidad_elementos = cantidad_elementos.trim().parse::<usize>().map_err(|_| {
        DatoRedis::new_simple_error("ERR protocol".to_string(), "parsing map length".to_string())
    })?;
    for _ in 0..cantidad_elementos {
        let key = get_element_as_resp_str(reader)?;
        let value = get_element_as_resp_str(reader)?;
        resp.push_str(&key);
        resp.push_str(&value);
    }
    Ok(resp)
}

/// Lee un dato del reader y lo devuelve como string a convertir a dato redis
///
/// # Parámetros
/// * `reader`: de donde se lee el string a interpretar
///
/// # Retorna
/// - Un String en caso de exito, error simple de redis en otro caso
fn get_element_as_resp_str<R: BufRead>(reader: &mut R) -> Result<String, DatoRedis> {
    let mut tipo_buf = vec![0u8; 1];
    reader.read_exact(&mut tipo_buf).map_err(|_| {
        DatoRedis::new_simple_error("ERR protocol".to_string(), "reading map data".to_string())
    })?;
    let tipo_dato = String::from_utf8(tipo_buf).map_err(|_| {
        DatoRedis::new_simple_error("ERR protocol".to_string(), "parsing map data".to_string())
    })?;
    let elemento_resp = match tipo_dato.as_str() {
        SIMPLE_STRING_SIMBOL => interpretar_simple_string(reader)?,
        BULK_STRING_SIMBOL => interpretar_bulk_string(reader)?,
        INTEGER_SIMBOL => interpretar_integer(reader)?,
        ARRAY_SIMBOL => interpretar_arrays(reader)?,
        SETS_SIMBOL => interpretar_sets(reader)?,
        NULL_SIMBOL => interpretar_nulls(reader)?,
        ERROR_SIMBOL => interpretar_simple_error(reader)?,
        MAP_SYMBOL => interpretar_map_reply(reader)?,
        _ => {
            return Err(DatoRedis::new_simple_error(
                "ERR protocol".to_string(),
                "unknown data type".to_string(),
            ));
        }
    };
    Ok(elemento_resp)
}

/// Lee un dato a interpretar del reader recibido y lo
/// devuelve como String
///
/// # Parámetros
/// * `reader`: de donde se lee el string a interpretar
/// * `interpretar`: funcion que recibe un reader y lo transforma en
///   un String que puede parsearse a dato redis
/// * `construir`: funcion que recibe un String y lo convierte a un tipo
///   de dato redis especifico
///
/// # Retorna
/// - Un String en caso de exito, error simple de redis en otro caso
fn read_and_convert_to_dato_redis<T: Read>(
    reader: &mut T,
    interpretar: fn(&mut T) -> Result<String, DatoRedis>,
) -> Result<DatoRedis, DatoRedis> {
    let contenido = interpretar(reader)?;
    DatoRedis::from_bytes(contenido.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tipos_datos::traits::TipoDatoRedis;
    use std::io::Cursor;

    #[test]
    fn test_resp_client_command_parsed_set() {
        let comando = "SET foo bar".to_string();
        let mut buffer = Vec::new();

        let result = resp_client_command_write(comando, &mut buffer);

        assert!(result.is_ok());

        let mut cursor = Cursor::new(&buffer);
        let desencrypted = decrypt_resp(&mut cursor).unwrap();

        let expected = "*3\r\n\
                        $3\r\nSET\r\n\
                        $3\r\nfoo\r\n\
                        $3\r\nbar\r\n";

        assert_eq!(desencrypted, expected);
    }

    #[test]
    fn test_resp_client_command_parsed_single_word() {
        let comando = "PING".to_string();
        let mut buffer = Vec::new();

        let result = resp_client_command_write(comando, &mut buffer);
        assert!(result.is_ok());

        let expected = "*1\r\n\
                        $4\r\nPING\r\n";
        let mut cursor = Cursor::new(&buffer);
        let desencrypted = decrypt_resp(&mut cursor).unwrap();

        assert_eq!(desencrypted, expected);
    }

    #[test]
    fn test_resp_client_command_parsed_empty() {
        let comando = "   ".to_string();
        let mut buffer = Vec::new();

        let result = resp_client_command_write(comando, &mut buffer);
        assert!(result.is_err());
    }

    #[test]
    fn test_resp_client_command_parsed_with_extra_spaces() {
        let comando = "  SET   key   value  ".to_string();
        let mut buffer = Vec::new();

        let result = resp_client_command_write(comando, &mut buffer);
        assert!(result.is_ok());

        let expected = "*3\r\n\
                    $3\r\nSET\r\n\
                    $3\r\nkey\r\n\
                    $5\r\nvalue\r\n";
        let mut cursor = Cursor::new(&buffer);
        let desencrypted = decrypt_resp(&mut cursor).unwrap();

        assert_eq!(desencrypted, expected);
    }

    #[test]
    fn test_resp_client_command_parsed_with_quotes() {
        let comando = "SET \"hola mundo\" \"valor\"".to_string();
        let mut buffer = Vec::new();

        let result = resp_client_command_write(comando, &mut buffer);
        assert!(result.is_ok());

        let expected = "*3\r\n\
                    $3\r\nSET\r\n\
                    $10\r\nhola mundo\r\n\
                    $5\r\nvalor\r\n";
        let mut cursor = Cursor::new(&buffer);
        let desencrypted = decrypt_resp(&mut cursor).unwrap();

        assert_eq!(desencrypted, expected);
    }

    #[test]
    fn test_resp_server_command_read_set1() {
        let input = "*3\r\n$3\r\nSET\r\n$5\r\nclave\r\n$5\r\nvalor\r\n";
        let encrypted = encrypt_resp(input).unwrap();
        let mut cursor = Cursor::new(encrypted);

        let result = resp_server_command_read(&mut cursor);

        assert_eq!(
            result,
            Ok(vec![
                "SET".to_string(),
                "clave".to_string(),
                "valor".to_string()
            ])
        );
    }

    #[test]
    fn test_resp_server_command_read_set2() {
        let input = "*3\r\n$3\r\nSET\r\n$11\r\nclave clave\r\n$17\r\nvalor valor valor\r\n";
        let encrypted = encrypt_resp(input).unwrap();
        let mut cursor = Cursor::new(encrypted);

        let result = resp_server_command_read(&mut cursor);

        assert_eq!(
            result,
            Ok(vec![
                "SET".to_string(),
                "clave clave".to_string(),
                "valor valor valor".to_string()
            ])
        );
    }

    #[test]
    fn test_resp_server_command_read_ping() {
        let input = "*1\r\n$4\r\nPING\r\n";
        let encrypted = encrypt_resp(input).unwrap();
        let mut cursor = Cursor::new(encrypted);

        let result = resp_server_command_read(&mut cursor);
        assert_eq!(result, Ok(vec!["PING".to_string()]));
    }

    #[test]
    fn test_resp_server_command_read_invalid_prefix() {
        let input = "$3\r\nSET\r\n$5\r\nclave\r\n";
        let encrypted = encrypt_resp(input).unwrap();
        let mut cursor = Cursor::new(encrypted);
        let result = resp_server_command_read(&mut cursor);

        assert!(result.is_err());
    }

    #[test]
    fn test_resp_server_command_read_wrong_length() {
        let input = "*2\r\n$3\r\nSET\r\n$10\r\nclave\r\n"; // dice que clave mide 10, pero mide 5
        let encrypted = encrypt_resp(input).unwrap();
        let mut cursor = Cursor::new(encrypted);

        let result = resp_server_command_read(&mut cursor);
        assert!(result.is_err());
    }

    #[test]
    fn test_resp_client_read_simple_string() {
        let input = "+OK\r\n";
        let encrypted = encrypt_resp(input).unwrap();
        let mut cursor = Cursor::new(encrypted);

        let result = resp_client_command_read(&mut cursor).unwrap();
        assert_eq!("OK\r\n", result.convertir_resp_a_string());
    }

    #[test]
    fn test_resp_client_read_unkown_data_type() {
        let input = "a\r\n";
        let encrypted = encrypt_resp(input).unwrap();
        let mut cursor = Cursor::new(encrypted);
        let result = resp_client_command_read(&mut cursor).err().unwrap();
        assert_eq!(
            "(error) ERR PROTOCOL unknown data type\r\n",
            result.convertir_resp_a_string()
        );
    }

    #[test]
    fn test_resp_client_read_bulk_string() {
        let input = "$5\r\nhello\r\n";
        let encrypted = encrypt_resp(input).unwrap();
        let mut cursor = Cursor::new(encrypted);

        let result = resp_client_command_read(&mut cursor).unwrap();
        assert_eq!("\"hello\"\r\n", result.convertir_resp_a_string());
    }

    #[test]
    fn test_resp_client_read_integer() {
        let input = ":33\r\n";
        let encrypted = encrypt_resp(input).unwrap();
        let mut cursor = Cursor::new(encrypted);

        let result = resp_client_command_read(&mut cursor).unwrap();
        assert_eq!("(integer) 33\r\n", result.convertir_resp_a_string());
    }

    #[test]
    fn test_resp_client_read_integer2() {
        let input = ":-33\r\n";
        let encrypted = encrypt_resp(input).unwrap();
        let mut cursor = Cursor::new(encrypted);

        let result = resp_client_command_read(&mut cursor).unwrap();
        assert_eq!("(integer) -33\r\n", result.convertir_resp_a_string());
    }

    #[test]
    fn test_resp_client_read_array1() {
        //let input = b"*2\r\n*0\r\n*1\r\n$5\r\nhello\r\n";
        let input = "*1\r\n$4\r\nPING\r\n";
        let encrypted = encrypt_resp(input).unwrap();
        let mut cursor = Cursor::new(encrypted);

        let result = resp_client_command_read(&mut cursor);
        assert_eq!("1) \"PING\"\r\n", result.unwrap().convertir_resp_a_string());
    }

    #[test]
    fn test_resp_client_read_array2() {
        //let input = b"*2\r\n*0\r\n*1\r\n$5\r\nhello\r\n";
        let input = "*2\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        let encrypted = encrypt_resp(input).unwrap();
        let mut cursor = Cursor::new(encrypted);

        let result = resp_client_command_read(&mut cursor).unwrap();
        assert_eq!(
            "1) \"PING\"\r\n2) \t1) \"PING\"\r\n",
            result.convertir_resp_a_string()
        );
    }

    #[test]
    fn test_resp_client_read_array_unkown_data_type() {
        let input = "*1\r\na\r\n";
        let encrypted = encrypt_resp(input).unwrap();
        let mut cursor = Cursor::new(encrypted);

        let result = resp_client_command_read(&mut cursor);
        assert_eq!(
            "(error) ERR PROTOCOL unknown data type\r\n",
            result.err().unwrap().convertir_resp_a_string()
        );
    }

    #[test]
    fn test_resp_client_read_set() {
        let input = "~2\r\n$4\r\nPING\r\n$9\r\nPING PONG\r\n";
        let encrypted = encrypt_resp(input).unwrap();
        let mut cursor = Cursor::new(encrypted);

        let result = resp_client_command_read(&mut cursor)
            .unwrap()
            .convertir_resp_a_string();
        assert!(result.contains("PING"));
        assert!(result.contains("PING PONG"));
    }

    #[test]
    fn test_resp_client_read_set_with_unkown_data_type() {
        let input = "~1\r\nb";
        let encrypted = encrypt_resp(input).unwrap();
        let mut cursor = Cursor::new(encrypted);

        let result = resp_client_command_read(&mut cursor)
            .err()
            .unwrap()
            .convertir_resp_a_string();
        assert_eq!(result, "(error) ERR PROTOCOL unknown data type\r\n");
    }

    #[test]
    fn test_resp_client_read_set_integers() {
        // Set de 3 enteros: 0, 42 y -7
        let input = "~3\r\n:0\r\n:42\r\n:-7\r\n";
        let encrypted = encrypt_resp(input).unwrap();
        let mut cursor = Cursor::new(encrypted);

        let result = resp_client_command_read(&mut cursor)
            .unwrap()
            .convertir_resp_a_string();

        // Sólo verificamos que los valores estén presentes (el orden no importa en un set)
        assert!(result.contains("0"));
        assert!(result.contains("42"));
        assert!(result.contains("-7"));
    }

    #[test]
    fn test_basico() {
        let entrada = "hola mundo".to_string();
        let esperado = vec!["hola", "mundo"];
        assert_eq!(parsear_comando(entrada), esperado);
    }

    #[test]
    fn test_espacios_multiples() {
        let entrada = "   hola      mundo   ".to_string();
        let esperado = vec!["hola", "mundo"];
        assert_eq!(parsear_comando(entrada), esperado);
    }

    #[test]
    fn test_con_nuevas_lineas() {
        let entrada = r#"   hola   \nmundo  "#.to_string();
        let esperado = vec!["hola", "\nmundo"];
        assert_eq!(parsear_comando(entrada), esperado);
    }

    #[test]
    fn test_vacio() {
        let entrada = "".to_string();
        let esperado: Vec<String> = vec![];
        assert_eq!(parsear_comando(entrada), esperado);
    }

    #[test]
    fn test_solo_espacios() {
        let entrada = "        ".to_string();
        let esperado: Vec<String> = vec![];
        assert_eq!(parsear_comando(entrada), esperado);
    }

    #[test]
    fn test_caracteres_especiales() {
        let entrada = r#"uno dos\t tres\ncuatro"#.to_string();
        let esperado = vec!["uno", "dos\t", "tres\ncuatro"];
        assert_eq!(parsear_comando(entrada), esperado);
    }

    #[test]
    fn test_parsear_comando() {
        let casos = vec![
            ("ping", vec!["ping"]),
            ("set clave valor", vec!["set", "clave", "valor"]),
            (
                r#"publish canal "mensaje con espacios""#,
                vec!["publish", "canal", "mensaje con espacios"],
            ),
            (
                r#"publish canal "mensaje \"interno\" con comillas""#,
                vec!["publish", "canal", "mensaje \"interno\" con comillas"],
            ),
            (
                r#"publish canal "entre comillas \"dentro\" de las comillas""#,
                vec![
                    "publish",
                    "canal",
                    "entre comillas \"dentro\" de las comillas",
                ],
            ),
            (
                r#""comando entero entre comillas""#,
                vec!["comando entero entre comillas"],
            ),
            (
                r#"set "clave con espacios" "valor con espacios""#,
                vec!["set", "clave con espacios", "valor con espacios"],
            ),
            ("echo \"\"", vec!["echo"]),
            (r#"\"inicio sin cierre"#, vec!["\"inicio", "sin", "cierre"]),
            (
                "espacios    múltiples   entre  tokens",
                vec!["espacios", "múltiples", "entre", "tokens"],
            ),
            (
                r#"PUBLISH llm:request "{\"prompt\":\"Hola \"hola mundo\"\",\"response_channel\":\"HOLA\"}""#,
                vec![
                    "PUBLISH",
                    "llm:request",
                    "{\"prompt\":\"Hola \"hola mundo\"\",\"response_channel\":\"HOLA\"}",
                ],
            ),
        ];

        for (entrada, esperado) in casos {
            let esperado: Vec<String> = esperado.iter().map(|s| s.to_string()).collect();
            assert_eq!(parsear_comando(entrada.to_string()), esperado);
        }
    }

    #[test]
    fn parse_largo() {}
}
