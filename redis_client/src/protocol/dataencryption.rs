use std::io::{Cursor, Read};

use aes::Aes128;
use block_modes::block_padding::Pkcs7;
use block_modes::{BlockMode, Cbc};
use rand::Rng;

use crate::protocol::constants::{AES_KEY, IV_LEN};
use crate::tipos_datos::traits::DatoRedis;

/// Tipo que representa una instancia de AES-128 en modo CBC con padding PKCS7
type Aes128Cbc = Cbc<Aes128, Pkcs7>;

/// Convierte un slice de bytes a un string hexadecimal en minúsculas.
///
/// # Argumentos
/// - `bytes`: Slice de bytes a convertir.
///
/// # Retorna
/// Un `String` que representa el contenido hexadecimal de los bytes.
fn to_hex_string(bytes: &[u8]) -> String {
    const HEX_CHARS: &[u8] = b"0123456789abcdef";
    let mut hex = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        hex.push(HEX_CHARS[(byte >> 4) as usize] as char);
        hex.push(HEX_CHARS[(byte & 0x0F) as usize] as char);
    }
    hex
}

/// Parsea un string hexadecimal a un vector de bytes.
///
/// # Argumentos
/// - `hex`: String con representación hexadecimal.
///
/// # Errores
/// - Si la longitud del string no es par.
/// - Si contiene caracteres que no son hexadecimales válidos.
///
/// # Retorna
/// Un `Vec<u8>` con los bytes decodificados.
fn from_hex_string(hex: &str) -> Result<Vec<u8>, String> {
    let bytes = hex.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err("Longitud impar en string hexadecimal".to_string());
    }

    let mut result = Vec::with_capacity(bytes.len() / 2);

    for i in (0..bytes.len()).step_by(2) {
        let high = hex_char_to_value(bytes[i])?;
        let low = hex_char_to_value(bytes[i + 1])?;
        result.push((high << 4) | low);
    }

    Ok(result)
}

/// Convierte un caracter hexadecimal a su valor numérico.
///
/// # Argumentos
/// - `c`: Byte que representa un caracter hexadecimal.
///
/// # Errores
/// - Si el caracter no está en 0-9, a-f o A-F.
///
/// # Retorna
/// Un `u8` con el valor del caracter.
fn hex_char_to_value(c: u8) -> Result<u8, String> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(format!("Caracter inválido: '{}'", c as char)),
    }
}

/// Encripta un string y lo convierte a hexadecimal.
///
/// Internamente usa AES-128-CBC con un IV aleatorio,
/// y empaqueta la longitud total, el IV y el ciphertext.
///
/// # Argumentos
/// - `input`: String a encriptar.
///
/// # Retorna
/// Un `Ok(String)` con el contenido hexadecimal del mensaje encriptado,
/// o un `Err(String)` si falla la encriptación.
pub fn encrypt_y_encode_hex(input: &str) -> Result<String, String> {
    let encrypted = encrypt_resp(input).map_err(|_| "Error encriptando datos")?;
    Ok(to_hex_string(&encrypted))
}

/// Decodifica un string hexadecimal y lo desencripta.
///
/// # Argumentos
/// - `input`: String hexadecimal previamente generado con [`encrypt_y_encode_hex`].
///
/// # Retorna
/// Un `Ok(String)` con el contenido desencriptado,
/// o un `Err(String)` si falla el parseo o la desencriptación.
pub fn decrypt_from_hex(input: &str) -> Result<String, String> {
    let bytes = from_hex_string(input)?;
    let mut cursor = Cursor::new(bytes);

    decrypt_resp(&mut cursor).map_err(|e| format!("Error desencriptando: {e:?}"))
}

/// Encripta una cadena RESP (como string plano) usando AES-128-CBC.
///
/// El resultado incluye:
/// - 2 bytes con la longitud total (u16 big endian)
/// - 16 bytes de IV aleatorio
/// - ciphertext
///
/// # Argumentos
/// - `resp_text`: Texto en formato RESP a encriptar.
///
/// # Retorna
/// Un vector de bytes encriptados, listo para enviarse o almacenarse.
///
/// # Errores
/// Retorna un `DatoRedis` si ocurre un error con el cifrado.
pub fn encrypt_resp(resp_text: &str) -> Result<Vec<u8>, DatoRedis> {
    let mut iv = [0u8; IV_LEN];
    rand::thread_rng().fill(&mut iv);

    let cipher = Aes128Cbc::new_from_slices(AES_KEY, &iv).map_err(|_| {
        DatoRedis::new_simple_error(
            "SECURITY".to_string(),
            "Error creating cipher for encryption.".to_string(),
        )
    })?;

    let ciphertext = cipher.encrypt_vec(resp_text.as_bytes());

    let total_len = (IV_LEN + ciphertext.len()) as u16;
    let mut result = Vec::with_capacity(2 + IV_LEN + ciphertext.len());
    result.extend_from_slice(&total_len.to_be_bytes());
    result.extend_from_slice(&iv);
    result.extend_from_slice(&ciphertext);

    Ok(result)
}

/// Desencripta una cadena RESP previamente cifrada con `encrypt_resp`.
///
/// Espera leer del stream en este orden:
/// - 2 bytes de longitud total (u16 big endian)
/// - IV de 16 bytes
/// - ciphertext
///
/// # Argumentos
/// - `stream`: Objeto que implementa `Read` desde el cual se leerán los datos encriptados.
///
/// # Retorna
/// Un `Ok(String)` con el contenido desencriptado.
///
/// # Errores
/// Devuelve un `DatoRedis` con mensajes de error en caso de:
/// - Error al leer del stream
/// - Error al construir el cipher
/// - Error al desencriptar o decodificar UTF-8.
pub fn decrypt_resp(stream: &mut dyn Read) -> Result<String, DatoRedis> {
    let mut len_buf = [0u8; 2];
    stream.read_exact(&mut len_buf).map_err(|_| {
        DatoRedis::new_simple_error(
            "SECURITY".to_string(),
            "Error reading encrypted message length.".to_string(),
        )
    })?;

    let total_len = u16::from_be_bytes(len_buf) as usize;

    let mut full_buf = vec![0u8; total_len];
    stream.read_exact(&mut full_buf).map_err(|_| {
        DatoRedis::new_simple_error(
            "SECURITY".to_string(),
            "Error reading encrypted message.".to_string(),
        )
    })?;

    let (iv, ciphertext) = full_buf.split_at(IV_LEN);

    let cipher = Aes128Cbc::new_from_slices(AES_KEY, iv).map_err(|_| {
        DatoRedis::new_simple_error(
            "SECURITY".to_string(),
            "Error decrypting message.".to_string(),
        )
    })?;

    let decrypted_data = cipher.decrypt_vec(ciphertext).map_err(|_| {
        DatoRedis::new_simple_error(
            "SECURITY".to_string(),
            "Error decrypting message.".to_string(),
        )
    })?;

    let decrypted_string = String::from_utf8(decrypted_data).map_err(|_| {
        DatoRedis::new_simple_error(
            "SECURITY".to_string(),
            "Decrypted message is not valid UTF-8.".to_string(),
        )
    })?;

    Ok(decrypted_string)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::protocol::dataencryption::{decrypt_resp, encrypt_resp};

    #[test]
    fn test_01_encrypt_and_decrypt_resp() {
        let resp = "*3\r\n\
                $3\r\nSET\r\n\
                $3\r\nfoo\r\n\
                $3\r\nbar\r\n";
        let encrypted_resp = encrypt_resp(resp).unwrap();
        let mut stream = Cursor::new(encrypted_resp);
        let decrypted_resp = decrypt_resp(&mut stream).unwrap();
        assert_eq!(resp, decrypted_resp);
    }

    #[test]
    fn test_02_encrypt_and_decrypt_long_resp() {
        let resp = "*6\r\n$9\r\nsubscribe\r\n$6\r\ncanal1\r\n:1\r\n$9\r\nsubscribe\r\n$6\r\ncanal2\r\n:2\r\n*3\r\n$11\r\nunsubscribe\r\n$6\r\ncanal1\r\n:1\r\n";
        let encrypted_resp = encrypt_resp(resp).unwrap();
        let mut stream = Cursor::new(encrypted_resp);
        let decrypted_resp = decrypt_resp(&mut stream).unwrap();
        assert_eq!(resp, decrypted_resp);
    }
}
