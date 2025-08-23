use std::io;
use std::io::{Read, Write};
use std::net::TcpStream;

/// Abstracción de “un stream RESP” (normalmente el socket a Redis).
///
/// *  Debe poder **leer** y **escribir** bytes.
/// *  Debe poder **moverse** entre hilos (`Send`).
/// *  Debe poder clonarse lógicamente mediante `try_clone` para que
///    el hilo lector y el hilo escritor utilicen conexiones independientes
///    al mismo socket subyacente.
pub trait RespConn: Read + Write + Send + 'static {
    fn try_clone(&self) -> io::Result<Self>
    where
        Self: Sized;
}

/// Trait implementado para TcpStream
impl RespConn for TcpStream {
    fn try_clone(&self) -> io::Result<Self> {
        TcpStream::try_clone(self)
    }
}
