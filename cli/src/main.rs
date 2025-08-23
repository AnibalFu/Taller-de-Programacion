use redis_client::protocol::protocol_resp::{resp_client_command_read, resp_client_command_write};
use redis_client::tipos_datos::traits::TipoDatoRedis;
use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::{env, io};
/*
fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Uso: cargo run -- <ip:puerto>");
        return;
    }

    let addr: SocketAddr = match args[1].parse() {
        Ok(a) => a,
        Err(_) => {
            eprintln!("Dirección inválida. Formato esperado: ip:puerto");
            return;
        }
    };

    let stream = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error al conectar con el servidor Redis: {e}");
            return;
        }
    };

    let mut stream_write = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error al clonar el stream: {e}");
            return;
        }
    };
    let mut stream_reader = match stream.try_clone() {
        Ok(r) => BufReader::new(r),
        Err(e) => {
            eprintln!("Error al clonar el stream: {e}");
            return;
        }
    };

    let writer = thread::spawn(move || {
        println!("Ingresar comando RESP ('q' para salir):");
        for line in io::stdin().lock().lines() {
            match line {
                Ok(input) if input.trim() == "q" => break,
                Ok(input) => {
                    if let Err(e) = resp_client_command_write(input, &mut stream_write) {
                        eprintln!("{}", e.convertir_resp_a_string());
                        println!("Vuelva a escribir el comando");
                    }
                }
                Err(e) => eprintln!("Error leyendo entrada del usuario: {e}"),
            }
        }
    });

    let sender = thread::spawn(move || {
        loop {
            match resp_client_command_read(&mut stream_reader) {
                Ok(res) => println!("{}", res.convertir_resp_a_string()),
                Err(er) => {
                    println!("{}", er.convertir_resp_a_string());
                    println!("Error del servidor, vuelva a conectarse más tarde");
                    break;
                }
            }
        }
    });

    // ─── Sincroniza hilos ───────────────────────────────────────────────────────
    if let Err(e) = writer.join() {
        eprintln!("Error en el hilo de entrada: {e:?}");
    }
    if let Err(e) = sender.join() {
        eprintln!("Error en el hilo de envío: {e:?}");
    }
}

 */

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Uso: cargo run -- <ip:puerto>");
        return;
    }

    let addr: SocketAddr = match args[1].parse() {
        Ok(a) => a,
        Err(_) => {
            eprintln!("Dirección inválida. Formato esperado: ip:puerto");
            return;
        }
    };

    let stream = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error al conectar con el servidor Redis: {e}");
            return;
        }
    };

    let mut stream_write = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error al clonar el stream: {e}");
            return;
        }
    };
    let mut stream_reader = match stream.try_clone() {
        Ok(r) => BufReader::new(r),
        Err(e) => {
            eprintln!("Error al clonar el stream: {e}");
            return;
        }
    };

    let writer = thread::spawn(move || {
        println!("Ingresar comando RESP ('q' para salir):");
        // ─── Bucle de entrada del usuario ────────────────────────────────────────
        for line in io::stdin().lock().lines() {
            match line {
                Ok(input) if input.trim() == "q" => break,
                Ok(input) => {
                    if let Err(e) = resp_client_command_write(input, &mut stream_write) {
                        eprintln!("{}", e.convertir_resp_a_string());
                        println!("Vuelva a escribir el comando");
                    }
                }
                Err(e) => eprintln!("Error leyendo entrada del usuario: {e}"),
            }
        }
    });

    let sender = thread::spawn(move || {
        loop {
            match resp_client_command_read(&mut stream_reader) {
                Ok(res) => println!("{}", res.convertir_resp_a_string()),
                Err(er) => {
                    println!("{}", er.convertir_resp_a_string());
                    println!("Error del servidor, vuelva a conectarse más tarde");
                    break;
                }
            }
        }
    });

    // ─── Sincroniza hilos ───────────────────────────────────────────────────────
    if let Err(e) = writer.join() {
        eprintln!("Error en el hilo de entrada: {e:?}");
    }
    if let Err(e) = sender.join() {
        eprintln!("Error en el hilo de envío: {e:?}");
    }
}

/*
fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Uso: cargo run -- <puerto>");
        return;
    }

    let puerto: u16 = match args[1].parse() {
        Ok(p) => p,
        Err(_) => {
            eprintln!("Puerto inválido");
            return;
        }
    };

    let redis_connection = RedisDriver::connect("127.0.0.1", puerto);
    let mut redis_connection = match redis_connection {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!("Error al conectar con el servidor Redis: {:?}", e);
            return;
        }
    };

    let (tx, rx) = mpsc::channel();

    let writer = thread::spawn(move || {
        let stdin = io::stdin();
        let stdin_lock = stdin.lock();

        println!("Ingresar comando RESP ('q' para salir):");

        for line in stdin_lock.lines() {
            match line {
                Ok(input) => {
                    if input.trim() == "q" {
                        break;
                    }
                    if let Err(e) = tx.send(input) {
                        eprintln!("Error enviando comando al hilo de envío: {e:?}");
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Error leyendo entrada del usuario: {e:?}");
                    continue;
                }
            }
        }
    });

    let sender = thread::spawn(move || {
        loop {
            match rx.try_recv() {
                Ok(command) => {
                    if command.trim().is_empty() {
                        continue;
                    }

                    if let Err(e) = redis_connection.command(command) {
                        eprintln!("Error enviando comando al servidor: {:?}", e);
                        println!("Vuelva a escribir el comando");
                        continue;
                    }
                }
                Err(mpsc::TryRecvError::Empty) => {
                    // No hay comandos por ahora, continuar
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    eprintln!("Canal de comandos desconectado.");
                    break;
                }
            }

            match redis_connection.receive_response_as_string() {
                Ok(res) => {
                    println!("{res}");
                }
                Err(er) => match er.kind {
                    RedisDriverErrorKind::EmptyStreamError => continue,
                    _ => {
                        eprintln!("Error leyendo del servidor: {:?}", er);
                        break;
                    }
                },
            }
        }
    });

    if let Err(e) = writer.join() {
        eprintln!("Error en el hilo de entrada: {e:?}");
    }

    if let Err(e) = sender.join() {
        eprintln!("Error en el hilo de envío: {e:?}");
    }
}
{
    "prompt":"<request>",
    "response_channel":"<channel>"
}
 */
