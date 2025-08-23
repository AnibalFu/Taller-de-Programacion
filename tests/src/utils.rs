use std::io::{BufRead, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc::{self, Receiver, Sender, channel};
use std::thread::JoinHandle;
use std::time::Duration;
use std::{io, thread};

pub fn ejecutar_test_cluster_cliente(comandos_cliente: Vec<String>, port: String) -> Vec<String> {
    let hilos = lanzar_masters();
    thread::sleep(Duration::from_secs(5));
    let (tx_output, rx_output): (Sender<String>, Receiver<String>) = channel();

    let hilo_cliente = thread::spawn(move || {
        let mut cliente = lanzar_cliente(port);
        if let Some(stdout) = cliente.stdout.take() {
            if let Some(stdin) = cliente.stdin.take() {
                escribir_entrada(comandos_cliente, stdin, false);
            } else {
                panic!("Error al escribir en el cliente");
            }
            leer_salida(stdout, tx_output);
        } else {
            panic!("Error al leer del cliente");
        }
        let _ = cliente.wait();
    });

    hilo_cliente.join().expect("El hilo del cliente falló");
    for (i, hilo) in hilos.into_iter().enumerate() {
        hilo.join()
            .expect(&format!("El hilo del nodo {} falló", i + 1)[..]);
    }

    devolver_salida(rx_output)
}

pub fn ejecutar_test_cluster_dos_clientes(
    comandos_cliente1: Vec<String>,
    port1: String,
    comandos_cliente2: Vec<String>,
    port2: String,
    esperar_primero: bool,
) -> Vec<String> {
    let hilos = lanzar_masters();
    thread::sleep(Duration::from_secs(5));
    let (tx_output, rx_output): (Sender<String>, Receiver<String>) = channel();

    let hilo_cliente1 = thread::spawn(move || {
        let mut cliente = lanzar_cliente(port1);
        esperar_otro_cliente(!esperar_primero);
        if let Some(stdin) = cliente.stdin.take() {
            escribir_entrada(comandos_cliente1, stdin, esperar_primero);
        }
        let _ = cliente.wait();
    });

    let hilo_cliente2 = thread::spawn(move || {
        let mut cliente = lanzar_cliente(port2);
        esperar_otro_cliente(esperar_primero);

        if let Some(stdout) = cliente.stdout.take() {
            if let Some(stdin) = cliente.stdin.take() {
                escribir_entrada(comandos_cliente2, stdin, false);
            } else {
                panic!("Error al escribir en el cliente");
            }
            leer_salida(stdout, tx_output);
        } else {
            panic!("Error al leer del cliente");
        }
        let _ = cliente.wait();
    });

    hilo_cliente1.join().expect("El hilo del cliente 1 falló");
    hilo_cliente2.join().expect("El hilo del cliente 2 falló");

    for (i, hilo) in hilos.into_iter().enumerate() {
        hilo.join()
            .expect(&format!("El hilo del nodo {} falló", i + 1)[..]);
    }

    devolver_salida(rx_output)
}

pub fn ejecutar_test_nodo_cliente(
    comandos_cliente: Vec<String>,
    config: String,
    port: String,
) -> Vec<String> {
    let (tx, rx) = mpsc::channel::<String>();

    let hilo_nodo = thread::spawn(|| {
        let mut nodo = lanzar_nodo(config);
        thread::sleep(Duration::from_secs(50));
        nodo.kill().expect("error al matar nodo");
        let _ = nodo.wait().expect("Error al esperar cliente");
    });

    thread::sleep(Duration::from_secs(5));

    let hilo_cliente = thread::spawn(move || {
        let mut cliente = lanzar_cliente(port);

        if let Some(stdout) = cliente.stdout.take() {
            if let Some(stdin) = cliente.stdin.take() {
                escribir_entrada(comandos_cliente, stdin, false);
            }
            leer_salida(stdout, tx);
        }
        let _ = cliente.wait().expect("Error al esperar cliente");
    });

    hilo_cliente.join().expect("El hilo del cliente falló: {}");
    hilo_nodo.join().expect("El hilo del nodo falló: {}");
    devolver_salida(rx)
}

pub fn devolver_salida(rx: Receiver<String>) -> Vec<String> {
    let mut salida_cliente = Vec::new();

    for line in rx.iter() {
        salida_cliente.push(line);
    }
    salida_cliente
}

pub fn escribir_entrada(comandos_cliente: Vec<String>, mut stdin: ChildStdin, esperar: bool) {
    for comando in comandos_cliente {
        if let Err(e) = writeln!(stdin, "{comando}") {
            eprintln!("Error al escribir comando '{comando}': {e}");
        }
        thread::sleep(Duration::from_secs(3));
        esperar_otro_cliente(esperar);
    }

    thread::sleep(Duration::from_secs(5));

    drop(stdin);
}

pub fn leer_salida(stdout: ChildStdout, tx: Sender<String>) {
    let reader = io::BufReader::new(stdout);
    for line in reader.lines() {
        match line {
            Ok(l) => {
                if let Err(e) = tx.send(l) {
                    eprintln!("Error al enviar línea al hilo principal: {e}");
                }
            }
            Err(e) => {
                eprintln!("Error al leer la salida estándar del cliente 2: {e}")
            }
        }
        thread::sleep(Duration::from_secs(4));
    }
}

fn lanzar_nodo(node: String) -> Child {
    Command::new("cargo")
        .arg("run")
        .arg("--bin")
        .arg("redis_node")
        .arg("--")
        .arg(node)
        .current_dir("..")
        .spawn()
        .expect("No se pudo iniciar el nodo")
}

fn lanzar_cliente(port: String) -> Child {
    Command::new("cargo")
        .arg("run")
        .arg("--bin")
        .arg("cliente")
        .arg("--")
        .arg(port)
        .current_dir("..")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("No se pudo iniciar el cliente")
}

pub fn lanzar_masters() -> Vec<JoinHandle<()>> {
    let configs = [
        ("configs/redis_01.conf", 50),
        ("configs/redis_02.conf", 50),
        ("configs/redis_03.conf", 50),
    ];

    let mut hilos = Vec::new();

    for (conf, duracion) in configs {
        let conf_string = conf.to_string();
        let hilo = thread::spawn(move || {
            let mut nodo = lanzar_nodo(conf_string.clone());
            thread::sleep(Duration::from_secs(duracion));
            nodo.kill()
                .unwrap_or_else(|_| panic!("Error al matar nodo {conf_string}"));
            let _ = nodo.wait();
        });

        hilos.push(hilo);
    }

    hilos
}

fn esperar_otro_cliente(esperar: bool) {
    if esperar {
        thread::sleep(Duration::from_secs(7));
    }
}
