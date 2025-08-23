#[cfg(test)]
mod tests {

    use std::io::Write;
    use std::io::{BufRead, BufReader};
    use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::thread;
    use std::time::Duration;
    #[test]
    fn test_rep_promotion() {
        let hilo_nodo_1 = thread::spawn(|| {
            let mut nodo = lanzar_nodo("configs/redis_01.conf".to_string());
            thread::sleep(Duration::from_secs(80));
            nodo.kill().expect("error al matar hilo 1");
            let _ = nodo.wait().expect("Error al esperar nodo");
        });

        let hilo_nodo_2 = thread::spawn(|| {
            let mut nodo = lanzar_nodo("configs/redis_02.conf".to_string());
            thread::sleep(Duration::from_secs(50));
            nodo.kill().expect("error al matar hilo 2");
            let _ = nodo.wait().expect("Error al esperar nodo");
        });

        let hilo_nodo_3 = thread::spawn(|| {
            let mut nodo = lanzar_nodo("configs/redis_03.conf".to_string());
            thread::sleep(Duration::from_secs(80));
            nodo.kill().expect("error al matar hilo 3");
            let _ = nodo.wait().expect("Error al esperar nodo");
        });

        let hilo_nodo_6 = thread::spawn(|| {
            let mut nodo = lanzar_nodo("configs/redis_06.conf".to_string());
            thread::sleep(Duration::from_secs(80));
            nodo.kill().expect("error al matar hilo 6");
            let _ = nodo.wait().expect("Error al esperar nodo");
        });

        let hilo_nodo_7 = thread::spawn(|| {
            let mut nodo = lanzar_nodo("configs/redis_07.conf".to_string());
            thread::sleep(Duration::from_secs(80));
            nodo.kill().expect("error al matar hilo 7");
            let _ = nodo.wait().expect("Error al esperar nodo");
        });

        hilo_nodo_2.join().expect("Falla hilo 2");
        thread::sleep(Duration::from_secs(15));

        let (tx, rx) = mpsc::channel::<String>();
        let hilo_cliente = thread::spawn(move || {
            let mut cliente = lanzar_cliente("127.0.0.1:8088".to_string());
            if let Some(stdout) = cliente.stdout.take() {
                if let Some(stdin) = cliente.stdin.take() {
                    escribir_entrada(stdin);
                } else {
                    panic!("Error al escribir en el cliente");
                }
                leer_salida(stdout, tx);
            } else {
                panic!("Error al leer del cliente");
            }
            cliente.kill().expect("Error al matar cliente");
            let _ = cliente.wait().expect("Error al esperar cliente");
        });

        hilo_cliente.join().expect("Falla cliente");
        hilo_nodo_1.join().expect("Falla hilo 1");
        hilo_nodo_3.join().expect("Falla hilo 3");
        hilo_nodo_6.join().expect("Falla hilo 6");
        hilo_nodo_7.join().expect("Falla hilo 7");

        let salida = devolver_salida(rx);
        let promocion = salida.contains(&"(error) MOVED 6539 127.0.0.1:8093".to_string())
            || salida.contains(&"(error) MOVED 6539 127.0.0.1:8094".to_string());
        assert!(promocion);
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

    fn escribir_entrada(mut stdin: ChildStdin) {
        if let Err(e) = writeln!(stdin, "auth user1 123456") {
            panic!("Error al escribir comando 'auth user1 123456': {e}");
        }
        if let Err(e) = writeln!(stdin, "set hola mundo") {
            panic!("Error al escribir comando 'set hola mundo': {e}");
        }
    }

    fn leer_salida(stdout: ChildStdout, tx: Sender<String>) {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            match line {
                Ok(l) => {
                    if let Err(e) = tx.send(l) {
                        panic!("Error al enviar línea al hilo principal: {e}");
                    }
                }
                Err(e) => {
                    panic!("Error al leer la salida estándar: {e}")
                }
            }
            thread::sleep(Duration::from_secs(5));
        }
    }

    fn devolver_salida(rx: Receiver<String>) -> Vec<String> {
        let mut salida_cliente = Vec::new();
        for line in rx.iter() {
            salida_cliente.push(line);
        }
        salida_cliente
    }
}
