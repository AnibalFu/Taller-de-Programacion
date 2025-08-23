//! Este modulo contiene la implementacion de la estructura logger,
//! encacargada de registrar mensajes al archivo de log de Redis
use chrono;
use std::{
    fmt::Display,
    fs::OpenOptions,
    io::Write,
    sync::mpsc::{Receiver, Sender, channel},
    thread::spawn,
};
/// Struct to hold the logger sender
#[derive(Debug, Clone)]
pub struct Logger {
    /// Sender to send log messages
    pub logger: Sender<String>,
}

impl Logger {
    /// Constructor de la estructura.
    /// Mueve la escritura de mensaje a un hilo, permitiendo
    /// recibir mensajes y escribir simultaneamente
    /// # Parametros
    /// * `filename`: nombre del archivo donde escribir los mensajes
    pub fn new(filename: &str) -> Logger {
        let (logger, receiver) = channel();
        let filename = filename.to_string();

        spawn(move || {
            write_to_file(&filename, receiver);
        });

        Logger { logger }
    }

    pub fn null() -> Self {
        let (sender, receiver) = channel::<String>();
        spawn(move || for _ in receiver {});
        Self { logger: sender }
    }

    /// Registra un mensaje de tipo info
    /// # Parametros
    /// * `message`: mensaje a registrar
    /// * `module`: unidad logica del programa que emite la informacion
    pub fn info(&self, message: &str, module: &str) {
        let now = chrono::Local::now();
        let timestamp = now.format("%Y-%m-%d %H:%M:%S").to_string();
        let log_message = format!("[INFO] - {timestamp} - {module}: {message}");
        self.log(log_message.as_str());
    }

    /// Registra un mensaje de tipo error
    /// # Parametros
    /// * `message`: mensaje a registrar
    /// * `module`: unidad logica del programa donde se produjo el error
    pub fn error(&self, message: &str, module: &str) {
        let now = chrono::Local::now();
        let timestamp = now.format("%Y-%m-%d %H:%M:%S").to_string();
        let log_message = format!("[ERROR] - {timestamp} - {module}: {message}");
        self.log(log_message.as_str());
    }

    /// Registra un mensaje de tipo warning
    /// # Parametros
    /// * `message`: mensaje a registrar
    /// * `module`: unidad logica del programa que emite la informacion
    pub fn warn(&self, message: &str, module: &str) {
        let now = chrono::Local::now();
        let timestamp = now.format("%Y-%m-%d %H:%M:%S").to_string();
        let log_message = format!("[WARNING] - {timestamp} - {module}: {message}");
        self.log(log_message.as_str());
    }

    /// Registra un mensaje de tipo DEBUG (muestra por consola)
    /// # Parametros
    /// * `message`: mensaje a registrar
    /// * `module`: unidad logica del programa que emite la informacion
    /// * `data`: informacion del caso particular
    pub fn debug<T: Display>(&self, message: &str, module: &str, data: T) {
        let now = chrono::Local::now();
        let timestamp = now.format("%Y-%m-%d %H:%M:%S").to_string();
        let log_message = format!("[DEBUG] - {timestamp} - {module}: {message} - {data}");
        self.log(log_message.as_str());
    }

    /// Envia un mensaje por el canal del logger
    /// # Parametros
    /// * `message`: mensaje a registrar
    fn log(&self, log_message: &str) {
        match self.logger.send(log_message.to_string()) {
            Ok(_) => {}
            Err(_) => {
                eprintln!("Error sending log message");
            }
        }
    }
}

/// Recibe mensajes y los escribe en un archivo con el nombre recibido por
/// parametro
///
/// # Parametros
/// * `filename`: nombre del archivo a crear para registrar los mensajes
/// * `reciever`: extremo de recepcion del canal que comparte con la
///   estructura principal por donde recibe los mensajes
fn write_to_file(filename: &str, receiver: Receiver<String>) {
    let file = OpenOptions::new().create(true).append(true).open(filename);

    let mut file = match file {
        Ok(file) => file,
        Err(e) => {
            eprintln!("Error opening log file: {e}");
            return;
        }
    };

    for message in receiver {
        if let Err(e) = writeln!(file, "{message}") {
            eprintln!("Error writing to log file: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use core::time;
    use std::{fs::remove_file, io::BufRead, thread::sleep};

    struct PersonTest {
        pub name: String,
        pub age: u32,
    }

    impl std::fmt::Display for PersonTest {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Name: {}, Age: {}", self.name, self.age)
        }
    }

    #[test]
    fn test_logger() {
        let logger = super::Logger::new("test.log");

        logger.info("This is an info message", "test_module");
        sleep(time::Duration::from_millis(100));
        logger.error("This is an error message", "test_module");
        sleep(time::Duration::from_millis(100));
        logger.debug("This is a debug message", "test_module", 42);
        sleep(time::Duration::from_millis(100));

        let file = std::fs::File::open("test.log").unwrap();
        let reader = std::io::BufReader::new(file);
        let lines: Vec<String> = reader.lines().map_while(Result::ok).collect();
        for line in lines {
            println!("{line}");
            assert!(
                line.contains("[INFO]") || line.contains("[ERROR]") || line.contains("[DEBUG]")
            );
        }
        remove_file("test.log").unwrap_or_default();
    }

    #[test]
    fn test_debug_with_struct() {
        let logger = super::Logger::new("test_struct.log");

        let person = PersonTest {
            name: "Tommy".to_string(),
            age: 26,
        };

        logger.debug("This is a debug message with struct", "test_module", person);
        sleep(time::Duration::from_millis(100));

        let file = std::fs::File::open("test_struct.log").unwrap();
        let reader = std::io::BufReader::new(file);
        let lines: Vec<String> = reader.lines().map_while(Result::ok).collect();

        for line in lines {
            assert!(line.contains("[DEBUG]"));
            assert!(line.contains("Name: Tommy"));
            assert!(line.contains("Age: 26"));
        }
        remove_file("test_struct.log").unwrap_or_default();
    }
}
