use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{JoinHandle, panicking, spawn};

/// Tipo que representa un trabajo (job) que puede ejecutarse en un hilo.
type Job = Box<dyn FnOnce() + Send + 'static>;

/// Enum que representa los mensajes que los hilos pueden recibir.
enum Message {
    /// Nuevo trabajo para ejecutar.
    NewJob(Job),
    /// Mensaje para terminar el hilo.
    Terminate,
}

/// Enum que representa los posibles errores que pueden ocurrir en el `ThreadPool`.
#[derive(Debug, PartialEq)]
pub enum ThreadPoolError {
    /// Error al intentar enviar un mensaje a un hilo trabajador.
    SendError,
    /// Error al intentar adquirir el bloqueo de un recurso.
    LockError,
}

impl fmt::Display for ThreadPoolError {
    /// Implementación del trait `fmt::Display` para `ThreadPoolError`.
    /// Permite que los errores sean impresos de manera legible.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ThreadPoolError::SendError => write!(f, "Failed to send message to worker thread"),
            ThreadPoolError::LockError => write!(f, "Failed to acquire lock"),
        }
    }
}

/// Guardián de un *worker*.
///
/// Vive durante toda la vida del hilo.
/// Cuando el hilo termina (ya sea por pánico o salida normal)
/// se ejecuta su `Drop`, que:
/// 1. Actualiza el contador de hilos vivos (`live`).
/// 2. Si el hilo murió por **`panic!`**, crea inmediatamente
///    otro *worker* para mantener constante el tamaño del pool.
///
/// El patrón corresponde a la técnica RAII: aprovechar `Drop`
/// para ejecutar lógica de limpieza/reposición automática.
struct Sentinel {
    shared: Arc<Shared>,
}

impl Drop for Sentinel {
    /// Lógica que se ejecuta al finalizar el hilo.
    ///
    /// - Siempre decrementa `live`, indicando que este *worker*
    ///   ya no está activo.
    /// - Si el hilo se está desenrollando por `panic!`
    ///   (`std::thread::panicking() == true`), invoca
    ///   [`Shared::spawn_worker`] para reponerlo.
    fn drop(&mut self) {
        self.shared.live.fetch_sub(1, Ordering::SeqCst);

        // Si hay pánico, lanzamos un nuevo hilo de reemplazo.
        if panicking() {
            self.shared.spawn_worker();
        }
    }
}

/// Datos compartidos por todos los *workers* del pool.
struct Shared {
    /// Cola de trabajo protegida por `Mutex`.
    receiver: Mutex<Receiver<Message>>,
    /// Número de hilos vivos en este momento.
    live: AtomicUsize,
    /// Número máximo de hilos que el pool debe mantener.
    max: usize,
}

impl Shared {
    /// Crea y lanza un nuevo *worker*.
    ///
    /// Pasos:
    /// 1. Incrementa el contador `live`.
    /// 2. Crea un hilo (`std::thread::spawn`) que:
    ///    * Instancia un [`Sentinel`] para vigilar su vida.
    ///    * Extrae mensajes de la cola (`receiver.recv()`).
    ///    * Ejecuta el trabajo (`job()`); un pánico dentro del
    ///      trabajo hará que el `Sentinel` reponga el hilo.
    ///    * Sale del bucle si recibe `Message::Terminate` o si
    ///      el canal se cierra (`Err(_)`).
    ///
    /// Devuelve el `JoinHandle` del hilo recién creado.
    fn spawn_worker(self: &Arc<Self>) -> JoinHandle<()> {
        self.live.fetch_add(1, Ordering::SeqCst);
        let shared = Arc::clone(self);

        spawn(move || {
            // El guardián garantiza reposición automática
            let _sentinel_guard = Sentinel {
                shared: Arc::clone(&shared),
            };

            loop {
                let msg = {
                    let rx = match shared.receiver.lock() {
                        Ok(guard) => guard,
                        Err(_) => break,
                    };
                    rx.recv()
                };

                match msg {
                    Ok(Message::NewJob(job)) => {
                        // Ejecutar el trabajo; si paniquea, Sentinel hará respawn
                        job();
                    }
                    Ok(Message::Terminate) | Err(_) => break,
                }
            }
            // Al salir del loop, `_sentinel_guard` entra en `drop()`
        })
    }
}

/// Estructura que representa un pool de hilos (*thread pool*).
///
/// Un `ThreadPool` mantiene una cantidad fija de hilos que ejecutan tareas concurrentemente.
/// Las tareas se envían al pool mediante el método [`execute`](Self::execute).
///
/// Cada hilo del pool toma tareas desde una cola compartida.
/// Si una tarea provoca un `panic!`, el hilo que la ejecutó será reemplazado automáticamente
/// para mantener constante la cantidad de hilos activos.
///
/// Cuando el `ThreadPool` es destruido (mediante `drop`), se envían señales de terminación
/// a todos los hilos y se espera a que cada uno finalice correctamente.
///
/// # Campos
///
/// - `handles`: Vector de `JoinHandle`, uno por cada hilo del pool, usados para esperar su finalización.
/// - `sender`: Canal para enviar mensajes (trabajos o señales de terminación) a los *workers*.
/// - `shared`: Estado compartido entre todos los hilos: la cola de tareas y contadores.
///
/// # Ejemplo
///
/// ```
/// use common::thread_pool::ThreadPool;
/// let pool = ThreadPool::new(4);
///
/// pool.execute(|| {
///     println!("Tarea ejecutada en un hilo del pool");
/// }).expect("TODO: panic message");
/// ```
pub struct ThreadPool {
    handles: Vec<Option<JoinHandle<()>>>,
    sender: Sender<Message>,
    shared: Arc<Shared>,
}

impl ThreadPool {
    /// Crea un nuevo `ThreadPool` con el número de hilos especificado.
    ///
    /// # Parámetros
    ///
    /// * `size`: Número de hilos que tendrá el `ThreadPool`.
    ///
    /// # Retorna
    ///
    /// * Una instancia de `ThreadPool`.
    pub fn new(mut size: usize) -> ThreadPool {
        if size == 0 {
            size += 1
        }

        let (tx, rx) = mpsc::channel();
        let shared = Arc::new(Shared {
            receiver: Mutex::new(rx),
            live: AtomicUsize::new(0),
            max: size,
        });

        let mut handles = Vec::with_capacity(size);
        for _ in 0..size {
            handles.push(Some(shared.spawn_worker()));
        }

        ThreadPool {
            handles,
            sender: tx,
            shared,
        }
    }

    /// Ejecuta un trabajo (job) en uno de los hilos del `ThreadPool`.
    ///
    /// # Parámetros
    ///
    /// * `f`: El trabajo a ejecutar, debe ser una función que implemente `FnOnce` y sea `Send`.
    ///
    /// # Retorna
    ///
    /// * `Ok(())`: Si el trabajo se envió correctamente al hilo.
    /// * `Err(ThreadPoolError::SendError)`: Si ocurrió un error al enviar el trabajo al hilo.
    pub fn execute<F>(&self, f: F) -> Result<(), ThreadPoolError>
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .send(Message::NewJob(Box::new(f)))
            .map_err(|_| ThreadPoolError::SendError)
    }

    /// Devuelve la cantidad de workers con los que fue creado el `ThreadPool`.
    ///
    /// Esta cantidad representa el número total de hilos que el pool debería mantener activos.
    /// Puede diferir de la cantidad actual de hilos en casos excepcionales, como fallos inesperados
    /// antes de que el mecanismo de recuperación los reemplace.
    ///
    /// # Ejemplo
    ///
    /// ```
    /// use common::thread_pool::ThreadPool;
    /// let pool = ThreadPool::new(4);
    /// assert_eq!(pool.cant_workers(), 4);
    /// ```
    pub fn cant_workers(&self) -> usize {
        self.shared.max
    }

    /// Devuelve la cantidad actual de workers activos (vivos) en el `ThreadPool`.
    ///
    /// Este valor puede cambiar dinámicamente. Si un hilo finaliza (por `panic!` o terminación),
    /// se intenta reemplazarlo automáticamente para mantener la cantidad original.
    ///
    /// # Ejemplo
    ///
    /// ```
    /// use common::thread_pool::ThreadPool;
    /// let pool = ThreadPool::new(4);
    /// let vivos = pool.cant_lives_workers();
    /// assert!(vivos <= 4);
    /// ```
    pub fn cant_lives_workers(&self) -> usize {
        self.shared.live.load(Ordering::SeqCst)
    }
}

impl Drop for ThreadPool {
    /// Libera de forma ordenada todos los recursos del `ThreadPool`.
    ///
    /// * Envía un mensaje [`Message::Terminate`] a cada *worker* → los hilos
    ///   salen de su bucle principal de manera limpia.
    /// * Después llama a `join()` sobre cada `JoinHandle` para bloquear el
    ///   hilo actual hasta que todos los *workers* hayan finalizado.
    ///
    /// De este modo:
    /// 1. Ningún hilo queda huérfano cuando el `ThreadPool` sale de *scope*.
    /// 2. Se evita terminar el proceso con hilos aún ejecutándose.
    fn drop(&mut self) {
        // 1 · Avisamos a todos los workers
        for _ in 0..self.shared.max {
            let _ = self.sender.send(Message::Terminate);
        }

        // 2 · Esperamos su finalización
        for handle_opt in &mut self.handles {
            if let Some(handle) = handle_opt.take() {
                let _ = handle.join(); // ignoramos el resultado
            }
        }
    }
}

impl fmt::Debug for ThreadPool {
    /// Imprime un resumen del estado interno del `ThreadPool`.
    ///
    /// Ejemplo de salida:
    ///
    /// ```text
    /// ThreadPool { workers: 4, live workers: 4 }
    /// ```
    ///
    /// * `workers`      → capacidad configurada (`max`)
    /// * `live workers` → cantidad de hilos actualmente vivos (`live`)
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ThreadPool")
            .field("workers", &self.shared.max)
            .field("lives workers", &self.shared.live.load(Ordering::SeqCst))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test01_thread_pool_creation() {
        // Prueba la creación de un ThreadPool con un tamaño válido
        let pool = ThreadPool::new(4);
        assert_eq!(pool.cant_workers(), 4); // Debe devolver un `ThreadPool`
    }

    #[test]
    fn test02_thread_pool_creation_with_zero_workers_default_one() {
        // Prueba que no se pueda crear un ThreadPool con 0 hilos
        let pool = ThreadPool::new(0);
        assert_eq!(pool.cant_workers(), 1);
    }

    #[test]
    fn test03_execute_job() {
        // Prueba que un trabajo se ejecute correctamente en el ThreadPool
        let pool = ThreadPool::new(4);
        let counter = Arc::new(Mutex::new(0));

        // Enviar un trabajo que incrementa el contador
        let counter_clone = Arc::clone(&counter);

        pool.execute(move || {
            let mut count = counter_clone.lock().unwrap();
            *count += 1;
        })
        .unwrap();

        // Esperar un momento para que el trabajo se ejecute
        thread::sleep(Duration::from_millis(100));

        // Verificar que el contador ha sido incrementado
        let count = counter.lock().unwrap();
        assert_eq!(*count, 1);
    }

    #[test]
    fn test04_execute_multiple_job1() {
        // Prueba que un trabajo se ejecute correctamente en el ThreadPool
        let pool = ThreadPool::new(4);
        let counter = Arc::new(Mutex::new(0));

        // Enviar un trabajo que incrementa el contador
        let counter_clone = Arc::clone(&counter);
        pool.execute(move || {
            let mut count = counter_clone.lock().unwrap();
            *count += 1;
        })
        .unwrap();

        let counter_clone = Arc::clone(&counter);
        pool.execute(move || {
            let mut count = counter_clone.lock().unwrap();
            *count += 1;
        })
        .unwrap();

        // Esperar un momento para que el trabajo se ejecute
        thread::sleep(Duration::from_millis(100));

        // Verificar que el contador ha sido incrementado
        let count = counter.lock().unwrap();
        assert_eq!(*count, 2);
    }

    #[test]
    fn test05_execute_multiple_job2() {
        // Prueba la ejecución de múltiples trabajos en paralelo
        let pool = ThreadPool::new(4);
        let counter = Arc::new(Mutex::new(0));

        // Enviar 10 trabajos que incrementan el contador
        for _ in 0..10 {
            let counter = Arc::clone(&counter);
            pool.execute(move || {
                let mut count = counter.lock().unwrap();
                *count += 1;
            })
            .unwrap();
        }

        // Esperar a que todos los trabajos terminen
        thread::sleep(Duration::from_millis(500));

        // Verificar que el contador ha sido incrementado correctamente
        let count = counter.lock().unwrap();
        assert_eq!(*count, 10);
    }

    #[test]
    fn test06_worker_panic_handling() {
        // Prueba que un panic dentro de un trabajador sea manejado
        let pool = ThreadPool::new(4);

        // Enviar un trabajo que cause un panic
        pool.execute(|| {
            panic!("This is a panic!");
        })
        .unwrap();
        thread::sleep(Duration::from_millis(100));
        pool.execute(|| println!("hola printeo algo desde el otro hilo"))
            .unwrap();

        thread::sleep(Duration::from_millis(100));
        // Verificar que no cause un crash en el pool
        assert_eq!(pool.cant_workers(), 4);
        assert_eq!(pool.cant_lives_workers(), 4);
    }

    #[test]
    fn test07_worker_panic_does_not_affect_pool() {
        // Verifica que el ThreadPool no se vea afectado por un panic en un trabajador
        let pool = ThreadPool::new(4);
        let counter = Arc::new(Mutex::new(0));

        // Enviar un trabajo que cause un panic
        pool.execute(|| {
            panic!("This worker panicked");
        })
        .unwrap();

        thread::sleep(Duration::from_millis(100));

        let counter_clone = Arc::clone(&counter);
        pool.execute(move || {
            let mut count = counter_clone.lock().unwrap();
            *count += 1;
        })
        .unwrap();

        thread::sleep(Duration::from_millis(100));

        // Verificar que el pool aún está funcionando
        let count = counter.lock().unwrap();
        assert_eq!(pool.cant_workers(), 4);
        assert_eq!(pool.cant_lives_workers(), 4);
        assert_eq!(*count, 1);
    }

    #[test]
    fn test08_thread_pool_print_debug() {
        // Verifica que el ThreadPool no se vea afectado por un panic en un trabajador
        let pool = ThreadPool::new(4);
        let debug_str = format!("{pool:?}");
        assert_eq!(debug_str, "ThreadPool { workers: 4, lives workers: 4 }");
    }
}
