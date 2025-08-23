#[cfg(test)]
mod tests {

    use crate::utils::*;

    #[test]
    //#[ignore]
    pub fn test_01_auth_required() {
        let comandos = vec!["set hola 1".to_string()];
        let salida = ejecutar_test_cluster_cliente(comandos, "127.0.0.1:8089".to_string());
        dbg!(salida.clone());
        assert!(salida.contains(&"(error) NOAUTH Authentication required".to_string()));
    }

    #[test]
    //#[ignore]
    pub fn test_02_auth_user_success() {
        let comandos = vec!["auth user1 123456".to_string()];
        let salida = ejecutar_test_cluster_cliente(comandos, "127.0.0.1:8089".to_string());
        dbg!(salida.clone());
        assert!(salida.contains(&"\"User authenticated\"".to_string()));
    }

    #[test]
    //#[ignore]
    pub fn test_03_set() {
        let comandos = vec!["auth user1 123456".to_string(), "set hola 1".to_string()];
        let salida = ejecutar_test_cluster_cliente(comandos, "127.0.0.1:8089".to_string());
        dbg!(salida.clone());
        assert!(salida.contains(&"OK".to_string()));
    }

    #[test]
    //#[ignore]
    pub fn test_04_set_y_get() {
        let comandos = vec![
            "auth user1 123456".to_string(),
            "set hola 1".to_string(),
            "get hola".to_string(),
        ];
        let salida = ejecutar_test_cluster_cliente(comandos, "127.0.0.1:8089".to_string());
        dbg!(salida.clone());
        assert!(salida.contains(&"OK".to_string()));
        assert!(salida.contains(&"\"1\"".to_string()));
    }

    #[test]
    //#[ignore]
    pub fn test_05_listas() {
        let comandos = vec![
            "auth user1 123456".to_string(),
            "LPUSH halo hello".to_string(),
            "LPUSH halo world".to_string(),
            "LRANGE halo 0 -1".to_string(),
        ];
        let salida = ejecutar_test_cluster_cliente(comandos, "127.0.0.1:8089".to_string());
        dbg!(salida.clone());
        assert!(salida.contains(&"1) \"world\"".to_string()));
        assert!(salida.contains(&"2) \"hello\"".to_string()));
    }

    #[test]
    //#[ignore]
    pub fn test_06_moved_error() {
        let comandos = vec![
            "auth user1 123456".to_string(),
            "LPUSH mylist hello".to_string(),
        ];
        let salida = ejecutar_test_cluster_cliente(comandos, "127.0.0.1:8089".to_string());
        dbg!(salida.clone());
        assert!(salida.contains(&"(error) MOVED 5282 127.0.0.1:8088".to_string()));
    }

    #[test]
    //#[ignore]
    pub fn test_07_persistencia() {
        let comandos_1 = vec![
            "auth user1 123456".to_string(),
            "set hola mundo".to_string(),
        ];
        let salida = ejecutar_test_cluster_cliente(comandos_1, "127.0.0.1:8089".to_string());
        dbg!(salida.clone());
        assert!(salida.contains(&"OK".to_string()));

        let comandos_2 = vec!["auth user1 123456".to_string(), "get hola".to_string()];
        let salida_2 = ejecutar_test_cluster_cliente(comandos_2, "127.0.0.1:8089".to_string());
        dbg!(salida_2.clone());
        assert!(salida_2.contains(&"\"mundo\"".to_string()));
    }

    #[test]
    //#[ignore]
    pub fn test_08_pub_sub_subscribe() {
        let comandos_1 = vec![
            "auth user1 123456".to_string(),
            "publish canal_prueba msg_prueba".to_string(),
        ];
        let comandos_2 = vec![
            "auth user1 123456".to_string(),
            "subscribe canal_prueba".to_string(),
        ];
        let port = "127.0.0.1:8089".to_string();
        let salida = ejecutar_test_cluster_dos_clientes(
            comandos_1,
            port.to_string(),
            comandos_2,
            port,
            false,
        );
        assert!(salida.contains(&"1) \"subscribe\"".to_string()));
        assert!(salida.contains(&"2) \"canal_prueba\"".to_string()));
        assert!(salida.contains(&"3) (integer) 1".to_string()));
        assert!(salida.contains(&"1) \"message\"".to_string()));
        assert!(salida.contains(&"2) \"canal_prueba\"".to_string()));
        assert!(salida.contains(&"3) \"msg_prueba\"".to_string()));
    }

    #[test]
    //#[ignore]
    pub fn test_09_cluster_fail() {
        let comandos_1 = vec![
            "auth user1 123456".to_string(),
            "set hola mundo".to_string(),
        ];
        let salida = ejecutar_test_nodo_cliente(
            comandos_1,
            "configs/redis_01.conf".to_string(),
            "127.0.0.1:8088".to_string(),
        );
        assert!(salida.contains(&"(error) CLUSTERDOWN The cluster is down".to_string()));
    }

    #[test]
    //#[ignore]
    pub fn test_10_get_y_set_dos_cli() {
        let comandos_1 = vec!["auth user1 123456".to_string(), "set hola 1".to_string()];
        let comandos_2 = vec!["auth user1 123456".to_string(), "get hola".to_string()];
        let port = "127.0.0.1:8089".to_string();
        let salida = ejecutar_test_cluster_dos_clientes(
            comandos_1,
            port.to_string(),
            comandos_2,
            port,
            true,
        );
        dbg!(salida.clone());

        assert!(salida.contains(&"\"1\"".to_string()));
    }

    #[test]
    //#[ignore]
    pub fn test_11_pub_sub_unsubscribe() {
        let comandos_1 = vec![
            "auth user1 123456".to_string(),
            "publish canal_prueba msg_prueba".to_string(),
        ];
        let comandos_2 = vec![
            "auth user1 123456".to_string(),
            "subscribe canal_prueba".to_string(),
            "unsubscribe canal_prueba".to_string(),
        ];
        let port = "127.0.0.1:8089".to_string();
        let salida = ejecutar_test_cluster_dos_clientes(
            comandos_1,
            port.to_string(),
            comandos_2,
            port,
            false,
        );
        dbg!(salida.clone());

        assert!(salida.contains(&"1) \"subscribe\"".to_string()));
        assert!(salida.contains(&"2) \"canal_prueba\"".to_string()));
        assert!(salida.contains(&"3) (integer) 1".to_string()));
        assert!(salida.contains(&"1) \"unsubscribe\"".to_string()));
        assert!(salida.contains(&"2) \"canal_prueba\"".to_string()));
        assert!(salida.contains(&"3) (integer) 0".to_string()));
    }

    #[test]
    //#[ignore]
    pub fn test_12_pub_sub_pubsubchannels() {
        let comandos = vec![
            "auth user1 123456".to_string(),
            "subscribe canal_prueba".to_string(),
            "subscribe canal_prueba2".to_string(),
            "pubsub channels".to_string(),
        ];
        let salida = ejecutar_test_cluster_cliente(comandos, "127.0.0.1:8089".to_string());
        assert!(
            salida
                .iter()
                .any(|linea| linea.contains("1) \"subscribe\""))
        );
        assert!(
            salida
                .iter()
                .any(|linea| linea.contains("2) \"canal_prueba\""))
        );
        assert!(salida.iter().any(|linea| linea.contains("3) (integer) 1")));

        assert!(
            salida
                .iter()
                .any(|linea| linea.contains("1) \"subscribe\""))
        );
        assert!(
            salida
                .iter()
                .any(|linea| linea.contains("2) \"canal_prueba2\""))
        );
        assert!(salida.iter().any(|linea| linea.contains("3) (integer) 2")));

        assert!(
            salida
                .iter()
                .any(|linea| linea.contains("\"canal_prueba\""))
        );
        assert!(
            salida
                .iter()
                .any(|linea| linea.contains("\"canal_prueba2\""))
        );
    }

    #[test]
    //#[ignore]
    pub fn test_13_pub_sub_pubsub_numsub() {
        let comandos = vec![
            "auth user1 123456".to_string(),
            "subscribe canal_prueba".to_string(),
            "subscribe canal_prueba2".to_string(),
            "pubsub numsub".to_string(),
        ];
        let salida = ejecutar_test_cluster_cliente(comandos, "127.0.0.1:8089".to_string());
        assert!(
            salida
                .iter()
                .any(|linea| linea.contains("1) \"subscribe\""))
        );
        assert!(
            salida
                .iter()
                .any(|linea| linea.contains("2) \"canal_prueba\""))
        );
        assert!(salida.iter().any(|linea| linea.contains("3) (integer) 1")));
        assert!(
            salida
                .iter()
                .any(|linea| linea.contains("1) \"subscribe\""))
        );
        assert!(
            salida
                .iter()
                .any(|linea| linea.contains("2) \"canal_prueba2\""))
        );
        assert!(salida.iter().any(|linea| linea.contains("3) (integer) 2")));
        assert!(
            salida
                .iter()
                .any(|linea| linea.contains("\"canal_prueba\""))
        );
        assert!(salida.iter().any(|linea| linea.contains("2) (integer) 1")));
        assert!(
            salida
                .iter()
                .any(|linea| linea.contains("\"canal_prueba2\""))
        );
        assert!(salida.iter().any(|linea| linea.contains("4) (integer) 1")));
    }
}
