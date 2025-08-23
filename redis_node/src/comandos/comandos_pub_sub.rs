//! Este modulo contiene los comandos de tipo pub sub de los nodos

use crate::client_struct::client::Client;
use crate::comandos::const_cmd::{
    CMD_PSUBSCRIBE, CMD_PUBSUB_CHANNELS, CMD_PUBSUB_NUMPAT, CMD_PUBSUB_NUMSUB,
    CMD_PUBSUB_SHARDCHANNELS, CMD_PUBSUB_SHARDNUMSUB, CMD_PUNSUBSCRIBE, CMD_SPUBLISH,
    CMD_SSUBSCRIBE, CMD_SUBSCRIBE, CMD_SUNSUBSCRIBE, CMD_UNSUBSCRIBE,
};
use crate::comandos::pub_sub_struct::{Canal, Channels, PubSubCore};
use crate::comandos::utils::{
    assert_correct_arguments_quantity, assert_number_of_arguments_distinct,
};
use crate::constantes::TOTAL_SLOTS;
use common::cr16::crc16;
use glob::Pattern;
use redis_client::tipos_datos::arrays::Arrays;
use redis_client::tipos_datos::traits::{DatoRedis, TipoDatoRedis};
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

impl PubSubCore {
    /// Maneja los comandos de suscripción:
    /// `SUBSCRIBE`, `PSUBSCRIBE` y `SSUBSCRIBE`.
    ///
    /// Según el primer token deriva a:
    /// * `subscribe_normal`   → canales clásicos.
    /// * `subscribe_pattern`  → patrones (`*`, `?`, `[ae]`, …).
    /// * `subscribe_shard`    → *shard‑channels* (canales asociados a un slot).
    ///
    /// # Parámetros
    /// * `tokens`  – `["<SUB|PSUB|SSUB>",
    ///                canales|patrones|shards…]`.
    /// * `client`  – `Arc<RwLock<Client>>` que solicita la suscripción.
    ///
    /// # Retorno
    /// `DatoRedis::Array` con una tripleta por cada ítem:
    /// `["<sub|psub|ssub>scribe", canal|patrón|shard, total_actual]`
    ///
    /// # Errores
    /// * Argumentos insuficientes.
    /// * Fallo al obtener el lock del cliente.
    ///
    /// # Efectos
    /// * Actualiza `self.channels`, `self.pchannels` o `self.schannels`.
    /// * Actualiza la lista interna de suscripciones del cliente.
    pub(crate) fn subscribe(
        &mut self,
        tokens: &[String],
        client: Arc<RwLock<Client>>,
    ) -> Result<DatoRedis, DatoRedis> {
        let cmd = tokens[0].to_uppercase();
        match cmd.as_str() {
            CMD_SUBSCRIBE => self.subscribe_normal(tokens, client),
            CMD_PSUBSCRIBE => self.subscribe_pattern(tokens, client),
            CMD_SSUBSCRIBE => self.subscribe_shard(tokens, client),
            _ => Err(DatoRedis::new_simple_error(
                "ERR".into(),
                "unknown sub cmd".into(),
            )),
        }
    }

    /// Maneja la suscripción a canales específicos con el comando `SUBSCRIBE`.
    ///
    /// # Parámetros
    /// * `tokens` - Vector con el comando y los canales a suscribirse.
    /// * `channels` - Mapa de canales a suscriptores.
    /// * `client` - Referencia al cliente que realiza la suscripción.
    ///
    /// # Retorno
    /// RESP Array con confirmaciones de suscripción por canal:
    /// ```text
    /// ["subscribe", canal, cantidad_total_suscripciones]
    /// ```
    ///
    /// # Errores
    /// - Si hay error al adquirir el lock del cliente.
    /// - Si la cantidad de argumentos es inválida.
    ///
    /// # Efectos
    /// - Registra al cliente en cada canal.
    /// - Actualiza los canales del cliente.
    fn subscribe_normal(
        &mut self,
        tokens: &[String],
        client: Arc<RwLock<Client>>,
    ) -> Result<DatoRedis, DatoRedis> {
        assert_correct_arguments_quantity(tokens[0].clone(), 2, tokens.len())?;

        let mut cli = client
            .write()
            .map_err(|e| DatoRedis::new_simple_error("ERR".into(), format!("client lock: {e}")))?;

        let mut resp = Arrays::new();
        for canal in tokens.iter().skip(1) {
            self.channels
                .entry(canal.clone())
                .or_default()
                .insert(cli.client_id(), cli.get_sender());

            cli.add_channel(canal.clone());

            resp.append(DatoRedis::new_bulk_string("subscribe".into())?);
            resp.append(DatoRedis::new_bulk_string(canal.clone())?);
            resp.append(DatoRedis::new_integer(cli.get_channels().len() as i64));
        }
        Ok(DatoRedis::new_array_con_contenido(resp))
    }

    /// Maneja la suscripción a patrones con el comando `PSUBSCRIBE`.
    ///
    /// # Parámetros
    /// * `tokens` - Vector con el comando y los patrones a suscribirse.
    /// * `pchannels` - Mapa de patrones a suscriptores.
    /// * `client` - Referencia al cliente que realiza la suscripción.
    ///
    /// # Retorno
    /// RESP Array con confirmaciones de suscripción por patrón:
    /// ```text
    /// ["psubscribe", patron, cantidad_total_suscripciones]
    /// ```
    ///
    /// # Errores
    /// - Si hay error al adquirir el lock del cliente.
    /// - Si la cantidad de argumentos es inválida.
    ///
    /// # Efectos
    /// - Registra al cliente en cada patrón.
    /// - Actualiza los patrones del cliente.
    fn subscribe_pattern(
        &mut self,
        tokens: &[String],
        client: Arc<RwLock<Client>>,
    ) -> Result<DatoRedis, DatoRedis> {
        assert_correct_arguments_quantity(tokens[0].to_string(), 2, tokens.len())?;

        let mut cli = client.write().map_err(|e| {
            DatoRedis::new_simple_error("ERR".to_string(), format!("client lock error: {e}"))
        })?;

        let mut respuesta = Arrays::new();

        for patron in tokens.iter().skip(1) {
            let patron: Canal = patron.to_string();

            // Agrego el sender a pchannels
            let canal_sub = self.pchannels.entry(patron.to_string()).or_default();

            canal_sub.insert(cli.client_id(), cli.get_sender());

            // Al cliente le agrego su nuevo patrón
            cli.add_pchannel(patron.to_string());

            // Formato de respuesta RESP para PSUBSCRIBE
            respuesta.append(DatoRedis::new_bulk_string("psubscribe".to_string())?);
            respuesta.append(DatoRedis::new_bulk_string(patron)?);
            respuesta.append(DatoRedis::new_integer(cli.get_pchannels().len() as i64));
        }

        Ok(DatoRedis::new_array_con_contenido(respuesta))
    }

    /// Suscribe un cliente a **shard‑channels** (`SSUBSCRIBE`).
    ///
    /// Cada nombre de shard se mapea a un *slot* CRC16 Redis
    /// y se valida contra `self.slot_range`:
    /// * Si **pertenece** al rango, se registra en `self.schannels`.
    /// * Si **no pertenece**, se devuelve un error `MOVED <slot>` en la
    ///   misma posición del array de respuesta.
    ///
    /// # Parámetros
    /// * `tokens` – `["SSUBSCRIBE", shard1, shard2, …]`.
    /// * `client` – cliente solicitante.
    ///
    /// # Retorno
    /// Mezcla de elementos:
    /// * Éxito → `["ssubscribe", shard, total]`.
    /// * Redirección → `shard`, `Moved(slot)`.
    ///
    /// # Efectos
    /// * Inserta el `Sender` en `self.schannels` solo para los shards del
    ///   rango local.
    /// * Añade el shard a `client.schannels`.
    fn subscribe_shard(
        &mut self,
        tokens: &[String],
        client: Arc<RwLock<Client>>,
    ) -> Result<DatoRedis, DatoRedis> {
        assert_correct_arguments_quantity(tokens[0].clone(), 2, tokens.len())?;

        let mut cli = client
            .write()
            .map_err(|e| DatoRedis::new_simple_error("ERR".into(), format!("client lock: {e}")))?;

        let mut resp = Arrays::new();
        for shard in tokens.iter().skip(1) {
            let (slot_valido, slot) = self.validar_slots(shard);
            if !slot_valido {
                resp.append(DatoRedis::new_bulk_string(shard.clone())?);
                resp.append(DatoRedis::new_moved_error(slot));
                continue;
            }

            self.schannels
                .entry(shard.clone())
                .or_default()
                .insert(cli.client_id(), cli.get_sender());

            cli.add_schannel(shard.clone());

            resp.append(DatoRedis::new_bulk_string("ssubscribe".into())?);
            resp.append(DatoRedis::new_bulk_string(shard.clone())?);
            resp.append(DatoRedis::new_integer(cli.get_schannels().len() as i64));
        }
        Ok(DatoRedis::new_array_con_contenido(resp))
    }

    /// Publica un mensaje a todos los clientes suscritos a:
    /// - Un canal (`PUBLISH`)
    /// - Un patrón (`PUBLISH`)
    /// - Un canal con slot válido (`SPUBLISH`)
    ///
    /// # Parámetros
    /// * `tokens` – Vector con el comando y argumentos:
    ///   - `["PUBLISH", canal, mensaje]`
    ///   - `["SPUBLISH", canal, mensaje]`
    /// * `_client` – Cliente que envía el mensaje (no usado).
    ///
    /// # Retorno
    /// `DatoRedis::Integer` con la cantidad de clientes notificados.
    ///
    /// # Errores
    /// - Si el comando tiene argumentos inválidos.
    /// - Si falla el envío a algún cliente.
    pub(crate) fn publish(
        &mut self,
        tokens: &[String],
        _client: Arc<RwLock<Client>>,
    ) -> Result<DatoRedis, DatoRedis> {
        self.publish_internal(tokens)
    }

    /// Implementa el comportamiento interno del comando `PUBLISH` o `SPUBLISH`.
    ///
    /// Llama a las funciones específicas según el tipo de publicación:
    /// - Canales normales     → `publish_normal`
    /// - Patrones coincidentes→ `publish_pattern`
    /// - Shard‑channels       → `publish_shard`
    ///
    /// # Parámetros
    /// * `tokens` – `["PUBLISH"|"SPUBLISH", canal, mensaje]`.
    ///
    /// # Retorno
    /// `DatoRedis::Integer` con el total de entregas exitosas.
    pub(crate) fn publish_internal(&mut self, tokens: &[String]) -> Result<DatoRedis, DatoRedis> {
        assert_number_of_arguments_distinct(tokens[0].clone(), 3, tokens.len())?;
        let tipo = &tokens[0].to_uppercase();
        let canal = &tokens[1];
        let mensaje = &tokens[2];

        let mut entregados = 0;
        match tipo.as_str() {
            CMD_SPUBLISH => {
                entregados += self.publish_shard(canal, mensaje)?;
            }
            _ => {
                entregados += self.publish_normal(canal, mensaje)?;
                entregados += self.publish_pattern(canal, mensaje)?;
            }
        }

        Ok(DatoRedis::new_integer(entregados))
    }

    /// Envía un mensaje a todos los clientes suscritos directamente a un canal.
    ///
    /// # Parámetros
    /// - `canal`: Nombre del canal al que se publica.
    /// - `mensaje`: Contenido del mensaje.
    ///
    /// # Retorno
    /// - `Ok(i64)`: Cantidad de clientes a los que se les envió el mensaje.
    /// - `Err(DatoRedis)`: Error al enviar el mensaje por un sender roto o inaccesible.
    ///
    /// # Formato RESP enviado a cada cliente:
    /// ```text
    /// *3
    /// $7
    /// message
    /// $<canal>
    /// $<mensaje>
    /// ```
    fn publish_normal(&mut self, canal: &str, mensaje: &str) -> Result<i64, DatoRedis> {
        let mut entregados = 0;

        if let Some(clientes) = self.channels.get(canal) {
            let mut arr = Arrays::new();
            arr.append(DatoRedis::new_bulk_string("message".into())?);
            arr.append(DatoRedis::new_bulk_string(canal.into())?);
            arr.append(DatoRedis::new_bulk_string(mensaje.into())?);
            let resp = DatoRedis::new_array_con_contenido(arr);

            for sender in clientes.values() {
                sender
                    .send(resp.convertir_a_protocolo_resp())
                    .map_err(|e| DatoRedis::new_simple_error("ERR".into(), e.to_string()))?;
                entregados += 1;
            }
        }
        Ok(entregados)
    }

    /// Envía un mensaje a todos los clientes suscritos a patrones que coincidan con el canal.
    ///
    /// # Parámetros
    /// - `canal`: Nombre del canal al que se publica.
    /// - `mensaje`: Contenido del mensaje.
    ///
    /// # Retorno
    /// - `Ok(i64)`: Cantidad de clientes a los que se les envió el mensaje.
    /// - `Err(DatoRedis)`: Error al enviar el mensaje.
    ///
    /// # Formato RESP enviado a cada cliente:
    /// ```text
    /// *4
    /// $8
    /// pmessage
    /// $<patrón>
    /// $<canal>
    /// $<mensaje>
    /// ```
    fn publish_pattern(&mut self, canal: &str, mensaje: &str) -> Result<i64, DatoRedis> {
        let mut entregados = 0;

        for (patron, clientes) in &self.pchannels {
            if matches_pattern(canal, patron) {
                let mut arr = Arrays::new();
                arr.append(DatoRedis::new_bulk_string("pmessage".into())?);
                arr.append(DatoRedis::new_bulk_string(patron.into())?);
                arr.append(DatoRedis::new_bulk_string(canal.into())?);
                arr.append(DatoRedis::new_bulk_string(mensaje.into())?);
                let resp = DatoRedis::new_array_con_contenido(arr);

                for sender in clientes.values() {
                    sender
                        .send(resp.convertir_a_protocolo_resp())
                        .map_err(|e| DatoRedis::new_simple_error("ERR".into(), e.to_string()))?;
                    entregados += 1;
                }
            }
        }
        Ok(entregados)
    }

    /// Envía un mensaje a todos los clientes suscritos directamente a un canal.
    ///
    /// # Parámetros
    /// - `canal`: Nombre del canal al que se publica.
    /// - `mensaje`: Contenido del mensaje.
    ///
    /// # Retorno
    /// - `Ok(i64)`: Cantidad de clientes a los que se les envió el mensaje.
    /// - `Err(DatoRedis)`: Error al enviar el mensaje por un sender roto o inaccesible.
    ///
    /// # Formato RESP enviado a cada cliente:
    /// ```text
    /// *3
    /// $8
    /// smessage
    /// $<canal>
    /// $<mensaje>
    /// ```
    fn publish_shard(&mut self, canal: &str, mensaje: &str) -> Result<i64, DatoRedis> {
        let mut entregados = 0;

        if !self.validar_slots(canal).0 {
            return Ok(entregados);
        }

        if let Some(clientes) = self.schannels.get(canal) {
            let mut arr = Arrays::new();
            arr.append(DatoRedis::new_bulk_string("smessage".into())?);
            arr.append(DatoRedis::new_bulk_string(canal.into())?);
            arr.append(DatoRedis::new_bulk_string(mensaje.into())?);
            let resp = DatoRedis::new_array_con_contenido(arr);

            for sender in clientes.values() {
                sender
                    .send(resp.convertir_a_protocolo_resp())
                    .map_err(|e| DatoRedis::new_simple_error("ERR".into(), e.to_string()))?;
                entregados += 1;
            }
        }

        Ok(entregados)
    }

    /// Procesa la desuscripción (`UNSUBSCRIBE`, `PUNSUBSCRIBE` o `SUNSUBSCRIBE`) del cliente
    /// de uno o más canales o patrones.
    ///
    /// # Parámetros
    /// - `tokens`: Vector con el comando y los canales o patrones a desuscribir.
    /// - `client`: Cliente que solicita la desuscripción.
    ///
    /// # Retorno
    /// Respuesta RESP con confirmaciones para cada canal o patrón desuscrito..
    pub(crate) fn unsubscribe(
        &mut self,
        tokens: &[String],
        client: Arc<RwLock<Client>>,
    ) -> Result<DatoRedis, DatoRedis> {
        match tokens[0].to_uppercase().as_str() {
            CMD_UNSUBSCRIBE => self.unsubscribe_normal(tokens, client),
            CMD_PUNSUBSCRIBE => self.unsubscribe_pattern(tokens, client),
            CMD_SUNSUBSCRIBE => self.sunsubscribe_shard(tokens, client),
            _ => Err(DatoRedis::new_simple_error(
                "ERR".into(),
                "unknown unsubscribe cmd".into(),
            )),
        }
    }

    /// Desuscribe el cliente de canales regulares (`UNSUBSCRIBE`).
    ///
    /// Llama a `unsubscribe_generico` con funciones específicas para canales normales.
    fn unsubscribe_normal(
        &mut self,
        tokens: &[String],
        client: Arc<RwLock<Client>>,
    ) -> Result<DatoRedis, DatoRedis> {
        Self::unsubscribe_generico(
            tokens,
            &mut self.channels,
            client,
            Client::remove_channel,
            Client::get_channels,
            CMD_UNSUBSCRIBE,
        )
    }

    /// Desuscribe el cliente de patrones (`PUNSUBSCRIBE`).
    ///
    /// Llama a `unsubscribe_generico` con funciones específicas para patrones.
    fn unsubscribe_pattern(
        &mut self,
        tokens: &[String],
        client: Arc<RwLock<Client>>,
    ) -> Result<DatoRedis, DatoRedis> {
        Self::unsubscribe_generico(
            tokens,
            &mut self.pchannels,
            client,
            Client::remove_pchannel,
            Client::get_pchannels,
            CMD_PUNSUBSCRIBE,
        )
    }

    /// Desuscribe el cliente de shard-channels (`SUNSUBSCRIBE`).
    ///
    /// Llama a `unsubscribe_generico` con funciones específicas para shard-channels.
    fn sunsubscribe_shard(
        &mut self,
        tokens: &[String],
        client: Arc<RwLock<Client>>,
    ) -> Result<DatoRedis, DatoRedis> {
        Self::unsubscribe_generico(
            tokens,
            &mut self.schannels,
            client,
            Client::remove_schannel,
            Client::get_schannels,
            CMD_SUNSUBSCRIBE,
        )
    }

    /// Función genérica para desuscribir a un cliente de múltiples canales o patrones.
    ///
    /// Si no se indican canales (es decir, solo el comando), se desuscribe de todos.
    ///
    /// # Parámetros
    /// - `tokens`: Comando y lista de canales o patrones a eliminar.
    /// - `mapa_canales`: Mapa de canales o patrones activos con sus suscriptores.
    /// - `client`: Cliente a desuscribir.
    /// - `remover_del_cliente`: Función para eliminar un canal o patrón del cliente.
    /// - `obtener_del_cliente`: Función para obtener las suscripciones actuales del cliente.
    /// - `nombre_comando`: Nombre del comando para respuesta (e.g. "unsubscribe").
    ///
    /// # Retorno
    /// Respuesta RESP con confirmaciones de desuscripción para cada canal o patrón..
    fn unsubscribe_generico(
        tokens: &[String],
        mapa_canales: &mut Channels,
        client: Arc<RwLock<Client>>,
        remover_del_cliente: fn(&mut Client, &Canal),
        obtener_del_cliente: fn(&Client) -> &HashSet<String>,
        nombre_comando: &str,
    ) -> Result<DatoRedis, DatoRedis> {
        let mut cli = client
            .write()
            .map_err(|e| DatoRedis::new_simple_error("ERR".into(), format!("client lock: {e}")))?;

        // canales a eliminar
        let canales_a_borrar: Vec<String> = if tokens.len() == 1 {
            obtener_del_cliente(&cli).iter().cloned().collect()
        } else {
            tokens.iter().skip(1).cloned().collect()
        };

        // armar respuesta
        let mut resp = Arrays::new();
        for canal in &canales_a_borrar {
            if let Some(subs) = mapa_canales.get_mut(canal) {
                subs.remove(&cli.client_id());
                if subs.is_empty() {
                    mapa_canales.remove(canal);
                }
            }
            remover_del_cliente(&mut cli, canal);

            resp.append(DatoRedis::new_bulk_string(nombre_comando.to_lowercase())?);
            resp.append(DatoRedis::new_bulk_string(canal.clone())?);
            resp.append(DatoRedis::new_integer(
                obtener_del_cliente(&cli).len() as i64
            ));
        }

        Ok(DatoRedis::new_array_con_contenido(resp))
    }

    /// Maneja los comandos relacionados con `PUBSUB <subcommand>`.
    ///
    /// Esta función procesa los subcomandos del comando `PUBSUB`, que proporcionan
    /// información sobre el sistema de publicación/suscripción del servidor.
    ///
    /// # Argumentos
    ///
    /// * `tokens` - Slice de strings que representa el comando y sus argumentos.
    ///   El primer elemento debe ser "PUBSUB", el segundo es el subcomando,
    ///   y los siguientes son argumentos específicos del subcomando.
    /// * `_client` - Referencia al cliente que realiza la consulta, actualmente no utilizado.
    ///
    /// # Subcomandos soportados
    ///
    /// 1. `NUMSUB [channel-1 channel-2 ...]` - Devuelve pares `[canal, cantidad]`
    ///    con la cantidad de suscriptores por canal. Si no se especifican canales,
    ///    muestra todos los canales activos.
    /// 2. `CHANNELS [pattern]` - Devuelve una lista de nombres de canales con al menos
    ///    un suscriptor, que coinciden opcionalmente con un patrón de filtro tipo glob.
    /// 3. `NUMPAT` - Devuelve un número entero con la cantidad de suscripciones activas
    ///    a patrones.
    /// 4. `SCHANNELS [pattern]` - Lista shard channels con suscriptores, opcionalmente filtrados.
    /// 5. `SNUMSUB [channel-1 channel-2 ...]` - Cantidad de suscriptores para shard channels.
    ///
    /// # Retorno
    ///
    /// Devuelve `Result<DatoRedis, DatoRedis>`:
    /// - `Ok(DatoRedis)` con la respuesta en formato RESP.
    /// - `Err(DatoRedis)` con un mensaje de error RESP si el subcomando es inválido
    ///   o la cantidad de argumentos es incorrecta.
    pub(crate) fn pub_sub(
        &mut self,
        tokens: &[String],
        _client: Arc<RwLock<Client>>,
    ) -> Result<DatoRedis, DatoRedis> {
        assert_correct_arguments_quantity(tokens[0].clone(), 1, tokens.len())?;

        match tokens[1].to_uppercase().as_str() {
            CMD_PUBSUB_NUMSUB => self.pubsub_numsub(tokens),
            CMD_PUBSUB_CHANNELS => self.pubsub_channels(tokens),
            CMD_PUBSUB_NUMPAT => self.pubsub_numpat(),
            CMD_PUBSUB_SHARDCHANNELS => self.pubsub_schannels(tokens),
            CMD_PUBSUB_SHARDNUMSUB => self.pubsub_snumsub(tokens),
            _ => Err(DatoRedis::new_simple_error(
                "ERR".into(),
                "unknown subcommand".into(),
            )),
        }
    }

    /// Obtiene el número de suscriptores para canales dados o todos.
    ///
    /// # Parámetros
    /// - `canales`: Mapa de canales a suscriptores.
    /// - `canales_consultar`: Opcional lista de nombres de canales para consultar.
    ///
    /// # Retorno
    /// - `Ok(DatoRedis)` con array RESP `[canal, cantidad, canal, cantidad, ...]`.
    fn obtener_numsub(
        canales: &Channels,
        canales_consultar: Option<&[String]>,
    ) -> Result<DatoRedis, DatoRedis> {
        let mut resultado = Arrays::new();

        if let Some(lista) = canales_consultar {
            for canal in lista {
                resultado.append(DatoRedis::new_bulk_string(canal.clone())?);
                let cantidad = canales.get(canal).map(|m| m.len()).unwrap_or(0);
                resultado.append(DatoRedis::new_integer(cantidad as i64));
            }
        } else {
            for (canal, mapa) in canales.iter() {
                resultado.append(DatoRedis::new_bulk_string(canal.clone())?);
                resultado.append(DatoRedis::new_integer(mapa.len() as i64));
            }
        }

        Ok(DatoRedis::new_array_con_contenido(resultado))
    }

    /// Implementa el subcomando `PUBSUB NUMSUB [canal ...]` para canales normales.
    fn pubsub_numsub(&self, tokens: &[String]) -> Result<DatoRedis, DatoRedis> {
        let canales_consultar = if tokens.len() > 2 {
            Some(&tokens[2..])
        } else {
            None
        };
        Self::obtener_numsub(&self.channels, canales_consultar)
    }

    /// Implementa el subcomando `PUBSUB SNUMSUB [canal ...]` para shard channels.
    fn pubsub_snumsub(&self, tokens: &[String]) -> Result<DatoRedis, DatoRedis> {
        let canales_consultar = if tokens.len() > 2 {
            Some(&tokens[2..])
        } else {
            None
        };
        Self::obtener_numsub(&self.schannels, canales_consultar)
    }

    /// Obtiene lista de canales con suscriptores que coincidan opcionalmente con un patrón.
    ///
    /// # Parámetros
    /// - `canales`: Mapa de canales a clientes suscriptos.
    /// - `patron_opt`: Patrón glob opcional para filtrar.
    ///
    /// # Retorna
    /// - `Ok(DatoRedis)` con array de nombres de canales.
    fn obtener_canales_con_suscriptores(
        canales: &Channels,
        patron_opt: Option<&str>,
    ) -> Result<DatoRedis, DatoRedis> {
        let mut resultado = Arrays::new();

        for (canal, clientes) in canales.iter() {
            if clientes.is_empty() {
                continue;
            }
            if patron_opt
                .map(|pat| matches_pattern(canal, pat))
                .unwrap_or(true)
            {
                resultado.append(DatoRedis::new_bulk_string(canal.clone())?);
            }
        }

        Ok(DatoRedis::new_array_con_contenido(resultado))
    }

    /// Implementa el subcomando `PUBSUB CHANNELS [pattern]` para canales normales.
    ///
    /// Devuelve los canales con suscriptores que coincidan opcionalmente con el patrón.
    fn pubsub_channels(&self, tokens: &[String]) -> Result<DatoRedis, DatoRedis> {
        let patron_opt = tokens.get(2).map(|s| s.as_str());
        Self::obtener_canales_con_suscriptores(&self.channels, patron_opt)
    }

    /// Implementa `PUBSUB SCHANNELS [pattern]` para shard channels.
    ///
    /// Devuelve shard channels con suscriptores que coincidan opcionalmente con el patrón.
    fn pubsub_schannels(&self, tokens: &[String]) -> Result<DatoRedis, DatoRedis> {
        let patron_opt = tokens.get(2).map(|s| s.as_str());
        Self::obtener_canales_con_suscriptores(&self.schannels, patron_opt)
    }

    /// Implementa `PUBSUB NUMPAT`.
    ///
    /// Devuelve cantidad de patrones activos con suscriptores.
    fn pubsub_numpat(&self) -> Result<DatoRedis, DatoRedis> {
        Ok(DatoRedis::new_integer(self.pchannels.len() as i64))
    }

    /// Verifica que todos los canales estén dentro del rango de slots.
    ///
    /// # Parámetros
    /// - `canal`: Nombre del canal.
    ///
    /// # Retorno
    /// - `(bool, u16)` indicando si el slot es válido y el valor del slot.
    fn validar_slots(&self, canal: &str) -> (bool, u16) {
        let slot = crc16(canal.as_bytes()) % TOTAL_SLOTS;
        (self.slot_range.contains(&slot), slot)
    }
}

/// Determina si el nombre de un canal cumple un determinado patron
///
/// # Parámetros
/// * `canal`: nombre del canal a analizar
/// * `patron`: patron con el que se desea comparar
///
/// # Retorna
/// - verdadero de cumplirse el patron, falso en otro caso
fn matches_pattern(canal: &str, patron: &str) -> bool {
    Pattern::new(patron)
        .map(|p| p.matches(canal))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_role::NodeRole;
    use logger::logger::Logger;
    use std::net::{TcpListener, TcpStream};
    use std::ops::Range;
    use std::sync::mpsc::{Receiver, Sender};
    use std::sync::{Arc, RwLock, mpsc::channel};

    fn dummy_tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();

        (client, server)
    }

    fn setup_cliente(nombre: &str) -> (Arc<RwLock<Client>>, Sender<String>, Receiver<String>) {
        // Crear un canal para la comunicación
        let (tx, rx) = channel::<String>();
        let looger = Logger::null();
        // Crear el cliente con el nombre proporcionado
        let (client_stream, _server_stream) = dummy_tcp_pair();
        let cliente = Client::new(
            nombre.to_string(),
            client_stream,
            looger,
            Arc::new(RwLock::new(NodeRole::Master)),
        );
        let cliente = Arc::new(RwLock::new(cliente));

        (cliente, tx, rx)
    }

    fn setup_pubsub_core(slot_range: Range<u16>) -> PubSubCore {
        let logger = Logger::null();
        PubSubCore::new(logger, slot_range)
    }

    #[test]
    fn test01_subscribe_agrega_canales_y_responde_ok() {
        let (_client_stream, _server_stream) = dummy_tcp_pair();
        let (cliente, _tx, _rx) = setup_cliente("cliente1");
        let mut pubsub = setup_pubsub_core(0..0);

        let tokens = vec![
            "SUBSCRIBE".to_string(),
            "canal1".to_string(),
            "canal2".to_string(),
        ];

        // 5. Ejecutar subscribe
        let result = pubsub.subscribe(&tokens, cliente.clone()).unwrap();

        // 6. Verificaciones
        let cliente_guard = cliente.read().unwrap();
        assert!(cliente_guard.get_channels().contains("canal1"));
        assert!(cliente_guard.get_channels().contains("canal2"));
        assert!(cliente_guard.get_modo_pub_sub());

        assert!(pubsub.channels.contains_key("canal1"));
        assert!(pubsub.channels.contains_key("canal2"));
        assert!(pubsub.channels["canal1"].contains_key("cliente1"));
        assert!(pubsub.channels["canal2"].contains_key("cliente1"));

        // 7. Verificar respuesta RESP
        let expected = "1) \"subscribe\"\r\n\
                               2) \"canal1\"\r\n\
                               3) (integer) 1\r\n\
                               4) \"subscribe\"\r\n\
                               5) \"canal2\"\r\n\
                               6) (integer) 2\r\n"
            .to_string();

        assert_eq!(expected, result.convertir_resp_a_string());
    }

    #[test]
    fn test02_subscribe_sin_agrega_canales_responde_err() {
        let (_client_stream, _server_stream) = dummy_tcp_pair();
        let (cliente, _tx, _rx) = setup_cliente("cliente1");
        let mut pubsub = setup_pubsub_core(0..0);

        let tokens = vec!["SUBSCRIBE".to_string()];

        assert!(pubsub.subscribe(&tokens, cliente.clone()).is_err());
    }

    #[test]
    fn test03_publish_envia_mensaje_a_suscriptores() {
        let (_client_stream, _server_stream) = dummy_tcp_pair();
        let (cliente, tx, rx) = setup_cliente("cliente1");
        let mut pubsub = setup_pubsub_core(0..0);

        pubsub
            .channels
            .entry("canal1".into())
            .or_default()
            .insert("cliente1".into(), tx.clone());

        let tokens = vec![
            "PUBLISH".to_string(),
            "canal1".to_string(),
            "¡Hola mundo!".to_string(),
        ];

        let result = pubsub.publish(&tokens, cliente.clone()).unwrap();
        assert_eq!(result, DatoRedis::new_integer(1));
        assert_eq!(
            rx.try_recv().unwrap(),
            "*3\r\n$7\r\nmessage\r\n$6\r\ncanal1\r\n$13\r\n¡Hola mundo!\r\n"
        );
    }

    #[test]
    fn test04_publish_envia_mensaje_sin_subscribes() {
        let (_client_stream, _server_stream) = dummy_tcp_pair();
        let (cliente, _tx, rx) = setup_cliente("cliente1");
        let mut pubsub = setup_pubsub_core(0..0);

        let tokens = vec![
            "PUBLISH".to_string(),
            "canal1".to_string(),
            "¡Hola mundo!".to_string(),
        ];

        let result = pubsub.publish(&tokens, cliente.clone()).unwrap();
        assert_eq!(result, DatoRedis::new_integer(0));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test04b_publish_envia_mensaje_multiples_subscribes() {
        let (_c, _s) = dummy_tcp_pair();
        let (cliente1, tx1, rx1) = setup_cliente("cliente1");
        let (_cliente2, tx2, _rx2) = setup_cliente("cliente2");
        let mut pubsub = setup_pubsub_core(0..0);

        pubsub
            .channels
            .entry("canal1".into())
            .or_default()
            .insert("cliente1".into(), tx1.clone());
        pubsub
            .channels
            .get_mut("canal1")
            .unwrap()
            .insert("cliente2".into(), tx2.clone());

        let tokens = vec![
            "PUBLISH".to_string(),
            "canal1".to_string(),
            "¡Hola mundo!".to_string(),
        ];
        let result = pubsub.publish(&tokens, cliente1.clone()).unwrap();
        assert_eq!(result, DatoRedis::new_integer(2));
        assert_eq!(
            rx1.try_recv().unwrap(),
            "*3\r\n$7\r\nmessage\r\n$6\r\ncanal1\r\n$13\r\n¡Hola mundo!\r\n"
        );
    }

    #[test]
    fn test05_unsubscribe_de_un_canal() {
        let (_c, _s) = dummy_tcp_pair();
        let (cliente, tx, rx) = setup_cliente("cliente1");
        let mut pubsub = setup_pubsub_core(0..0);

        pubsub
            .channels
            .entry("canal1".into())
            .or_default()
            .insert("cliente1".into(), tx.clone());
        {
            let mut g = cliente.write().unwrap();
            g.add_channel("canal1".parse().unwrap());
        }

        let tokens = vec!["UNSUBSCRIBE".into(), "canal1".into()];
        let result = pubsub.unsubscribe(&tokens, cliente.clone()).unwrap();
        assert_eq!(
            result.convertir_a_protocolo_resp(),
            "*3\r\n$11\r\nunsubscribe\r\n$6\r\ncanal1\r\n:0\r\n"
        );
        assert!(!pubsub.channels.contains_key("canal1"));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test06_unsubscribe_de_varios_canales() {
        let (_c, _s) = dummy_tcp_pair();
        let (cliente, tx, rx) = setup_cliente("cliente1");
        let mut pubsub = setup_pubsub_core(0..0);

        for nombre in &["canal1", "canal2", "canal3"] {
            pubsub
                .channels
                .entry((*nombre).into())
                .or_default()
                .insert("cliente1".into(), tx.clone());
        }
        {
            let mut g = cliente.write().unwrap();
            for n in &["canal1", "canal2", "canal3"] {
                g.add_channel((*n).parse().unwrap());
            }
        }

        let tokens = vec!["UNSUBSCRIBE".into()];
        let result = pubsub.unsubscribe(&tokens, cliente.clone()).unwrap();
        let res = result.convertir_resp_a_string();
        assert!(res.contains("canal1"));
        assert!(res.contains("canal2"));
        assert!(res.contains("canal3"));
        for nombre in &["canal1", "canal2", "canal3"] {
            assert!(!pubsub.channels.contains_key(*nombre));
        }
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test07_unsubscribe_de_varios_canales_con_otros_clientes() {
        let (_c, _s) = dummy_tcp_pair();
        let (cliente, tx1, rx) = setup_cliente("cliente1");
        let (_c2, tx2, _rx2) = setup_cliente("cliente2");
        let mut pubsub = setup_pubsub_core(0..0);

        for nombre in &["canal1", "canal2", "canal3"] {
            pubsub
                .channels
                .entry((*nombre).into())
                .or_default()
                .insert("cliente1".into(), tx1.clone());
            pubsub
                .channels
                .get_mut(*nombre)
                .unwrap()
                .insert("cliente2".into(), tx2.clone());
        }
        {
            let mut g = cliente.write().unwrap();
            for n in &["canal1", "canal2", "canal3"] {
                g.add_channel((*n).parse().unwrap());
            }
        }

        let tokens = vec!["UNSUBSCRIBE".into(), "canal1".into()];
        let result = pubsub.unsubscribe(&tokens, cliente.clone()).unwrap();
        assert_eq!(
            result.convertir_a_protocolo_resp(),
            "*3\r\n$11\r\nunsubscribe\r\n$6\r\ncanal1\r\n:2\r\n"
        );
        assert!(!pubsub.channels["canal1"].contains_key("cliente1"));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test08_pubsub_numsub_con_varios_canales_y_clientes() {
        let (cliente1, tx1, _) = setup_cliente("cliente1");
        let (_c2, tx2, _) = setup_cliente("cliente2");
        let (_c3, tx3, _) = setup_cliente("cliente3");
        let mut pubsub = setup_pubsub_core(0..0);

        pubsub
            .channels
            .entry("canal1".into())
            .or_default()
            .insert("cliente1".into(), tx1.clone());
        pubsub
            .channels
            .get_mut("canal1")
            .unwrap()
            .insert("cliente2".into(), tx2.clone());
        pubsub
            .channels
            .entry("canal2".into())
            .or_default()
            .insert("cliente3".into(), tx3.clone());
        pubsub.channels.entry("canal3".into()).or_default();

        let tokens = vec![
            "PUBSUB".into(),
            "NUMSUB".into(),
            "canal1".into(),
            "canal2".into(),
            "canal3".into(),
            "canal_inexistente".into(),
        ];
        let result = pubsub.pub_sub(&tokens, cliente1.clone()).unwrap();
        let esperado = "*8\r\n\
        $6\r\ncanal1\r\n:2\r\n\
        $6\r\ncanal2\r\n:1\r\n\
        $6\r\ncanal3\r\n:0\r\n\
        $17\r\ncanal_inexistente\r\n:0\r\n";
        assert_eq!(result.convertir_a_protocolo_resp(), esperado);
    }

    #[test]
    fn test09_pubsub_channels_con_y_sin_patron() {
        let (cliente1, tx1, _) = setup_cliente("cliente1");
        let (_cliente2, tx2, _) = setup_cliente("cliente2");
        let mut pubsub = setup_pubsub_core(0..0);

        pubsub
            .channels
            .entry("hello".into())
            .or_default()
            .insert("cliente1".into(), tx1.clone());
        pubsub
            .channels
            .entry("hallo".into())
            .or_default()
            .insert("cliente2".into(), tx2.clone());
        for ch in &["hxllo", "heeeello", "hllo", "hillo"] {
            pubsub
                .channels
                .entry((*ch).into())
                .or_default()
                .insert("cliente1".into(), tx1.clone());
        }

        let tokens = vec!["PUBSUB".into(), "CHANNELS".into()];
        let resp = pubsub
            .pub_sub(&tokens, cliente1.clone())
            .unwrap()
            .convertir_a_protocolo_resp();
        assert!(resp.contains("hello"));
        assert!(resp.contains("hallo"));
        assert!(resp.contains("hxllo"));
        assert!(resp.contains("heeeello"));
        assert!(resp.contains("hllo"));
        assert!(resp.contains("hillo"));

        let tokens = vec!["PUBSUB".into(), "CHANNELS".into(), "h?llo".into()];
        let resp = pubsub
            .pub_sub(&tokens, cliente1.clone())
            .unwrap()
            .convertir_a_protocolo_resp();
        assert!(resp.contains("hello"));
        assert!(resp.contains("hallo"));
        assert!(resp.contains("hxllo"));
        assert!(resp.contains("hillo"));
        assert!(!resp.contains("hllo"));
        assert!(!resp.contains("heeeello"));

        let tokens = vec!["PUBSUB".into(), "CHANNELS".into(), "h*llo".into()];
        let resp = pubsub
            .pub_sub(&tokens, cliente1.clone())
            .unwrap()
            .convertir_a_protocolo_resp();
        assert!(resp.contains("hllo"));
        assert!(resp.contains("heeeello"));
        assert!(resp.contains("hxllo"));
        assert!(resp.contains("hallo"));
        assert!(resp.contains("hillo"));
        assert!(resp.contains("hello"));

        let tokens = vec!["PUBSUB".into(), "CHANNELS".into(), "h[ae]llo".into()];
        let resp = pubsub
            .pub_sub(&tokens, cliente1.clone())
            .unwrap()
            .convertir_a_protocolo_resp();
        assert!(resp.contains("hello"));
        assert!(resp.contains("hallo"));
        assert!(!resp.contains("heeeello"));
        assert!(!resp.contains("hxllo"));
        assert!(!resp.contains("hillo"));
        assert!(!resp.contains("hllo"));
    }

    #[test]
    fn test09_psubscribe_sin_canal() {
        let (_cs, _ss) = dummy_tcp_pair();
        let (cliente, _tx, _rx) = setup_cliente("cliente1");
        let mut pubsub = setup_pubsub_core(0..0);

        let tokens = vec!["PSUBSCRIBE".into()];
        let result = pubsub.subscribe(&tokens, cliente.clone());

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().convertir_resp_a_string(),
            "(error) ERR wrong number of arguments for 'PSUBSCRIBE' command\r\n"
        );
    }

    #[test]
    fn test10_psubscribe_con_canal() {
        let (_cs, _ss) = dummy_tcp_pair();
        let (cliente, _tx, _rx) = setup_cliente("cliente1");
        let mut pubsub = setup_pubsub_core(0..0);

        let tokens = vec!["PSUBSCRIBE".into(), "canal prueba*".into()];
        let result = pubsub.subscribe(&tokens, cliente.clone());

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().convertir_resp_a_string(),
            "1) \"psubscribe\"\r\n2) \"canal prueba*\"\r\n3) (integer) 1\r\n"
        );
        assert!(pubsub.pchannels.contains_key("canal prueba*"));
        assert!(pubsub.pchannels["canal prueba*"].contains_key("cliente1"));
        assert!(pubsub.channels.is_empty());
    }

    #[test]
    fn test11_psubscribe_con_varios_canal() {
        let (_cs, _ss) = dummy_tcp_pair();
        let (cliente, _tx, _rx) = setup_cliente("cliente1");
        let mut pubsub = setup_pubsub_core(0..0);

        let tokens = vec![
            "PSUBSCRIBE".into(),
            "canal prueba*".into(),
            "otro canal*".into(),
        ];
        let result = pubsub.subscribe(&tokens, cliente.clone());

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().convertir_resp_a_string(),
            "1) \"psubscribe\"\r\n2) \"canal prueba*\"\r\n3) (integer) 1\r\n\
4) \"psubscribe\"\r\n5) \"otro canal*\"\r\n6) (integer) 2\r\n"
        );
        assert!(pubsub.pchannels["canal prueba*"].contains_key("cliente1"));
        assert!(pubsub.pchannels["otro canal*"].contains_key("cliente1"));
        assert!(pubsub.channels.is_empty());
    }

    #[test]
    fn test12_punsubscribe_con_canal() {
        let (_cs, _ss) = dummy_tcp_pair();
        let (cliente, _tx, _rx) = setup_cliente("cliente1");
        let mut pubsub = setup_pubsub_core(0..0);

        let tokens = vec![
            "PSUBSCRIBE".into(),
            "canal prueba*".into(),
            "otro canal*".into(),
        ];
        let _ = pubsub.subscribe(&tokens, cliente.clone());

        let tokens = vec!["PUNSUBSCRIBE".into(), "canal prueba*".into()];
        let result = pubsub.unsubscribe(&tokens, cliente.clone());

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().convertir_resp_a_string(),
            "1) \"punsubscribe\"\r\n2) \"canal prueba*\"\r\n3) (integer) 1\r\n"
        );
        assert!(!pubsub.pchannels.contains_key("canal prueba*"));
        assert!(pubsub.pchannels["otro canal*"].contains_key("cliente1"));
        assert!(pubsub.channels.is_empty());
    }

    #[test]
    fn test13_publish_con_pcanales() {
        let (_cs, _ss) = dummy_tcp_pair();
        let (cliente, _, _) = setup_cliente("cliente1");
        let mut core = setup_pubsub_core(0..0);

        let _ = core.subscribe(&["PSUBSCRIBE".into(), "canal*".into()], cliente.clone());

        let resp = core.publish(
            &["PUBLISH".into(), "canal123".into(), "Hola!".into()],
            cliente.clone(),
        );

        assert!(resp.is_ok());
        assert_eq!(resp.unwrap().convertir_resp_a_string(), "(integer) 1\r\n");
    }

    #[test]
    fn test14_pubsub_numpat() {
        let (_cs, _ss) = dummy_tcp_pair();
        let (cliente, _, _) = setup_cliente("cliente1");
        let mut core = setup_pubsub_core(0..0);

        let _ = core.subscribe(
            &["PSUBSCRIBE".into(), "canal*".into(), "otro*".into()],
            cliente.clone(),
        );

        let resp = core.pub_sub(&["PUBSUB".into(), "NUMPAT".into()], cliente.clone());

        assert!(resp.is_ok());
        assert_eq!(resp.unwrap().convertir_resp_a_string(), "(integer) 2\r\n");
    }

    #[test]
    fn test15_punsubscribe_sin_argumentos() {
        let (_cs, _ss) = dummy_tcp_pair();
        let (cliente, _, _) = setup_cliente("cliente1");
        let mut core = setup_pubsub_core(0..0);

        let _ = core.subscribe(
            &["PSUBSCRIBE".into(), "canal*".into(), "otro*".into()],
            cliente.clone(),
        );

        let resp = core.unsubscribe(&["PUNSUBSCRIBE".into()], cliente.clone());

        assert!(resp.is_ok());
        let str_resp = resp.unwrap().convertir_resp_a_string();
        assert!(str_resp.contains("punsubscribe"));
        assert!(str_resp.contains("canal*"));
        assert!(str_resp.contains("otro*"));
        assert!(str_resp.contains("(integer) 1"));
        assert!(str_resp.contains("(integer) 0"));
    }

    #[test]
    fn test16_pubsub_channels_con_pcanales_y_canales() {
        let (_cs, _ss) = dummy_tcp_pair();
        let (cliente, _, _) = setup_cliente("cliente1");
        let mut core = setup_pubsub_core(0..0);

        let _ = core.subscribe(&["SUBSCRIBE".into(), "canal1".into()], cliente.clone());
        let _ = core.subscribe(&["PSUBSCRIBE".into(), "news*".into()], cliente.clone());

        let resp = core.pub_sub(&["PUBSUB".into(), "CHANNELS".into()], cliente.clone());

        assert!(resp.is_ok());
        let out = resp.unwrap().convertir_resp_a_string();
        assert!(out.contains("\"canal1\""));
        assert!(!out.contains("\"news*\""));
    }

    #[test]
    fn test17_publish_multiples_match_de_pcanales() {
        let (_c1, _s1) = dummy_tcp_pair();
        let (_c2, _s2) = dummy_tcp_pair();
        let (cliente1, _, _) = setup_cliente("cliente1");
        let (cliente2, _, _) = setup_cliente("cliente2");
        let mut core = setup_pubsub_core(0..0);

        let _ = core.subscribe(&["PSUBSCRIBE".into(), "chat.*".into()], cliente1.clone());
        let _ = core.subscribe(
            &["PSUBSCRIBE".into(), "chat.general".into()],
            cliente2.clone(),
        );

        let resp = core.publish(
            &["PUBLISH".into(), "chat.general".into(), "mensaje".into()],
            cliente1.clone(),
        );

        assert!(resp.is_ok());
        assert_eq!(resp.unwrap().convertir_resp_a_string(), "(integer) 2\r\n");
    }

    #[test]
    fn test18_ssubscribe_agrega_canales_y_responde_ok() {
        let (_c, _s) = dummy_tcp_pair();
        let (cliente, _tx, _rx) = setup_cliente("cliente1");
        let mut core = setup_pubsub_core(0..16384);

        let resp = core
            .subscribe(
                &["SSUBSCRIBE".into(), "sch1".into(), "sch2".into()],
                cliente.clone(),
            )
            .unwrap();

        assert!(core.schannels.contains_key("sch1"));
        assert!(core.schannels.contains_key("sch2"));
        assert_eq!(
            resp.convertir_resp_a_string(),
            "1) \"ssubscribe\"\r\n2) \"sch1\"\r\n3) (integer) 1\r\n\
4) \"ssubscribe\"\r\n5) \"sch2\"\r\n6) (integer) 2\r\n"
        );
    }

    #[test]
    fn test19_ssubscribe_sin_canal_responde_err() {
        let (_c, _s) = dummy_tcp_pair();
        let (cliente, _tx, _rx) = setup_cliente("cliente1");
        let mut core = setup_pubsub_core(0..0);

        let err = core
            .subscribe(&["SSUBSCRIBE".into()], cliente.clone())
            .unwrap_err();
        assert_eq!(
            err.convertir_resp_a_string(),
            "(error) ERR wrong number of arguments for 'SSUBSCRIBE' command\r\n"
        );
    }

    #[test]
    fn test20_spublish_envia_mensaje_a_suscriptores() {
        let (_c, _s) = dummy_tcp_pair();
        let (cliente, tx, rx) = setup_cliente("cliente1");
        let mut core = setup_pubsub_core(0..16384);

        core.schannels
            .entry("sch1".into())
            .or_default()
            .insert("cliente1".into(), tx.clone());

        let resp = core
            .publish(
                &["SPUBLISH".into(), "sch1".into(), "Hola!".into()],
                cliente.clone(),
            )
            .unwrap();

        assert_eq!(resp.convertir_resp_a_string(), "(integer) 1\r\n");
        assert_eq!(
            rx.try_recv().unwrap(),
            "*3\r\n$8\r\nsmessage\r\n$4\r\nsch1\r\n$5\r\nHola!\r\n"
        );
    }

    #[test]
    fn test21_sunsubscribe_de_un_canal() {
        let (_c, _s) = dummy_tcp_pair();
        let (cliente, tx, _rx) = setup_cliente("cliente1");
        let mut core = setup_pubsub_core(0..16384);

        core.schannels
            .entry("sch1".into())
            .or_default()
            .insert("cliente1".into(), tx.clone());

        let _ = core.subscribe(&["SSUBSCRIBE".into(), "sch1".into()], cliente.clone());

        let resp = core
            .unsubscribe(&["SUNSUBSCRIBE".into(), "sch1".into()], cliente.clone())
            .unwrap();

        assert_eq!(
            resp.convertir_resp_a_string(),
            "1) \"sunsubscribe\"\r\n2) \"sch1\"\r\n3) (integer) 0\r\n"
        );
        assert!(core.schannels.is_empty());
    }

    #[test]
    fn test22_pubsub_shardnumsub() {
        let (cli1, tx1, _) = setup_cliente("cli1");
        let (_cli2, tx2, _) = setup_cliente("cli2");
        let mut core = setup_pubsub_core(0..16384);

        core.schannels
            .entry("sch1".into())
            .or_default()
            .insert("cli1".into(), tx1.clone());
        core.schannels
            .get_mut("sch1")
            .unwrap()
            .insert("cli2".into(), tx2.clone());
        core.schannels.entry("sch2".into()).or_default();

        let resp = core
            .pub_sub(
                &[
                    "PUBSUB".into(),
                    "SHARDNUMSUB".into(),
                    "sch1".into(),
                    "sch2".into(),
                    "sch3".into(),
                ],
                cli1.clone(),
            )
            .unwrap();

        assert_eq!(
            resp.convertir_a_protocolo_resp(),
            "*6\r\n$4\r\nsch1\r\n:2\r\n$4\r\nsch2\r\n:0\r\n$4\r\nsch3\r\n:0\r\n"
        );
    }

    #[test]
    fn test23_pubsub_shardchannels_con_y_sin_patron() {
        let (cli1, tx1, _) = setup_cliente("cli1");
        let mut core = setup_pubsub_core(0..0);

        for c in &["alpha", "aleph", "beta", "abacus", "gamma"] {
            core.schannels
                .entry((*c).into())
                .or_default()
                .insert("cli1".into(), tx1.clone());
        }

        let all = core
            .pub_sub(&["PUBSUB".into(), "SHARDCHANNELS".into()], cli1.clone())
            .unwrap()
            .convertir_resp_a_string();
        assert!(all.contains("alpha"));
        assert!(all.contains("aleph"));
        assert!(all.contains("beta"));
        assert!(all.contains("abacus"));
        assert!(all.contains("gamma"));

        let pat = core
            .pub_sub(
                &["PUBSUB".into(), "SHARDCHANNELS".into(), "a*".into()],
                cli1.clone(),
            )
            .unwrap()
            .convertir_resp_a_string();
        assert!(pat.contains("alpha"));
        assert!(pat.contains("aleph"));
        assert!(pat.contains("abacus"));
        assert!(!pat.contains("beta"));
        assert!(!pat.contains("gamma"));
    }
}
