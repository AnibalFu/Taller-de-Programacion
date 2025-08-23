# Documents Handler Service


## MS: Incoming Events

Son los eventos que el microservicio o otros clientes pueden escuchar.
Los eventos que son solo del microservicio, se marcarán con un "*".

### *Utilidades de documentos
Este canal se encargará de recibir peticiones para crear un nuevo documento.

- Channel:
    documents:utils
- MsgContent:
    -MsgEvent:
        action:create
        document_name: String
        user_id: String
    -MsgEvent:
        action:list
        user_id: String
        file_type: documents | sheets

### Interacción con los documentos

- Channel:
    documents:<document_id>
- MsgContent: El contenido de estos eventos puede variar dependiendo de que acción se realiza  
    - action: join | edition | disconnect
    - user_id: String
    - op: insert | delete
    - start_position: usize
    - end_position: usize

En base al campo action, recibiremos los siguientes formatos de evento:

- action: join; indica que un usuario se está conectando para editar un documento
    - user_id: String

- action: edition; puede recibir una lista de operaciones separadas por el operador |. 
    - op: insert 
        - content: String
        - position: usize 
    - op: delete
        - start_position: usize
        - end_position: usize

- action: disconnect; indica que un usuario en particular desconectó de la edición del documento
    - user_id: String

--- 

## MS: Brod Events

Son los eventos que el microservicio va a publicar

### Canal personal del usuario

Este canal es personal de cada usuario, donde pueden recibir distintos tipos de eventos en respuesta a sus peticiones.

- Channel:
    users:user_id


- MsgContent:  
    - MsgEvent: Evento de respuesta ante la creación de un documento  
        - event: document_created
        - document_id: usize

## Exposed Data

Datos que el microservicio expone

### Claves 
- KEY: ms:documents
- CONTENT: lista de documentos creados hasta el momento en el formato document_name:id
