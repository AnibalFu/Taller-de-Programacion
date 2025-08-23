
#!/bin/bash
NODE_ID=$1
if [ -z "$NODE_ID" ]; then
    echo "Error: No se proporcionó el ID del nodo."
    exit 1
fi

NODE_PORT=$2
if [ -z "$NODE_PORT" ]; then
    echo "Error: No se proporcionó el puerto externo."
    exit 1
fi

CLUSTER_PORT=$3
if [ -z "$CLUSTER_PORT" ]; then
    echo "Error: No se pudo calcular el puerto de métricas."
    exit 1
fi  

gnome-terminal -- bash -c "
    echo 'Levantando redis_node_$NODE_ID en puerto $NODE_PORT...';
    NODE_ID=$NODE_ID \
    NODE_PORT=$NODE_PORT \
    CLUSTER_PORT=$CLUSTER_PORT \
    docker compose -f docker-compose.redis.yaml --project-name redis_node_0$NODE_ID up --build || true
    read -p "Presiona Enter para cerrar la terminal..."
    "
    