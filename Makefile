.PHONY: run_client, run_node, clippy, test, up, down, build


run_client:
	cargo run --bin cliente -- 127.0.0.1:8089

run_node1:
	cargo run --bin redis_node -- redis_node/src/redis1.conf

run_node2:
	cargo run --bin redis_node -- redis_node/src/redis2.conf

run_node3:
	cargo run --bin redis_node -- redis_node/src/redis3.conf

run_ui:
	USER=ui \
	PASSWORD=test \
	REDIS_PORT=8088 \
	REDIS_HOST=127.0.0.1 cargo run --bin ui_client

run_microservices:
	cargo run --bin documents-handler-service -- redis_address=127.0.0.1:8088

clippy:
	cargo clippy --all-targets --all-features

test:
	cargo test --all-targets --all-features && \
	cargo test -- --ignored --test-threads=1

setup_cluster:
	gnome-terminal -- bash -c "cargo run --bin redis_node -- configs/redis_01.conf" && \
	gnome-terminal -- bash -c "cargo run --bin redis_node -- configs/redis_02.conf" && \
	gnome-terminal -- bash -c "cargo run --bin redis_node -- configs/redis_03.conf"

setup_client:
	gnome-terminal -- bash -c "cargo run --bin cliente -- 8088" && \
	gnome-terminal -- bash -c "cargo run --bin cliente -- 8089" && \
	gnome-terminal -- bash -c "cargo run --bin cliente -- 8090" && \
	gnome-terminal -- bash -c "cargo run --bin cliente -- 8091" && \
	gnome-terminal -- bash -c "cargo run --bin cliente -- 8092" && \
	gnome-terminal -- bash -c "cargo run --bin cliente -- 8093" && \
	gnome-terminal -- bash -c "cargo run --bin cliente -- 8094" && \
	gnome-terminal -- bash -c "cargo run --bin cliente -- 8095" && \
	gnome-terminal -- bash -c "cargo run --bin cliente -- 8096"



run_llm:
	./microservice_llm.sh

run_docker_cluster:
	./cluster.sh

kill_docker_cluster:
	docker stop $$(docker ps -a -q) && docker-compose -f docker-compose.redis.yaml down

run_docker_single_node:
	./node.sh $(NODE_ID) $(NODE_PORT) $(CLUSTER_PORT)

build_docker_image:
	docker build -t rusty_docs:latest .

run_docker_microservices:
	docker compose -f docker-compose.microservices.yaml up --build
