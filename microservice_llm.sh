#!/bin/bash
API_KEY="Poner la API Key de gemini aquí"

cargo run --bin microservice_llm -- host=127.0.0.1 port=8088 api_key="$API_KEY"
