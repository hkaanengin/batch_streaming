FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Detect architecture and download appropriate DuckDB version
RUN arch=$(uname -m) && \
    case $arch in \
        x86_64) arch_name="amd64" ;; \
        aarch64) arch_name="aarch64" ;; \
        *) echo "Unsupported architecture: $arch" && exit 1 ;; \
    esac && \
    wget https://github.com/duckdb/duckdb/releases/download/v0.9.2/duckdb_cli-linux-${arch_name}.zip && \
    unzip duckdb_cli-linux-${arch_name}.zip && \
    chmod +x duckdb && \
    mv duckdb /usr/local/bin/

RUN pip install --no-cache-dir duckdb

WORKDIR /data

CMD ["/bin/bash"]