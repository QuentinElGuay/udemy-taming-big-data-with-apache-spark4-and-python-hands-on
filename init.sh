#!/usr/bin/env bash

set -euo pipefail

readonly DEFAULT_SPARK_VERSION="4.1.2"
readonly HADOOP_PROFILE="hadoop3"
readonly DEFAULT_INSTALL_DIR="/opt/spark"

SPARK_VERSION="$DEFAULT_SPARK_VERSION"
INSTALL_DIR="$DEFAULT_INSTALL_DIR"

usage() {
    cat <<EOF
Usage: ./init.sh [OPTIONS]

Options:
  --spark-version VERSION   Spark version to install (default: ${DEFAULT_SPARK_VERSION})
  --install-dir DIR         Spark installation directory (default: ${DEFAULT_INSTALL_DIR})
  -h, --help                Show this help message

Examples:
  ./init.sh
  ./init.sh --spark-version 4.2.0
  ./init.sh --install-dir \$HOME/.local/spark
EOF
}

error() {
    echo "ERROR: $1" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --spark-version)
            [[ $# -ge 2 ]] || error "Missing value for --spark-version"
            SPARK_VERSION="$2"
            shift 2
            ;;
        --install-dir)
            [[ $# -ge 2 ]] || error "Missing value for --install-dir"
            INSTALL_DIR="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            error "Unknown option '$1'"
            ;;
    esac
done

for cmd in curl tar python3 pip3 java; do
    command -v "$cmd" >/dev/null || error "'$cmd' is required but was not found."
done

if command -v sha512sum >/dev/null; then
    CHECKSUM_CMD=(sha512sum)
elif command -v shasum >/dev/null; then
    CHECKSUM_CMD=(shasum -a 512)
else
    error "Neither 'sha512sum' nor 'shasum' is installed."
fi

JAVA_VERSION=$(java -version 2>&1 | head -n1)

if ! grep -Eq '"(17|21)\.' <<<"$JAVA_VERSION"; then
    error "Spark 4 requires Java 17 or 21.
Detected: $JAVA_VERSION"
fi

echo "Using $JAVA_VERSION"

PARENT_DIR=$(dirname "$INSTALL_DIR")
[[ -w "$PARENT_DIR" ]] || error "Cannot write to '$PARENT_DIR'. Try another directory or run with sudo."

ARCHIVE="spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz"
BASE_URL="https://downloads.apache.org/spark/spark-${SPARK_VERSION}"

TMP_DIR=$(mktemp -d)

cleanup() {
    rm -rf "$TMP_DIR"
}

trap cleanup EXIT

cd "$TMP_DIR"

echo "Checking Spark ${SPARK_VERSION}..."

curl --silent --fail --head "${BASE_URL}/${ARCHIVE}" >/dev/null \
    || error "Spark version ${SPARK_VERSION} does not exist."

if [[ -d "$INSTALL_DIR" ]]; then
    echo "Spark already installed in ${INSTALL_DIR}. Skipping installation."
else
    echo "Downloading Spark ${SPARK_VERSION}..."

    curl -fLO "${BASE_URL}/${ARCHIVE}"
    curl -fLO "${BASE_URL}/${ARCHIVE}.sha512"

    echo "Verifying checksum..."
    "${CHECKSUM_CMD[@]}" -c "${ARCHIVE}.sha512"

    echo "Extracting..."
    tar -xf "$ARCHIVE"

    mv "spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}" "$INSTALL_DIR"

    echo "Spark installed."
fi

LOG4J_CONFIG="${INSTALL_DIR}/conf/log4j2.properties"

if [[ ! -f "$LOG4J_CONFIG" ]]; then
    echo "Configuring Spark logging..."

    cp "${INSTALL_DIR}/conf/log4j2.properties.template" "$LOG4J_CONFIG"

    sed -i 's/rootLogger.level = .*/rootLogger.level = error/' "$LOG4J_CONFIG"
fi

if [[ ! -f env.sh ]]; then
    cat > env.sh <<EOF
export SPARK_HOME=${INSTALL_DIR}
export PATH=\$PATH:\$SPARK_HOME/bin
export PYTHONPATH=\$SPARK_HOME/python:\$PYTHONPATH
export PYSPARK_PYTHON=python3
EOF

    echo "Created env.sh"
else
    echo "env.sh already exists. Skipping."
fi

if ! command -v uv >/dev/null; then
    echo "Installing uv..."
    python3 -m pip install --upgrade uv
fi

if [[ ! -f pyproject.toml ]]; then
    echo "Initializing project..."
    uv init
fi

echo "Installing Python dependencies..."
uv add py4j pandas pyarrow

cat <<EOF

Done!

Next steps:

    source env.sh

Verify the installation:

    spark-submit --version
    pyspark

EOF
