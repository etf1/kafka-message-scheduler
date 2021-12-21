#!/usr/bin/env bash

set -euo pipefail

function wait_for_kafka(){
    local server="$1"

    echo "Waiting for kafka cluster to be ready ..."
    kcat -m 120 -b "${server}" -L
}

wait_for_kafka "kafka:29092"

make tests
