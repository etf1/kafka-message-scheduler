#!/usr/bin/env bash

set -euo pipefail

function wait_for_kafka(){
    local server="$1"

    for i in {1..10}
    do
        echo "Waiting for kafka cluster to be ready ..."
        # kafkacat has 5s timeout
        kafkacat -b "${server}" -L > /dev/null 2>&1 && break
    done
}

wait_for_kafka "kafka:29092"

make tests