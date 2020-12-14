#!/usr/bin/env bash

set -euo pipefail

readonly TESTS_RESULT=$1
readonly RUN_COUNT=$(grep "RUN" "${TESTS_RESULT}" | wc -l | awk '{print $1}')
readonly FAIL_COUNT=$(grep "FAIL" "${TESTS_RESULT}" | wc -l | awk '{print $1}')

if [ "${RUN_COUNT}" -gt 0 ] && [ "${FAIL_COUNT}" -eq 0 ]; then
    echo "Test Passed !"
    exit 0
fi

echo "Test Failed !!"
exit 1