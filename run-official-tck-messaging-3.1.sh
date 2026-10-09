#!/usr/bin/env bash
# shellcheck shell=bash
#
# Runs the official Jakarta Messaging TCK (jakarta.tck:jms-tck, JUnit 5,
# "Jakarta Messaging Outside container") against Tassis.
#
# Modes:
#   ./run-official-tck-messaging-3.1.sh                      # the tests Tassis is expected to pass (tck.test in tassis-tck/pom.xml)
#   ./run-official-tck-messaging-3.1.sh -Dtck.test='...'     # another selection, e.g. '**/core/selectorQueue/*IT'
#
# Behaviour:
#   1. Installs tassis-api and tassis-core locally (./mvnw install -DskipTests)
#   2. Runs ./mvnw -Ptck -pl tassis-tck test [args...]
#      (tassis-tck is in the reactor, enabled by the `tck` Maven profile)
#   3. Writes tassis-tck/target/tck-report.txt with the test counts and PASS/FAIL
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TCK_DIR="${ROOT_DIR}/tassis-tck"
REPORT_FILE="${TCK_DIR}/target/tck-report.txt"

echo "==> Step 1/2: local install of the Tassis artifacts (./mvnw install -DskipTests)"
( cd "${ROOT_DIR}" && ./mvnw -ntp -pl tassis-api,tassis-core -am install -DskipTests )

echo "==> Step 2/2: Maven run on the in-reactor tassis-tck (profile=tck)"
mkdir -p "${TCK_DIR}/target"

set +e
( cd "${ROOT_DIR}" && ./mvnw -ntp -Ptck -pl tassis-tck test "$@" ) | tee "${REPORT_FILE}.raw"
status=${PIPESTATUS[0]}
set -e

echo "==> Writing the report: ${REPORT_FILE}"
{
    echo "# Tassis TCK report"
    echo "# Generated $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "# Args    : $*"
    echo
    grep -E "Tests run:" "${REPORT_FILE}.raw" || true
    echo
    if [ "${status}" -eq 0 ]; then
        echo "RESULT : PASS"
    else
        echo "RESULT : FAIL (exit ${status})"
    fi
} > "${REPORT_FILE}"

cat "${REPORT_FILE}"
exit "${status}"
