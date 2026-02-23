#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

AGENT_DIR="${SCRIPT_DIR}"
MEET_DIR="${MEET_DIR:-${WORKSPACE_ROOT}/meet}"
CLIENT_SDK_DIR="${CLIENT_SDK_DIR:-${WORKSPACE_ROOT}/client-sdk-js}"

LIVEKIT_HTTP_URL="${LIVEKIT_HTTP_URL:-http://127.0.0.1:7880}"
LIVEKIT_WS_URL="${LIVEKIT_WS_URL:-ws://127.0.0.1:7880}"
LIVEKIT_API_KEY="${LIVEKIT_API_KEY:-devkey}"
LIVEKIT_API_SECRET="${LIVEKIT_API_SECRET:-secret}"

MEET_PORT="${MEET_PORT:-3000}"
MEET_BASE_URL="${MEET_BASE_URL:-http://127.0.0.1:${MEET_PORT}}"

AGENT_NAME="${AGENT_NAME:-publisher-hls-meet-av1-e2ee-agent}"
ROOM_NAME="${ROOM_NAME:-meet-av1-e2ee-$(date +%s)}"
PARTICIPANT_IDENTITY="${PARTICIPANT_IDENTITY:-meet-user-$(date +%s)}"
PARTICIPANT_NAME="${PARTICIPANT_NAME:-meet-user}"
E2EE_PASSPHRASE="${E2EE_PASSPHRASE:-test-e2ee-secret-123}"
CODEC="${CODEC:-av1}"

SKIP_SDK_LINK="${SKIP_SDK_LINK:-0}"
OPEN_BROWSER="${OPEN_BROWSER:-0}"
LEAVE_RUNNING="${LEAVE_RUNNING:-0}"

RUN_ID="$(date +%Y%m%d-%H%M%S)"
LOG_DIR="${LOG_DIR:-${AGENT_DIR}/tmp/meet-av1-e2ee-${RUN_ID}}"
OUTPUT_DIR="${OUTPUT_DIR:-${AGENT_DIR}/hls-agent-recordings-meet-av1-e2ee}"

LIVEKIT_LOG="${LOG_DIR}/livekit-server.log"
AGENT_LOG="${LOG_DIR}/publisher-hls-agent.log"
MEET_LOG="${LOG_DIR}/meet.log"

PIDS=()

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

wait_for_http() {
  local url="$1"
  local timeout_secs="$2"
  local start
  start="$(date +%s)"
  while true; do
    if curl -sS --max-time 2 "$url" >/dev/null 2>&1; then
      return 0
    fi
    if (( "$(date +%s)" - start >= timeout_secs )); then
      return 1
    fi
    sleep 1
  done
}

wait_for_log_contains() {
  local file="$1"
  local pattern="$2"
  local timeout_secs="$3"
  local start
  start="$(date +%s)"
  while true; do
    if [[ -f "$file" ]] && rg -q "$pattern" "$file"; then
      return 0
    fi
    if (( "$(date +%s)" - start >= timeout_secs )); then
      return 1
    fi
    sleep 1
  done
}

start_bg() {
  local name="$1"
  shift
  "$@" &
  local pid=$!
  PIDS+=("$pid")
  echo "started ${name} (pid=${pid})"
}

cleanup() {
  local status=$?

  if [[ "$LEAVE_RUNNING" == "1" ]]; then
    echo
    echo "LEAVE_RUNNING=1 set, leaving processes running."
    echo "LiveKit log: ${LIVEKIT_LOG}"
    echo "Agent log:   ${AGENT_LOG}"
    echo "Meet log:    ${MEET_LOG}"
    return "$status"
  fi

  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done

  sleep 1

  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
  done

  if (( status != 0 )); then
    echo
    echo "script failed (exit=${status})"
    echo "LiveKit log: ${LIVEKIT_LOG}"
    echo "Agent log:   ${AGENT_LOG}"
    echo "Meet log:    ${MEET_LOG}"
  fi

  return "$status"
}

trap cleanup EXIT INT TERM

need_cmd go
need_cmd rg
need_cmd curl
need_cmd pnpm
need_cmd livekit-server

if [[ ! -d "$MEET_DIR" ]]; then
  echo "meet directory not found: ${MEET_DIR}" >&2
  exit 1
fi
if [[ ! -d "$CLIENT_SDK_DIR" ]]; then
  echo "client-sdk-js directory not found: ${CLIENT_SDK_DIR}" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"
mkdir -p "$OUTPUT_DIR"

echo "log dir:    ${LOG_DIR}"
echo "output dir: ${OUTPUT_DIR}"
echo

if [[ "$SKIP_SDK_LINK" != "1" ]]; then
  echo "building patched client-sdk-js from ${CLIENT_SDK_DIR} ..."
  (
    cd "$CLIENT_SDK_DIR"
    pnpm install
    pnpm build
  )

  echo "linking patched livekit-client into meet ..."
  (
    cd "$MEET_DIR"
    pnpm install
    pnpm add --save-exact "livekit-client@file:${CLIENT_SDK_DIR}"
  )
else
  echo "SKIP_SDK_LINK=1 set, skipping SDK build/link step"
fi

echo "starting livekit-server ..."
start_bg "livekit-server" bash -c \
  "cd '${REPO_ROOT}' && livekit-server --dev --config '${REPO_ROOT}/examples/livekit-server-dev.yaml' >'${LIVEKIT_LOG}' 2>&1"

if ! wait_for_http "${LIVEKIT_HTTP_URL}" 20; then
  echo "livekit-server did not become reachable at ${LIVEKIT_HTTP_URL}" >&2
  exit 1
fi

echo "starting publisher-hls-agent (${AGENT_NAME}) ..."
start_bg "publisher-hls-agent" bash -c \
  "cd '${AGENT_DIR}' && \
   LIVEKIT_URL='${LIVEKIT_WS_URL}' \
   LIVEKIT_API_KEY='${LIVEKIT_API_KEY}' \
   LIVEKIT_API_SECRET='${LIVEKIT_API_SECRET}' \
   AGENT_NAME='${AGENT_NAME}' \
   OUTPUT_DIR='${OUTPUT_DIR}' \
   AUTO_ACTIVATE_RECORDING='true' \
   E2EE_PASSPHRASE='${E2EE_PASSPHRASE}' \
   HLS_SEGMENT_DURATION='2' \
   HLS_MAX_SEGMENTS='0' \
   go run . >'${AGENT_LOG}' 2>&1"

if ! wait_for_log_contains "${AGENT_LOG}" "Worker registered" 45; then
  echo "agent did not register in time; check ${AGENT_LOG}" >&2
  exit 1
fi

echo "starting meet on port ${MEET_PORT} ..."
start_bg "meet" bash -c \
  "cd '${MEET_DIR}' && \
   LIVEKIT_API_KEY='${LIVEKIT_API_KEY}' \
   LIVEKIT_API_SECRET='${LIVEKIT_API_SECRET}' \
   LIVEKIT_URL='${LIVEKIT_WS_URL}' \
   pnpm dev --port '${MEET_PORT}' >'${MEET_LOG}' 2>&1"

if ! wait_for_http "${MEET_BASE_URL}" 60; then
  echo "meet did not become reachable at ${MEET_BASE_URL}; check ${MEET_LOG}" >&2
  exit 1
fi

helper_output="$(
  cd "$AGENT_DIR"
  go run ./cmd/meet-av1-e2ee-repro-helper \
    -livekit-http-url "${LIVEKIT_HTTP_URL}" \
    -livekit-ws-url "${LIVEKIT_WS_URL}" \
    -api-key "${LIVEKIT_API_KEY}" \
    -api-secret "${LIVEKIT_API_SECRET}" \
    -meet-base-url "${MEET_BASE_URL}" \
    -room "${ROOM_NAME}" \
    -agent-name "${AGENT_NAME}" \
    -participant-identity "${PARTICIPANT_IDENTITY}" \
    -participant-name "${PARTICIPANT_NAME}" \
    -codec "${CODEC}" \
    -passphrase "${E2EE_PASSPHRASE}"
)"

MEET_URL="$(printf '%s\n' "${helper_output}" | awk -F= '/^MEET_URL=/{sub(/^MEET_URL=/, "", $0); print; exit}')"

echo
echo "repro environment ready"
echo "-----------------------"
printf '%s\n' "${helper_output}"
echo
echo "next actions:"
echo "1. Open MEET_URL in a browser."
echo "2. Allow camera/microphone."
echo "3. Stay connected for ~20-30 seconds and publish video."
echo "4. Confirm recording artifacts under: ${OUTPUT_DIR}"
echo
echo "useful commands:"
echo "  tail -f '${AGENT_LOG}'"
echo "  tail -f '${LIVEKIT_LOG}'"
echo "  tail -f '${MEET_LOG}'"
echo "  find '${OUTPUT_DIR}' -name 'video.m3u8' -o -name 'audio.json'"
echo

if [[ "$OPEN_BROWSER" == "1" ]] && command -v open >/dev/null 2>&1; then
  open "${MEET_URL}"
fi

echo "waiting for recording output (Ctrl+C to stop all services) ..."
last_status_ts="$(date +%s)"
while true; do
  playlist="$(find "${OUTPUT_DIR}" -name "video.m3u8" -print -quit 2>/dev/null || true)"
  if [[ -n "${playlist}" ]]; then
    echo
    echo "recording detected: ${playlist}"
    echo "services are still running; press Ctrl+C when done."
    break
  fi

  now_ts="$(date +%s)"
  if (( now_ts - last_status_ts >= 15 )); then
    echo "still waiting for recording output ..."
    last_status_ts="${now_ts}"
  fi
  sleep 2
done

while true; do
  sleep 3600
done
