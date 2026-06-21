#!/bin/bash
# ============================================================================
# API Curl Examples
# ============================================================================

# ----------------------------------------------------------------------------
# Global Variables (Quickly change these to test different scenarios)
# ----------------------------------------------------------------------------
DOMAIN="sndbx.com"
API_URL="http://127.0.0.1:8000"
API_KEY="api_key_12_12_12_12"   # Traffic access token
SID="sb-286e38db492c4314"       # Sandbox ID
ENVD_TOKEN="dt2YTDDxSaPX5cijUJ2RMy8yDeUNRl89NCYQOvyX2W4" # env token (from sandbox metadata)
# Note: The data plane domain uses the format: <port>-<sandbox_id>.<domain>
HOST="49983-${SID}.sndbx.com"
ENVD_URL="https://49983-${SID}.sndbx.com"
TOKEN="ffL7un0kAAXM31OdYMLbWWmHnlmtieePzHzlDfomBXQ"
echo "Using API_URL: $API_URL"
echo "Using API_KEY: $API_KEY"
echo "Using SID: $SID"
echo "Using ENVD_TOKEN: $ENVD_TOKEN"
echo "Using ENVD_URL: $ENVD_URL"
echo "----------------------------------------------------------------------"

# ============================================================================
# 1. Template Registration
# ============================================================================

# 1a. Registering a standard template
# curl -s -X POST "$API_URL/templates" \
#   -H "X-API-Key: $API_KEY" \
#   -H "Content-Type: application/json" \
#   -d '{
#     "template_id": "my-template-id",
#     "base_image": "python:3.11-slim",
#     "start_cmd": "echo ready"
#   }'

# 1b. Registering a template from Dockerfile (Kaniko Path)
# curl -s -X POST "$API_URL/templates/from-dockerfile" \
#   -H "X-API-Key: $API_KEY" \
#   -H "Content-Type: application/json" \
#   -d '{
#     "template_id": "test-kaniko-v4",
#     "dockerfile": "FROM python:3.11-slim\nRUN pip install requests",
#     "start_cmd": "python3"
#   }'

# ============================================================================
# 2. Sandbox Lifecycle Operations
# ============================================================================

# 2a. Create Sandbox
# curl -s -X POST "$API_URL/sandboxes" \
#   -H "X-API-Key: $API_KEY" \
#   -H "Content-Type: application/json" \
#   -d '{
#     "template_id": "python:3.11",
#     "metadata": { "guest_ports": [8765] }
#   }'

# 2b. Create Sandbox from Snapshot
# curl -s -X POST "$API_URL/sandboxes" \
#   -H "X-API-Key: $API_KEY" \
#   -H "Content-Type: application/json" \
#   -d '{
#     "template_id": "test-kaniko-v3",
#     "from_snapshot_image": "my-snapshot-repo:v1"
#   }'

# 2c. Pause Sandbox
# curl -s -X POST "$API_URL/sandboxes/$SID/pause" \
#   -H "X-API-Key: $API_KEY"

# 2d. Resume / Reconnect Sandbox
# curl -s -X POST "$API_URL/sandboxes/$SID/resume" \
#   -H "X-API-Key: $API_KEY"

# 2e. Create Snapshot
# curl -s -X POST "$API_URL/sandboxes/$SID/snapshot" \
#   -H "X-API-Key: $API_KEY" \
#   -H "Content-Type: application/json" \
#   -d '{
#     "label": "my-snapshot-v1"
#   }'

# ============================================================================
# 3. Connection Details
# ============================================================================

# 3a. Get standard guest connection info (includes traffic_access_token)
# curl -s -X GET "$API_URL/sandboxes/$SID/connection?port=49983&scheme=ws" \
#   -H "X-API-Key: $API_KEY"

# 3b. Get direct envd connection info (includes envd_access_token)
# curl -s -X GET "$API_URL/sandboxes/$SID/envd-connection" \
#   -H "X-API-Key: $API_KEY"

# ============================================================================
# 4. File Operations via API Server (envd proxy)
# ============================================================================

# 4a. List files in a directory
# curl -s -X GET "$API_URL/sandboxes/$SID/files?path=/tmp" \
#   -H "X-API-Key: $API_KEY"
# curl -s -k -X POST "${PROXY}/v1/fs/list_dir" \
#   -H "Host: ${HOST}" \
#   -H "e2b-traffic-access-token: ${TOKEN}" \
#   -H "X-Access-Token: ${ENVD_TOKEN}" \
#   -H "Content-Type: application/json" \
#   -d '{"path": "/tmp"}'

# 4b. Read a file
# curl -s -k -X GET "$API_URL/sandboxes/$SID/files/read?path=/tmp/hello.txt" \
#   -H "X-API-Key: $API_KEY"

# 4c. Write to a file
# curl -s -k -X POST "$API_URL/sandboxes/$SID/files/write" \
#   -H "X-API-Key: $API_KEY" \
#   -H "Content-Type: application/json" \
#   -d '{
#     "path": "/tmp/hello.txt",
#     "content": "Hello from API!"
#   }'
# curl -s -X POST "${PROXY}/files?path=/tmp/myfile.txt" \
#   -H "Host: ${HOST}" \
#   -H "e2b-traffic-access-token: ${TOKEN}" \
#   -H "X-Access-Token: ${ENVD_TOKEN}" \
#   -H "Content-Type: application/octet-stream" \
#   --data-binary "hello world"
# 4d. Create a directory
# curl -s -X POST "$API_URL/sandboxes/$SID/files/mkdir" \
#   -H "X-API-Key: $API_KEY" \
#   -H "Content-Type: application/json" \
#   -d '{
#     "path": "/tmp/new_folder"
#   }'

# 4e. Delete a file or directory
# curl -s -k -X POST "$API_URL/sandboxes/$SID/files/delete" \
#   -H "X-API-Key: $API_KEY" \
#   -H "Content-Type: application/json" \
#   -d '{
#     "path": "/tmp/hello.txt",
#     "recursive": false
#   }'
# curl -s -k -X GET "$API_URL/sandboxes/$SID/files/read?path=/tmp/hello.txt" \
#   -H "X-API-Key: $API_KEY"
# ============================================================================
# 5. Direct Data-Plane Operations via Ingress (envd guest daemon)
#    These hit the sandbox directly via the ingress controller on port 49983.
# ============================================================================

# 5a. List directory (Data Plane)
# curl -s -X POST "$ENVD_URL/v1/fs/list_dir" \
#   -H "E2b-Traffic-Access-Token: $API_KEY" \
#   -H "X-Access-Token: $ENVD_TOKEN" \
#   -H "Content-Type: application/json" \
#   -d '{ "path": "/tmp" }'
# curl -vk --resolve "49983-sb-c2cd7dccf9b742c6.sndbx.com:443:192.168.58.2" "https://49983-sb-c2cd7dccf9b742c6.sndbx.com/v1/fs/list_dir" -H "E2b-Traffic-Access-Token: ${TOKEN}" -H "X-Access-Token: 9anBHF_skSoghxxfPvWJ_noDfQO_JkCOO-vL8InTH44" -H "Content-Type: application/json" -d '{"path": "/tmp"}'
# curl -s -k -X POST "${ENVD_URL}/v1/fs/list_dir" \
#   -H "Host: ${HOST}" \
#   -H "e2b-traffic-access-token: ${TOKEN}" \
#   -H "X-Access-Token: ${ENVD_TOKEN}" \
#   -H "Content-Type: application/json" \
#   -d '{"path": "/"}'
# 5b. Read a file (Data Plane)
# curl -s -k -X GET "$ENVD_URL/files?path=/etc/os-release" \
#   -H "e2b-traffic-access-token: ${TOKEN}" \
#   -H "X-Access-Token: $ENVD_TOKEN"

# 5c. Write to a file (Data Plane)

  # -H "E2b-Traffic-Access-Token: $TOKEN" 
# curl -s -X POST "$ENVD_URL/files?path=/tmp/direct_hello.txt" \
#   -H "X-Access-Token: $ENVD_TOKEN" \
#   -d "Hello directly from the data plane ingress!"

# 5d. Make a directory (Data Plane)
# curl -s -X POST "$ENVD_URL/v1/fs/mkdir" \
#   -H "E2b-Traffic-Access-Token: $API_KEY" \
#   -H "X-Access-Token: $ENVD_TOKEN" \
#   -H "Content-Type: application/json" \
#   -d '{ "path": "/tmp/direct_dir" }'

# 5e. Delete a file/directory (Data Plane)
# curl -s -X POST "$ENVD_URL/v1/fs/remove" \
#   -H "E2b-Traffic-Access-Token: $API_KEY" \
#   -H "X-Access-Token: $ENVD_TOKEN" \
#   -H "Content-Type: application/json" \
#   -d '{ "path": "/tmp/direct_hello.txt" }'

# 5f. Run a Process (Data Plane)
curl -s -k -X POST "$ENVD_URL/v1/process/start" \
  -H "E2b-Traffic-Access-Token: $TOKEN" \
  -H "X-Access-Token: $ENVD_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "command": "uname -a",
    "cwd": "/tmp"
  }'
