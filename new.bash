# Replace these:
export DOMAIN="sndbx.com"        # your SANDBOX_DOMAIN
export SID="sb-9cad2e23ab144531"           # your sandbox ID
# injected at sandbox create
export ENVD_TOKEN="SbT3othmAh9HSJAau2Mq1CkibTr9AX_1a3nRhRSxhPI"
# The envd port is 49983 by default, so host = 49983-{SID}.{DOMAIN}
HOST="49983-${SID}.${DOMAIN}"
PROXY="http://${HOST}"  # or through your proxy at localhost:8080 with Host header

# --- List directory ---

# --- Stat a file/dir ---


# --- Read/download a file ---

# --- Write/upload a file ---
curl -s -X POST "49983-sb-9cad2e23ab144531.sndbx.com/files?path=/tmp/myfile.txt" \
  -H "Host: 49983-sb-9cad2e23ab144531.sndbx.com:443:192.168.49.2" \
  -H "e2b-traffic-access-token: wXhiT5os5_-5zPx1R0CHmA2x30mxSFcUgTUGAACUxRg" \
  -H "X-Access-Token: SbT3othmAh9HSJAau2Mq1CkibTr9AX_1a3nRhRSxhPI" \
  -H "Content-Type: application/octet-stream" \
  --data-binary "hello world"

curl -s -X POST "${PROXY}/v1/fs/list_dir" \
  -H "Host: ${HOST}" \
  -H "e2b-traffic-access-token: ${TOKEN}" \
  -H "X-Access-Token: ${ENVD_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"path": "/tmp"}'

# --- Create directory ---
curl -s -X POST "${PROXY}/v1/fs/mkdir" \
  -H "Host: ${HOST}" \
  -H "e2b-traffic-access-token: ${TOKEN}" \
  -H "X-Access-Token: ${ENVD_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"path": "/tmp/newdir"}'

# --- Remove file/dir ---
curl -s -X POST "${PROXY}/v1/fs/remove" \
  -H "Host: ${HOST}" \
  -H "e2b-traffic-access-token: ${TOKEN}" \
  -H "X-Access-Token: ${ENVD_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"path": "/tmp/myfile.txt"}'

curl -s -X POST "${PROXY}/v1/fs/list_dir" \
  -H "Host: ${HOST}" \
  -H "e2b-traffic-access-token: ${TOKEN}" \
  -H "X-Access-Token: ${ENVD_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"path": "/tmp"}'
# --- Move/rename ---
# curl -s -X POST "${PROXY}/v1/fs/move" \
#   -H "Host: ${HOST}" \
#   -H "e2b-traffic-access-token: ${TOKEN}" \
#   -H "X-Access-Token: ${ENVD_TOKEN}" \
#   -H "Content-Type: application/json" \
#   -d '{"source": "/tmp/old.txt", "destination": "/tmp/new.txt"}'