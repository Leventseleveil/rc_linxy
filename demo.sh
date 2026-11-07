#!/usr/bin/env bash
# 端到端演示：
# 1. 起一个 mock 供应商服务（三种行为：正常 200 / 先 500 后 200 / 400 拒绝）
# 2. 起通知服务
# 3. 提交四种通知，观察状态流转（DELIVERED / RETRYING→DELIVERED / 死信 / 幂等去重）
set -euo pipefail
cd "$(dirname "$0")"

MVN="${MVN:-mvn}"
if ! command -v "$MVN" >/dev/null 2>&1 && [ -x "$HOME/tools/apache-maven-3.9.9/bin/mvn" ]; then
  MVN="$HOME/tools/apache-maven-3.9.9/bin/mvn"
fi

echo "==> [demo] 编译打包（跳过测试）"
"$MVN" -q package -DskipTests

echo "==> [demo] 启动 mock 供应商端点 :9099"
python3 - <<'PY' &
from http.server import BaseHTTPRequestHandler, HTTPServer
import time, json, sys

state = {}

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_POST(self):
        n = int(self.headers.get('X-Notify-Dedup-Key', '0').split('-')[-1] or 0)
        body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
        if self.path == '/ok':
            self._reply(200, b'{"ok":true}')
        elif self.path == '/flaky':
            # 第一次 500，之后 200 —— 演示重试成功
            k = state.setdefault(self.path, 0) + 1
            state[self.path] = k
            if k == 1:
                self._reply(500, b'{"err":"boom"}')
            else:
                self._reply(200, b'{"ok":true}')
        elif self.path == '/timeout':
            time.sleep(10)
            self._reply(200, b'{}')
        elif self.path == '/reject':
            self._reply(400, b'{"err":"bad request"}')
        else:
            self._reply(404, b'{}')
    def _reply(self, code, body):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(body)

HTTPServer(('127.0.0.1', 9099), H).serve_forever()
PY
MOCK_PID=$!
trap "kill $MOCK_PID 2>/dev/null || true" EXIT
sleep 1

echo "==> [demo] 写演示供应商配置"
mkdir -p ./demo-suppliers
cat > demo-suppliers/demo_ok.yaml <<'YAML'
name: demo_ok
request:
  method: POST
  url: http://127.0.0.1:9099/ok
  headers: {Content-Type: application/json}
  body: '{"eventType": "{{event_type}}", "userId": "{{payload.user_id}}"}'
YAML
cat > demo-suppliers/demo_flaky.yaml <<'YAML'
name: demo_flaky
request:
  method: POST
  url: http://127.0.0.1:9099/flaky
  headers: {Content-Type: application/json}
  body: '{"eventType": "{{event_type}}"}'
retry:
  backoff_seconds: [1, 1, 2]
YAML
cat > demo-suppliers/demo_reject.yaml <<'YAML'
name: demo_reject
request:
  method: POST
  url: http://127.0.0.1:9099/reject
  headers: {Content-Type: application/json}
  body: '{"eventType": "{{event_type}}"}'
retry:
  backoff_seconds: [1, 1]
YAML

echo "==> [demo] 启动通知服务 :8080"
mkdir -p ./data
SUPPLIERS_DIR=demo-suppliers NOTIFY_DB=./data/demo.db \
  java -jar target/notify-service-1.0.0.jar \
  --server.port=8080 \
  --notify.suppliers-dir=demo-suppliers \
  --spring.datasource.url=jdbc:sqlite:./data/demo.db &
SVC_PID=$!
trap "kill $SVC_PID $MOCK_PID 2>/dev/null || true" EXIT

T="Authorization: Bearer dev-token-1"
CURL="curl -s --noproxy localhost,127.0.0.1"

# 等服务就绪
for i in $(seq 1 30); do
  if python3 -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8080/api/v1/metrics')" 2>/dev/null; then break; fi
  sleep 1
done

submit() { # supplier eventType payload_json dedup
  local supplier="$1" eventType="$2" payload="$3" dedup="$4"
  python3 - "$supplier" "$eventType" "$payload" "$dedup" <<'PY'
import json, sys, urllib.request
supplier, event_type, payload, dedup = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
data = json.dumps({"supplier": supplier, "eventType": event_type, "payload": payload, "dedupKey": dedup}).encode()
req = urllib.request.Request("http://127.0.0.1:8080/api/v1/notifications", data=data,
                             headers={"Authorization": "Bearer dev-token-1", "Content-Type": "application/json"})
try:
    with urllib.request.urlopen(req) as resp:
        print(resp.status, resp.read().decode())
except urllib.error.HTTPError as e:
    print(e.code, e.read().decode())
PY
}

echo
echo "==> [demo] 1) 正常投递（一次成功）"
submit demo_ok user.registered '{"user_id":"u-1"}' demo-1

echo "==> [demo] 2) 不稳定供应商（首次 500 → 重试成功）"
submit demo_flaky payment.succeeded '{"order_id":"o-1"}' demo-2

echo "==> [demo] 3) 不可重试失败（400 → 直接死信）"
submit demo_reject stock.change '{"sku":"s-1","qty":2}' demo-3

echo "==> [demo] 4) 幂等去重（同 dedupKey 重复提交）"
submit demo_ok user.registered '{"user_id":"u-1"}' demo-1

echo
echo "==> [demo] 等待投递引擎处理..."
sleep 8

get() { # path
  python3 - "$1" <<'PY'
import sys, urllib.request
req = urllib.request.Request("http://127.0.0.1:8080" + sys.argv[1],
                             headers={"Authorization": "Bearer dev-token-1"})
try:
    with urllib.request.urlopen(req) as resp:
        print(resp.read().decode())
except urllib.error.HTTPError as e:
    print(e.code, e.read().decode())
PY
}

echo "==> [demo] 各通知最终状态："
for id in $(get /api/v1/dead-letters | python3 -c 'import sys,json;print(" ".join(x["notification_id"] for x in json.load(sys.stdin)))'); do
  get /api/v1/notifications/$id
done

echo "==> [demo] 指标："
get /api/v1/metrics

echo
echo "[demo] 完成。日志/数据在 ./data/"
