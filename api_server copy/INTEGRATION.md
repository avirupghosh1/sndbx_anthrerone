# Integration Guide: SDK + API Server

Complete guide to integrating the Python SDK with the API Server.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Integration Steps](#integration-steps)
3. [Testing Locally](#testing-locally)
4. [Production Deployment](#production-deployment)
5. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

```
┌──────────────────────────┐
│   Your Application       │
│  (Using Python SDK)      │
└────────────┬─────────────┘
             │
    ┌────────▼────────────┐
    │  Python SDK         │
    │  (my_sdk package)   │
    │  - Sandbox class    │
    │  - Commands API     │
    │  - Files API        │
    │  - Async support    │
    └────────┬────────────┘
             │ REST API calls (HTTP)
    ┌────────▼─────────────────────┐
    │   API Server (FastAPI)       │
    │   Location: api_server/      │
    │   - Authentication           │
    │   - Request routing          │
    │   - Error handling           │
    └────────┬─────────────────────┘
             │
    ┌────────▼──────────────────────┐
    │   Orchestrator               │
    │   - SandboxManager           │
    │   - ContainerManager         │
    │   - AgentRuntime             │
    └────────┬──────────────────────┘
             │ Docker API
    ┌────────▼──────────────────────┐
    │   Docker Engine              │
    │   (Container Runtime)        │
    └────────┬──────────────────────┘
             │
    ┌────────▼──────────────────────┐
    │   Containers (Sandboxes)     │
    │   - python:3.11              │
    │   - node:18                  │
    │   - custom images            │
    └──────────────────────────────┘
```

---

## Integration Steps

### Step 1: Start API Server

**Option A: Docker Compose (Recommended)**

```bash
cd api_server
docker-compose up -d

# Verify
curl http://localhost:8000/health
# {"status": "ok", "version": "1.0.0"}
```

**Option B: Direct Python**

```bash
cd api_server
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py
```

**Option C: Docker Manual**

```bash
cd api_server
docker build -t sandbox-api .
docker run -d \
  -p 8000:8000 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e API_KEY=test-key-12345 \
  sandbox-api
```

### Step 2: Configure SDK

Create `.env` file in project root:

```bash
# .env
MY_SDK_API_URL=http://localhost:8000
MY_SDK_API_KEY=test-key-12345
MY_SDK_REQUEST_TIMEOUT=30
```

Or set environment variables:

```bash
export MY_SDK_API_URL=http://localhost:8000
export MY_SDK_API_KEY=test-key-12345
export MY_SDK_REQUEST_TIMEOUT=30
```

### Step 3: Use SDK

```python
from my_sdk import Sandbox

# Create sandbox
sandbox = Sandbox.create()

# Run command
result = sandbox.commands.run("python -c 'print(\"Hello\")'")
print(result.stdout)  # Hello

# Use files
sandbox.files.write("/tmp/test.txt", "content")
content = sandbox.files.read("/tmp/test.txt")

# Cleanup
sandbox.kill()
```

---

## Testing Locally

### Quick Test Script

```python
# test_integration.py
import os
from my_sdk import Sandbox
from dotenv import load_dotenv

# Load environment
load_dotenv()

def test_basic_workflow():
    """Test basic sandbox operations."""
    print("Creating sandbox...")
    sandbox = Sandbox.create(
        api_url=os.getenv("MY_SDK_API_URL"),
        api_key=os.getenv("MY_SDK_API_KEY")
    )
    print(f"✓ Sandbox created: {sandbox.sandbox_id}")

    # Test command execution
    print("Running command...")
    result = sandbox.commands.run("echo 'Hello World'")
    assert result.exit_code == 0
    assert "Hello World" in result.stdout
    print(f"✓ Command executed: {result.stdout.strip()}")

    # Test file operations
    print("Writing file...")
    sandbox.files.write("/tmp/test.txt", "Hello from file")
    print("✓ File written")

    print("Reading file...")
    content = sandbox.files.read("/tmp/test.txt")
    assert content == "Hello from file"
    print(f"✓ File read: {content}")

    # Test metrics
    print("Getting metrics...")
    metrics = sandbox.metrics()
    print(f"✓ Metrics: Memory={metrics['memory_usage']/1024/1024:.1f}MB")

    # Cleanup
    print("Killing sandbox...")
    sandbox.kill()
    print("✓ Sandbox killed")


async def test_concurrent_operations():
    """Test concurrent sandbox operations."""
    from my_sdk import AsyncSandbox
    import asyncio

    print("Creating 5 concurrent sandboxes...")
    
    async def create_and_test(i):
        sandbox = await AsyncSandbox.create()
        result = await sandbox.commands.run(f"echo 'Sandbox {i}'")
        await sandbox.kill()
        return result.stdout

    tasks = [create_and_test(i) for i in range(5)]
    results = await asyncio.gather(*tasks)
    
    print(f"✓ Concurrent operations completed")
    for i, result in enumerate(results):
        print(f"  Sandbox {i}: {result.strip()}")


def test_agent_workflow():
    """Test agent spawning and messaging."""
    print("Creating sandbox with agent...")
    sandbox = Sandbox.create()

    print("Spawning agent...")
    agent = sandbox.spawn_agent(
        agent_name="test_agent",
        agent_code="""
import json
import time

count = 0
while True:
    count += 1
    print(f"Agent tick: {count}")
    time.sleep(1)
""",
        config={"debug": True}
    )
    print(f"✓ Agent spawned: {agent.agent_id}")

    # Wait for agent to run
    time.sleep(2)

    # Send message
    print("Sending agent message...")
    sandbox.send_agent_message(
        agent_id=agent.agent_id,
        message_type="task",
        content={"task": "test"}
    )
    print("✓ Message sent")

    # Get agent status
    status = sandbox.get_agent(agent.agent_id)
    print(f"✓ Agent status: {status['state']}")

    # Get messages
    messages = sandbox.get_agent_messages(agent.agent_id)
    print(f"✓ Agent messages: {len(messages)}")

    # Cleanup
    sandbox.kill_agent(agent.agent_id)
    sandbox.kill()
    print("✓ Cleanup complete")


if __name__ == "__main__":
    print("=== Sandbox Integration Tests ===\n")
    
    try:
        print("Test 1: Basic Workflow")
        test_basic_workflow()
        print()
        
        print("Test 2: Concurrent Operations")
        import asyncio
        asyncio.run(test_concurrent_operations())
        print()
        
        print("Test 3: Agent Workflow")
        import time
        test_agent_workflow()
        print()
        
        print("=== All Tests Passed ✓ ===")
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
```

Run tests:

```bash
# Set environment
export MY_SDK_API_URL=http://localhost:8000
export MY_SDK_API_KEY=test-key-12345

# Run tests
python test_integration.py
```

Expected output:

```
=== Sandbox Integration Tests ===

Test 1: Basic Workflow
Creating sandbox...
✓ Sandbox created: sb-abc123...
Running command...
✓ Command executed: Hello World
Writing file...
✓ File written
Reading file...
✓ File read: Hello from file
Getting metrics...
✓ Metrics: Memory=52.1MB
Killing sandbox...
✓ Sandbox killed

Test 2: Concurrent Operations
Creating 5 concurrent sandboxes...
✓ Concurrent operations completed
  Sandbox 0: Sandbox 0
  Sandbox 1: Sandbox 1
  ...

Test 3: Agent Workflow
Creating sandbox with agent...
✓ Agent spawned: agent-abc123
Sending agent message...
✓ Message sent
✓ Agent status: running
✓ Agent messages: 1
✓ Cleanup complete

=== All Tests Passed ✓ ===
```

---

## Production Deployment

### Docker Compose Stack

```yaml
version: '3.8'

services:
  # API Server
  sandbox-api:
    build:
      context: ./api_server
      dockerfile: Dockerfile
    ports:
      - "8000:8000"
    environment:
      - API_KEY=${API_KEY}
      - DEBUG=false
      - DATABASE_PATH=/data/sandboxes.db
      - LOG_LEVEL=INFO
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - api_data:/data
    networks:
      - sandbox-net
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  # Nginx reverse proxy
  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - ./ssl:/etc/nginx/ssl:ro
    networks:
      - sandbox-net
    depends_on:
      - sandbox-api

networks:
  sandbox-net:
    driver: bridge

volumes:
  api_data:
    driver: local
```

### Nginx Configuration

```nginx
# nginx.conf
upstream sandbox_api {
    server sandbox-api:8000;
}

server {
    listen 80;
    server_name api.example.com;
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name api.example.com;

    ssl_certificate /etc/nginx/ssl/cert.pem;
    ssl_certificate_key /etc/nginx/ssl/key.pem;

    location / {
        proxy_pass http://sandbox_api;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-API-Key $http_x_api_key;
        
        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 300s;
        proxy_read_timeout 300s;
    }

    # Rate limiting
    limit_req_zone $binary_remote_addr zone=api_limit:10m rate=100r/s;
    limit_req zone=api_limit burst=200 nodelay;
}
```

### Environment Setup

Create `.env` for production:

```bash
# Production secrets
API_KEY=your-strong-random-api-key-here
DEBUG=false
LOG_LEVEL=INFO

# Database
DATABASE_PATH=/data/sandboxes.db

# Resource defaults
DEFAULT_TEMPLATE=python:3.11
DEFAULT_CPU_LIMIT=2
DEFAULT_MEMORY_LIMIT=1g
DEFAULT_TIMEOUT=7200
```

### Start Production Stack

```bash
docker-compose up -d

# Verify
curl -H "X-API-Key: your-api-key" http://localhost/health

# View logs
docker-compose logs -f sandbox-api

# Monitor
docker stats sandbox-api
```

---

## Troubleshooting

### SDK Can't Connect to Server

**Issue**: `ConnectionError: Failed to connect to http://localhost:8000`

**Solution**:
```bash
# Check server is running
curl http://localhost:8000/health

# Check API key
export MY_SDK_API_KEY=test-key-12345

# Check URL
export MY_SDK_API_URL=http://localhost:8000

# Test with curl
curl -H "X-API-Key: test-key-12345" http://localhost:8000/sandboxes
```

### Docker Socket Permission Denied

**Issue**: `permission denied while trying to connect to Docker daemon`

**Solution (Linux)**:
```bash
# Add user to docker group
sudo usermod -aG docker $USER

# Restart Docker
sudo systemctl restart docker

# Log out and back in
```

**Solution (Docker Compose)**:
```yaml
volumes:
  - /var/run/docker.sock:/var/run/docker.sock
```

### API Server Crashes

**Issue**: Container exits with error

**Solution**:
```bash
# Check logs
docker logs sandbox-api

# Common issues:
# - Docker not running: start Docker daemon
# - Port already in use: change PORT env var
# - Invalid API key: use strong random string

# Restart
docker-compose restart sandbox-api
```

### High Memory Usage

**Issue**: Sandboxes using too much memory

**Solution**:
```python
# Set memory limits
sandbox = Sandbox.create(
    memory_limit="512m"  # Reduce from default 1g
)

# Clean up old sandboxes
import requests
api_url = "http://localhost:8000"
api_key = "test-key-12345"

response = requests.get(
    f"{api_url}/sandboxes",
    headers={"X-API-Key": api_key}
)

for sandbox in response.json():
    requests.post(
        f"{api_url}/sandboxes/{sandbox['sandbox_id']}/kill",
        headers={"X-API-Key": api_key}
    )
```

### Database Locked

**Issue**: `sqlite3.OperationalError: database is locked`

**Solution**:
```bash
# Check for running servers
ps aux | grep main.py

# Kill all instances
pkill -f "python main.py"

# Remove database
rm sandboxes.db

# Restart
python main.py
```

---

## Performance Tuning

### Connection Pooling

Modify `api_server/config.py`:

```python
# Increase connection pool size
SQLITE_POOL_SIZE = 10
```

### Concurrent Sandboxes

```python
import asyncio
from my_sdk import AsyncSandbox

async def create_many_sandboxes(count):
    # Create N sandboxes concurrently
    tasks = [AsyncSandbox.create() for _ in range(count)]
    sandboxes = await asyncio.gather(*tasks)
    return sandboxes

# Create 100 sandboxes in parallel
sandboxes = asyncio.run(create_many_sandboxes(100))
```

### Load Testing

```python
# load_test.py
import asyncio
import time
from my_sdk import AsyncSandbox

async def load_test(num_sandboxes, commands_per_sandbox):
    """Load test the API server."""
    print(f"Load test: {num_sandboxes} sandboxes, {commands_per_sandbox} commands each")
    
    start_time = time.time()
    
    # Create sandboxes
    sandboxes = await asyncio.gather(*[
        AsyncSandbox.create() for _ in range(num_sandboxes)
    ])
    
    create_time = time.time() - start_time
    print(f"✓ Created {num_sandboxes} sandboxes in {create_time:.1f}s")
    
    # Run commands
    start_time = time.time()
    tasks = []
    for sandbox in sandboxes:
        for i in range(commands_per_sandbox):
            tasks.append(sandbox.commands.run(f"echo 'Command {i}'"))
    
    results = await asyncio.gather(*tasks)
    
    cmd_time = time.time() - start_time
    print(f"✓ Ran {len(results)} commands in {cmd_time:.1f}s")
    
    # Cleanup
    start_time = time.time()
    await asyncio.gather(*[sandbox.kill() for sandbox in sandboxes])
    cleanup_time = time.time() - start_time
    print(f"✓ Cleaned up {num_sandboxes} sandboxes in {cleanup_time:.1f}s")
    
    print(f"\nPerformance:")
    print(f"  Sandbox creation: {create_time/num_sandboxes*1000:.1f}ms each")
    print(f"  Command execution: {cmd_time/len(results)*1000:.1f}ms each")
    print(f"  Sandbox cleanup: {cleanup_time/num_sandboxes*1000:.1f}ms each")

# Run load test
asyncio.run(load_test(num_sandboxes=50, commands_per_sandbox=10))
```

---

## Next Steps

1. **Deploy to production** following the deployment guide
2. **Set up monitoring** with Prometheus/Grafana
3. **Configure logging** with ELK stack or CloudWatch
4. **Implement CI/CD** with GitHub Actions/GitLab CI
5. **Add metrics** and alerting
6. **Scale horizontally** with load balancing

---

**Happy sandboxing! 🎉**
