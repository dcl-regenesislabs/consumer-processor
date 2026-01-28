#!/bin/bash
set -m

# Start Xvfb
/usr/bin/Xvfb -ac :99 -screen 0 1280x1024x24 > /dev/null 2>&1 &
export DISPLAY=:99
sleep 1

# Start asset-server in background
cd /app
RUST_LOG=warn,dclgodot::content::resource_provider=debug,dclgodot::asset_server=info \
./decentraland.godot.client.x86_64 --headless --asset-server --asset-server-port ${ASSET_SERVER_PORT:-8080} &
GODOT_PID=$!

# Wait for server to be ready
echo "Waiting for asset-server on port ${ASSET_SERVER_PORT:-8080}..."
for i in {1..60}; do
    if curl -s http://localhost:${ASSET_SERVER_PORT:-8080}/health > /dev/null 2>&1; then
        echo "Asset-server ready"
        break
    fi
    sleep 1
done

# Start Node.js app
cd /service
exec /usr/bin/node \
    --enable-source-maps \
    --trace-warnings \
    --abort-on-uncaught-exception \
    --unhandled-rejections=strict \
    dist/index.js "$@"
