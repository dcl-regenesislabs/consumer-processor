#!/bin/bash

# Godot environment
export DISPLAY=:99
export RUST_LOG=warn,dclgodot=info,dclgodot::content::resource_provider=debug
export NO_COLOR=1

# Start Xvfb
/usr/bin/Xvfb -ac :99 -screen 0 1280x1024x24 > /dev/null 2>&1 &
sleep 1

# Start Node.js app (Godot is started/restarted per entity by the asset-server adapter)
cd /service
exec /usr/bin/node \
    --enable-source-maps \
    --trace-warnings \
    --abort-on-uncaught-exception \
    --unhandled-rejections=strict \
    dist/index.js "$@"
