#!/bin/bash
set -e

# Godot environment
export DISPLAY=:99
export RUST_LOG=warn,dclgodot=info,dclgodot::content::resource_provider=debug
export NO_COLOR=1

# Force Mesa to use the software rasterizer (llvmpipe). Without these, Mesa
# tries pci-id detection against /dev/dri first; in a container without a
# real GPU node that path can silently fall through to a no-op driver and
# Godot's opengl3 init fails, dropping the engine into the dummy renderer
# (texture_2d_get returns null, every SubViewport readback comes back blank,
# every impostor bake fails with `fail_blank_albedo`).
export LIBGL_ALWAYS_SOFTWARE=1
export MESA_LOADER_DRIVER_OVERRIDE=llvmpipe

# Start Xvfb in the background
/usr/bin/Xvfb -ac :99 -screen 0 1280x1024x24 > /dev/null 2>&1 &

# Wait for Xvfb to actually accept X connections before launching the Node
# app (which spawns Godot on demand). A blind `sleep 1` raced — under
# cold-start container load Xvfb occasionally needed >1s, Godot started
# without a display, and the opengl3 driver silently fell back to dummy.
for i in 1 2 3 4 5 6 7 8 9 10; do
  if xdpyinfo -display :99 >/dev/null 2>&1; then
    echo "[entrypoint] Xvfb ready after ${i} attempts"
    break
  fi
  sleep 0.5
  if [ "$i" = "10" ]; then
    echo "[entrypoint] FATAL: Xvfb never came up on :99"
    exit 1
  fi
done

# Sanity-print the GL renderer so misconfigured deploys are obvious in logs.
glxinfo -B 2>&1 | grep -E "renderer string|OpenGL version" | head -2 || echo "[entrypoint] WARN: glxinfo failed"

# Start Node.js app (Godot is started/restarted per entity by the asset-server adapter)
cd /service
exec /usr/bin/node \
    --enable-source-maps \
    --trace-warnings \
    --abort-on-uncaught-exception \
    --unhandled-rejections=strict \
    dist/index.js "$@"
