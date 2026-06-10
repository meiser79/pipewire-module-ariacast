# pipewire-module-ariacast

A PipeWire `.so` module that registers as an **Audio/Sink** in the PipeWire
graph and streams audio to an **AriaCast** receiver over WebSocket, implementing
the [AriaCast Protocol Spec v1.1](https://github.com/AriaCast/AriaCast-Protocol-Spec).

Loaded directly by the PipeWire daemon — no separate process required.
Connects to the AriaCast server **only when audio is actively routed to the
sink**, and disconnects when the sink goes idle.

This project was created with the assistance of Claude and verified through
continuous testing.
If you prefer software built without AI-assisted coding, this project may not
be the right fit for you.

## Files

```
pipewire-module-ariacast/
├── module-ariacast.c          Module source
├── meson.build                Build definition
├── pipewire.conf.d/
│   └── ariacast.conf          Config snippet for ~/.config/pipewire/
└── README.md
```

## Dependencies

```bash
# Debian / Ubuntu
sudo apt install \
  libpipewire-0.3-dev libspa-0.2-dev \
  libdbus-1-dev \
  meson ninja-build pkg-config gcc

# Arch Linux
sudo pacman -S pipewire dbus meson ninja pkgconf gcc

# Fedora
sudo dnf install pipewire-devel dbus-devel meson ninja-build gcc pkgconfig
```

## Build & install

```bash
meson setup build
ninja -C build

# Install .so to PipeWire module dir + .conf to /etc/pipewire/pipewire.conf.d/
sudo ninja -C build install
```

## Configuration

Copy the config snippet to your user config directory:

```bash
mkdir -p ~/.config/pipewire/pipewire.conf.d
cp pipewire.conf.d/ariacast.conf ~/.config/pipewire/pipewire.conf.d/
```

Restart PipeWire:

```bash
systemctl --user restart pipewire
```

The AriaCast sink appears in audio mixers such as pavucontrol, GNOME Sound
Settings, KDE Audio and qpwgraph.
With auto-discovery enabled, its name and description are derived from the
discovered device name, so the displayed identity is different for each
AriaCast receiver. Route any application there and audio will be streamed
live to the AriaCast receiver.

### Auto-discovery (default)

Without `ariacast.server.host` the module broadcasts `DISCOVER_AUDIOCAST` on
UDP port 12888 at startup (5 attempts, 3 s apart) and connects to the first
server that responds.  This happens in a background thread and never blocks
the PipeWire daemon.

### Sink naming from the discovered server (default)

The discovery response contains a `server_name` field. When discovery is
enabled, the module derives the PipeWire sink identity automatically from it.
No sink name or description needs to be configured.

For example:

| Discovered `server_name` | `node.name` | Description / `node.nick` |
|---|---|---|
| `Living Room` | `ariacast-living-room` | `Living Room [AriaCast]` |
| `Küche (Echo)` | `ariacast-k-che-echo` | `Küche (Echo) [AriaCast]` |

The generated sink name is a lowercase slug with non-alphanumeric characters
collapsed to `-`, prefixed with `ariacast-`. The description keeps the device
name unchanged and appends `[AriaCast]` so the protocol is immediately visible
in audio-device lists.

`ariacast.sink.name` and `ariacast.sink.description` are optional manual
overrides. They can be configured independently; a configured value is kept
while the other value continues to follow discovery.

The default configuration therefore does not need either setting:

```
context.modules = [
  {
    name  = libpipewire-module-ariacast
    flags = [ ifexists nofail ]
    args  = {
      # Optional manual overrides:
      #"ariacast.sink.name" = "ariacast"
      #"ariacast.sink.description" = "AriaCast Speaker"
    }
  }
]
```

### Fixed server

When `ariacast.server.host` is configured, UDP discovery is skipped. In this
mode there is no discovered device name, so configuring `ariacast.sink.name`
and/or `ariacast.sink.description` is recommended if you want a specific sink
identity.

Uncomment the host/port lines in `ariacast.conf`:

```
context.modules = [
  {
    name  = libpipewire-module-ariacast
    flags = [ ifexists nofail ]
    args  = {
      "ariacast.server.host"        = "192.168.1.10"
      "ariacast.server.port"        = "12889"
      "ariacast.sink.name"          = "ariacast"
      "ariacast.sink.description"   = "Living Room"
    }
  }
]
```

Setting `ariacast.server.host` automatically disables UDP discovery.

### Multiple AriaCast targets simultaneously

Load the module twice with different `args`:

```
context.modules = [
  {
    name  = libpipewire-module-ariacast
    flags = [ ifexists nofail ]
    args  = {
      "ariacast.server.host"       = "192.168.1.10"
      "ariacast.sink.name"         = "ariacast-living-room"
      "ariacast.sink.description"  = "Living Room"
    }
  }
  {
    name  = libpipewire-module-ariacast
    flags = [ ifexists nofail ]
    args  = {
      "ariacast.server.host"       = "192.168.1.20"
      "ariacast.sink.name"         = "ariacast-kitchen"
      "ariacast.sink.description"  = "Kitchen"
    }
  }
]
```

## Logging & debugging

```bash
# Debug log for this module only
PIPEWIRE_DEBUG="mod.libpipewire-module-ariacast:4" pipewire

# All PipeWire logs
PIPEWIRE_DEBUG=4 pipewire

# Load at runtime without restarting (useful for testing)
pw-cli load-module libpipewire-module-ariacast \
  '"ariacast.server.host"="192.168.1.10" "ariacast.sink.name"="test"'
```

## Protocol implementation (AriaCast Spec v1.1)

| Spec requirement | Implementation |
|---|---|
| Discovery: `DISCOVER_AUDIOCAST` UDP broadcast | `discover()` |
| Discovery: 5 attempts, 3 s delay, ephemeral socket | `discover()`, called at startup + on retries by `thr_connmgr_fn()` |
| Discovery: `server_name` used for sink naming | `apply_server_name()` / `slugify()` |
| Discovery: sink description gets `[AriaCast]` suffix | `apply_server_name()` |
| `/audio`: binary WS frames, 3840 bytes | `thr_audio_fn()` + `ring_t` |
| `/audio`: READY handshake, 3 s timeout | `audio_connect()` |
| `/audio`: reconnect on send failure | `thr_audio_fn()` retry |
| `/control`: receive `action`-keyed commands | `thr_ctrl_fn()` + `handle_ctrl_action()` |
| `/control`: optional Python server handshake | `ctrl_connect()` (1 s timeout) |
| `/stats`: receive server push | `thr_stats_fn()` |
| Metadata: `HTTP POST /metadata` with `{"data":{…}}` | `http_post_meta()` |
| Metadata: both camelCase + snake_case keys | `meta_to_json()` |
| Metadata: refresh every 10 s | `thr_meta_fn()` |
| Metadata: re-send on reconnect | `do_connect()` |
| Reconnect backoff: `1000 × 2^min(attempt, 5)` ms | `isleep_ms()` inline |

### Audio format

The stream's `EnumFormat` announces a single fixed format -
S16LE, 2 channels, 48000 Hz - so PipeWire's `audioconvert` filter handles
any sample-rate conversion and channel mapping upstream of this module.
`on_process()` always receives audio already in this format; it only
splits the buffer into 3840-byte (20 ms) AriaCast frames.

## Metadata sources

A dedicated thread (`thr_mpris_fn`) listens for
`org.freedesktop.DBus.Properties.PropertiesChanged` on the session bus and
reads standard MPRIS2 keys from the `Metadata` property:

| MPRIS2 key | AriaCast field |
|---|---|
| `xesam:title`  | `title` |
| `xesam:artist` | `artist` (first element if an array) |
| `xesam:album`  | `album` |
| `mpris:artUrl` | `artworkUrl` |

Empty-string values are treated as "not present", since some players send
empty strings for fields they have cleared rather than omitting them.

`mpris:trackid` is used only to detect track changes: when the track ID
changes and the new `Metadata` update has no `xesam:album` / `mpris:artUrl`,
the previous track's album/artwork are cleared so stale cover art isn't
shown for the new track.

Works with any MPRIS2-compatible player: VLC, Spotify, Rhythmbox,
Clementine, Firefox ≥ 85, Chromium. Internet radio stations that only expose
the current track via non-standard properties (e.g. VLC's `vlc:nowplaying`)
are not specifically parsed — `xesam:title`/`xesam:artist` are used as-is,
which for some stations may show the station name rather than the current
song.

## Architecture

```
PipeWire daemon
└── libpipewire-module-ariacast.so
    │
    ├── pw_stream  media.class=Audio/Sink  (RT thread)
    │     on_process()        ──► silence detection ──► want_connect flag
    │                          ──► push_audio() ──► ring_push()
    │     on_state_changed()  ──► UNCONNECTED/ERROR ──► want_connect=false
    │
    ├── thr_watchdog   every 250 ms: clears want_connect after AC_IDLE_MS
    │                  of silence (no audio in on_process)
    │
    ├── thr_connmgr   waits for want_connect=true
    │     ├── (first connect) runs UDP discovery if no host configured
    │     ├── do_connect()    → /audio WS + READY handshake
    │     │                   → /control WS
    │     │                   → /stats WS (optional)
    │     │                   → HTTP POST /metadata
    │     ├── reconnects with exponential backoff while want_connect
    │     │   stays true (re-discovers every AC_REDISCOVER_EVERY attempts)
    │     └── do_disconnect() when want_connect=false (sink idle)
    │
    ├── ring_t  [256 × 3840 B]
    │
    ├── thr_audio   ring_pop() → ws_send_frame(/audio, BINARY, 3840 B)
    ├── thr_ctrl    ws_recv_frame(/control) → handle_ctrl_action()
    ├── thr_stats   ws_recv_frame(/stats)   → pw_log_debug()
    ├── thr_meta    HTTP POST /metadata every 10 s (while connected)
    └── thr_mpris   DBus session bus → MPRIS2 PropertiesChanged → meta update
```

### Shutdown handling

`pw_context_connect_self()` creates an internal client connection used for
the `pw_stream`. During a full daemon shutdown (`pw_context_destroy()`),
PipeWire may reclaim this connection (and the stream/proxy objects bound to
it) *before* invoking this module's `.destroy` hook. Calling
`pw_stream_destroy()` on such an already-reclaimed object crashes inside
libpipewire.

A `pw_proxy` listener on the core connection (`core_proxy_destroy`) detects
this case: if the daemon has already torn the connection down, the module
skips its own `pw_stream_destroy()`/`pw_core_disconnect()` calls. On a
normal module unload (no full daemon stop), this listener does not fire and
the module tears down its objects itself — no leak in that case.

## Known limitations / TODO

- No TLS (`wss://`) — spec security model targets trusted LAN only
- `seek` commands from `/control` are received and logged but not forwarded
  to PipeWire (would require MediaSession integration)
- `/stats` data is logged via `pw_log_debug` but not exposed as PipeWire
  node properties

## License

MIT
