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

# Install .so to the PipeWire module dir; the config snippet is installed
# as an example to <prefix>/share/doc/pipewire-module-ariacast/
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

The sink is created once this initial discovery has finished (immediately
after the first response, or after the last attempt if no server answers),
so that its `node.name` is derived from the discovered device from the
start.

### Sink naming from the discovered server (default)

The discovery response contains a `server_name` field. When discovery is
enabled, the module derives the PipeWire sink identity automatically from it.
No sink name or description needs to be configured.

For example:

| Discovered `server_name` | `node.name` | `node.description` / `node.nick` |
|---|---|---|
| `Living Room` | `ariacast-living-room` | `Living Room [AriaCast]` |
| `Küche (Echo)` | `ariacast-k-che-echo` | `Küche (Echo) [AriaCast]` |

The generated sink name is a lowercase slug with non-alphanumeric characters
collapsed to `-`, prefixed with `ariacast-`. The description keeps the device
name unchanged and appends `[AriaCast]` so the protocol is immediately visible
in audio-device lists.

If a later re-discovery finds a server with a different name, only the
description/nick are updated. `node.name` stays fixed while the sink exists,
because session managers such as WirePlumber store default-sink and routing
choices by `node.name`.

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
| Discovery: `server_name` used for sink naming | `apply_server_name()` / `slugify()` / `create_stream()` |
| Discovery: sink description gets `[AriaCast]` suffix | `apply_server_name()` |
| `/audio`: binary WS frames, 3840 bytes | `thr_audio_fn()` + `ring_t` |
| WebSocket upgrade incl. `Sec-WebSocket-Accept` check | `ws_connect()` |
| `/audio`: READY handshake, 3 s timeout | `audio_connect()` |
| `/audio`: reconnect on send failure | `thr_audio_fn()` retry |
| `/control`: receive `action`-keyed commands | `thr_ctrl_fn()` + `handle_ctrl_action()` |
| `/control`: forward commands to the active MPRIS player | `mpris_exec_cmd()` |
| `/control`: optional Python server handshake | `ctrl_connect()` (1 s timeout) |
| `/stats`: receive server push | `thr_stats_fn()` |
| Metadata: `HTTP POST /metadata` with `{"data":{…}}` | `http_post_meta()` |
| Metadata: both camelCase + snake_case keys | `meta_to_json()` |
| Metadata: refresh every 10 s (every 1 s while playing), immediately on change | `thr_meta_fn()` |
| Metadata: `positionMs` = last position + elapsed × rate | `meta_pos_locked()` |
| Metadata: re-send on reconnect | `do_connect()` |
| Metadata: JSON string escaping | `json_escape()` / `meta_to_json()` |
| Reconnect backoff: `1000 × 2^min(attempt, 5)` ms | `isleep_ms()` inline |

### Audio format

The stream's `EnumFormat` announces a single fixed format -
S16LE, 2 channels (FL, FR), 48000 Hz - so PipeWire's `audioconvert` filter handles
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
| `mpris:length` | `durationMs` (µs → ms) |

and these player properties:

| MPRIS2 property | AriaCast field |
|---|---|
| `PlaybackStatus` | `isPlaying` (`false` when `Paused`/`Stopped`) |
| `Position`, `Rate` | `positionMs` |

`isPlaying` is sent only when it changes (and once after each connect), not
with every periodic update. Receivers act on it: Music Assistant starts
playback when a sender reports `true` and drops the AriaCast source when it
reports `false`. Repeating a stale `true` while the receiver is pausing would
make it restart playback and then lose the source, so play from the receiver
would no longer reach the sender.

MPRIS players don't signal position changes, so the module estimates the
position between updates (last known position + elapsed time × `Rate`) and
corrects it from `Seeked` signals and by re-reading `Position` every 5 s
while playing. While playing, metadata is posted every second so the
receiver's progress display stays current.

Only signals for the `org.mpris.MediaPlayer2.Player` interface are used, and
only from the *active* player: the one that most recently reported
`PlaybackStatus` = `Playing` (or the first player seen, until then). When a
player becomes active, its full `Metadata` is fetched once. This keeps an
idle player (e.g. a paused browser tab) from overwriting the metadata of the
one that is actually playing. It is a heuristic: MPRIS does not say which
PipeWire stream a player feeds, so two players playing at the same time
still compete.

Metadata is posted to the server only while connected (immediately on
change, and every 10 s).

Empty-string values are treated as "not present", since some players send
empty strings for fields they have cleared rather than omitting them.

A new track is detected when `mpris:trackid` or `xesam:title` changes; the
title covers players that reuse one track ID for all media. On a new track,
the position restarts at 0, and album and artwork are cleared unless the
update provides new ones, so stale cover art isn't shown. The track ID is
also the target for seeks.

`Metadata` always carries the player's complete set of fields, so an update
without `mpris:length` (e.g. a livestream) clears the duration.

### Playback control from the receiver

`/control` commands from the AriaCast receiver are forwarded to the active
MPRIS player (`action` is matched case-insensitively):

| `/control` message | MPRIS2 call |
|---|---|
| `{"action":"play"}` | `Play` |
| `{"action":"pause"}` | `Pause` |
| `{"action":"toggle"}` | `PlayPause` |
| `{"action":"stop"}` | `Stop` |
| `{"action":"next"}` | `Next` |
| `{"action":"previous"}` | `Previous` |
| `{"action":"seek","position_ms":N}` | `SetPosition(trackid, N)`, or a relative `Seek` if the player has no usable track ID |

`isPlaying` is not toggled by the command itself; it follows the player's
`PlaybackStatus` once the player has reacted. Without an active MPRIS player,
commands are logged and ignored.

Normally the module disconnects from the receiver after 3 s of silence.
While the active MPRIS player is `Paused`, it stays connected instead, so a
later `play` from the receiver still reaches the player. This keep-alive
ends after `ariacast.pause.keepalive` seconds of silence (default `600`;
`0` disables it), or as soon as the player is no longer paused (stopped,
closed) and the sink is silent.

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
    ├── pw_stream  media.class=Audio/Sink  (RT thread, never blocks)
    │     on_process()        ──► silence detection ──► want_connect (atomic)
    │                          ──► push_audio() ──► ring_push() (try-lock,
    │                              drops the frame if full/busy)
    │     on_state_changed()  ──► UNCONNECTED/ERROR ──► want_connect=false
    │
    ├── thr_watchdog   every 250 ms: clears want_connect after AC_IDLE_MS
    │                  of silence (no audio in on_process), or after
    │                  ariacast.pause.keepalive while the player is paused
    │
    ├── thr_connmgr   runs UDP discovery once (if no host configured),
    │     │               then signals the main loop to create the sink
    │     ├── waits for want_connect=true
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
    │               → command queue → thr_mpris → MPRIS method call
    ├── thr_stats   ws_recv_frame(/stats)   → pw_log_debug()
    ├── thr_meta    HTTP POST /metadata every 10 s (1 s while playing)
    │               + on change (while connected)
    └── thr_mpris   private DBus session connection → MPRIS2
                    PropertiesChanged/Seeked (active player) → meta update,
                    executes queued /control commands
```

### Threading and shutdown

- `on_process()` runs on PipeWire's real-time data thread. It uses only
  atomics, `pthread_mutex_trylock()` and `memcpy()`; when the send buffer
  is full (network stall) frames are dropped and counted, never waited for.
- Sockets live in `conn_t`. A thread using an fd holds a reference;
  `conn_close()` calls `shutdown()` (which wakes blocked `read()`/`send()`),
  waits for all references to drop and only then `close()`s the fd, so an
  fd number is never reused under a running I/O call.
- The wake pipe is written once at shutdown and never drained, so every
  interruptible sleep (backoff, discovery, polling) returns immediately and
  the module unloads without waiting for timeouts.
- Worker threads are started last in `pipewire__module_init()`; if any
  setup step fails, `teardown()` stops everything and frees all state, so
  nothing keeps running inside the daemon after a failed load.

`pw_context_connect_self()` creates an internal client connection used for
the `pw_stream`. During a full daemon shutdown (`pw_context_destroy()`),
PipeWire may reclaim this connection (and the stream/proxy objects bound to
it) *before* invoking this module's `.destroy` hook. A `pw_proxy` listener
on the core connection (`core_proxy_destroy`) detects this: it unlinks its
own hook right away (the proxy is freed after the event) and makes
`teardown()` skip `pw_stream_destroy()`/`pw_core_disconnect()`. On a normal
module unload the listener does not fire and the module tears down its
objects itself.

## Known limitations / TODO

- No TLS (`wss://`) — spec security model targets trusted LAN only.
  Frames from the server are limited to 64 KiB.
- The connection to the receiver (including `/control`) exists only while
  audio is flowing, or while the active MPRIS player is paused (up to
  `ariacast.pause.keepalive`, default 10 min). After that, a `play` from the
  receiver can't reach the module — resume on the computer instead.
- `/stats` data is logged via `pw_log_debug` but not exposed as PipeWire
  node properties

## License

MIT
