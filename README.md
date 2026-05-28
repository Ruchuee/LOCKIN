# LOCKIN

LOCKIN is a Valorant agent instalocker. Pick your agent, arm it from the dashboard, and LOCKIN automatically locks that agent when match is found.

## Requirements

- Windows
- Valorant running

## Run

Download the latest `.exe` from the [releases page](https://github.com/Ruchuee/LOCKIN/releases/latest), and run it.

For development runs from the project directory:

```powershell
cargo run --release
```

Or build the binary:

```powershell
cargo build --release
.\target\release\LOCKIN.exe
```

When LOCKIN starts, it prints the dashboard URL:

```text
LOCKIN dashboard: http://127.0.0.1:8787
```

If port `8787` is busy, LOCKIN tries the next local ports automatically.

## Basic Usage

1. Start Valorant and reach the game menu.
2. Start LOCKIN.
3. Open the printed dashboard URL.
4. Choose an agent.
5. Optional: choose allowed maps.
6. Choose detection mode.
7. Press **Arm**.

When a pre-game lobby is detected, LOCKIN resolves the match, checks your map filter, then sends the lock request.

## Dashboard Controls

- **Agent list**: choose the agent LOCKIN should lock.
- **Maps**: choose maps where LOCKIN is allowed to lock. No selected maps means all maps are allowed.
- **Select before lock**: sends a select request before the lock request.
- **Auto re-arm**: after a terminal outcome, waits until game is over and arms again.
- **Detection mode**:
  - `websocket`: listens to Riot Client local websocket presence updates.
  - `polling`: checks the pre-game player endpoint repeatedly.
  - `hybrid`: uses websocket detection with a polling fallback.
- **Arm**: starts live detection.
- **Disarm**: stops detection and clears runtime state.
- **Event Log**: shows recent runtime events, warnings, and errors.

## Runtime States

Common states shown in the dashboard:

- `idle`: not armed.
- `arming`: refreshing Riot live data.
- `detecting`: waiting for a pre-game lobby.
- `resolving_match`: pre-game was detected and LOCKIN is resolving match data.
- `locking`: match data is ready and LOCKIN is sending select/lock requests.
- `locked`: selected agent is locked or already locked.
- `locked_other_agent`: your player is already locked on a different agent.
- `skipped_map`: the lobby map is not in your selected map list.
- `waiting_menus`: waiting for Valorant to return to menus before auto re-arm.
- `warning` / `error`: a recoverable warning or hard failure occurred.

## Configuration

LOCKIN stores settings in `lockin-config.json` next to the executable during normal runs, or in the project root when run through Cargo.

Example:

```json
{
  "selected_agent_uuid": null,
  "selected_map_urls": [],
  "select_before_lock": false,
  "auto_rearm": true,
  "poll_ms": 250,
  "detection_mode": "hybrid"
}
```

Most users should change settings through the dashboard instead of editing JSON manually.

## Troubleshooting

- **Valorant session was not found**: start Valorant before pressing **Arm**.
- **Riot lockfile errors**: make sure Riot Client is running.
- **No agents or maps shown**: check internet access; public data is fetched at startup.
- **Websocket detection misses a lobby**: switch to `hybrid` or `polling`.
- **Lock did not happen immediately after pre-game**: Riot endpoints can lag behind presence. Try to switch to `websocket` or `hybrid` modes.
- **Wrong map skipped**: clear map selection or verify the map is selected in the dashboard.

## Development

Useful commands:

```powershell
cargo test
cargo build --release
```

LOCKIN runs on your machine and reads Valorant local state through Riot Client local APIs.

Important files:

- `src/main.rs`: startup, config location, dashboard binding.
- `src/app.rs`: app state, config, UI phase model.
- `src/detection.rs`: detection modes, lock sequence, auto re-arm flow.
- `src/riot.rs`: Riot local/auth/API/websocket helpers.
- `static/index.html`: dashboard UI.

The dashboard API is local-only and served on `127.0.0.1`. State updates are streamed with server-sent events from `/api/stream`.
