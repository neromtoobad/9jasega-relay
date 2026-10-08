# 9ja Sega relay

A tiny WebSocket relay that pairs two phones for online play in [9ja Sega](https://9jasega.nerom.site) (Biro Game, Cork Ball, Ayo, Gbas Gbos).

- `GET /health` → `{ ok, rooms, served }`
- `WS /ws` messages: `create {game}`, `join {code, game}`, `rejoin {code, token}`, `msg {d}`, `ping {id}`, `leave`.
  The server answers `room {code, slot, token}`, `peer {on, left?, back?}`, `msg {d}`, `pong`, `error {code}`.

Rooms have 4-letter codes, two slots, a 2-minute rejoin grace, and are cleaned up after 45 minutes idle. The relay knows nothing about game rules.

```
npm install && npm start   # PORT defaults to 8787
```
