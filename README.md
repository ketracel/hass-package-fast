# `package_fast` detector and Home Assistant shell

This directory contains the pure, offline-testable detector core and its HACS-ready Home Assistant custom-integration shell. The canonical core has no Home Assistant imports, network calls, or asyncio requirement. `Detector.step()` performs synchronous in-memory work; the shell owns polling, event emission, retention, metrics, and privacy-gated sparse JPEG persistence.

The binding design is [CONVERGED.md](../../.dual-lane-pkgfast-20260827/converged/CONVERGED.md), especially Architecture — FSM, Data model, §5c, and §5h, as amended by [ERRATA.md](../../.dual-lane-pkgfast-20260827/converged/ERRATA.md) ERR-06, ERR-07, ERR-08, and ERR-11.

## Publication boundary

`step()` returns staged `DetectionEnvelope` objects. They are not safe to expose as HA events until the journal returns them after append, flush, and `fsync`:

```python
from homeassistant.package_fast.core import Detector, Journal

detector = Detector()
journal = Journal("/media/package_fast", frame_persistence_enabled=False)

staged = detector.step(frame, signals)
durable = journal.commit(staged)
for envelope in durable:
    # The shell may project or emit only these durable envelopes.
    pass
```

On a journal error, `commit()` truncates the uncommitted tail and returns an empty list. The canonical store is only `episodes.jsonl`; per-episode `meta.json` is a derived pretty-print written at close. `Journal.persist_frame()` invokes a caller-supplied callback only when `frame_persistence_enabled=True`; the default is off for the ERR-08 privacy gate. The core never writes JPEGs.

## Owner install path (not performed by this work order)

The owner first publishes this `homeassistant/package_fast/` directory as the root of a HACS custom repository, preserving `hacs.json` and `custom_components/package_fast/`. Publishing or adding that repository is an owner action; this work order does not contact GitHub or the live HA instance.

After publication, in Home Assistant:

1. Open HACS → Integrations → Custom repositories, add the published repository URL as category **Integration**, and download **Package Fast**.
2. Restart Home Assistant, then open Settings → Devices & services → Add integration → **Package Fast**. The flow is credential-free and single-instance.
3. Confirm the default camera and 0.5 Hz idle / 2 Hz armed rates in Configure. Leave **Persist sparse frames** off.
4. Confirm `switch.package_fast_detector`, `sensor.package_fast_heartbeat`, and the other entities below appear before considering the separately versioned watchdog automation for deployment.

The HACS payload contains a byte-identical vendored copy of `core/` because HACS installs only `custom_components/package_fast/`. `test_shell_logic.py` pins that copy to the accepted core so shell packaging cannot silently fork the frozen contract.

## Runtime contract

The shell lives in `custom_components/package_fast/`:

- one bounded frame slot and one sequential fetch → executor decode/`step()`/`commit()` → sleep loop; fetches never overlap or accumulate;
- `camera.async_get_image` with a one-second budget and exactly one retry;
- state-edge envelopes from both person binary sensors and the package-detection master;
- `package_fast_shadow_v1` for every durable envelope, and `package_fast_confirmed_v1` only for an announce-eligible durable detection while both the master and `input_boolean.package_fast_promoted` are on;
- no notification, Alexa, latch, or device action in the integration. Consumers remain separate, and none are shipped here;
- Phase-0 SLO monitoring, `feed_suspect`, suspension, a heartbeat that deliberately stales while suspended, and clean-window recovery;
- a 60-second cold-start stabilization interval at idle cadence; person edges during that interval are discarded, then any currently active person state is re-seeded before fast decisions resume;
- 30-second projection cadence for heartbeat/fetch/cpu and the other polling metrics, avoiding per-frame recorder churn while state transitions and durable results still publish immediately;
- canonical per-episode poll-gap counts in each `episode_closed.poll` block, plus ERR-11 `system_log`/blocking-call observations in the noncanonical `/media/package_fast/metrics/health.jsonl` soak journal;
- admin-gated `package_fast/journal` and `package_fast/health` WebSocket commands for authenticated journal export and diagnosis, with every disk read dispatched to the executor;
- passive shadow-phase joining of the existing early/final Sol decisions: per-lane call-counter edges infer the lane, `input_text.package_detection_last_decision` supplies the result, and a timestamp match commits `sol_result` to the open or newest recent episode; and
- independent daily HA-start and interrupted-episode counters, clean `homeassistant_stop` teardown, and shadow publication of any `interrupted_restart` closure recovered at startup.

The 0.8 distinct-fps freshness floor is Phase-0-seeded from the snapshot path's measured ~1.0 distinct frame/s; the camera attribute's 2 FPS is not what that path delivers.

Public entities are:

- `binary_sensor.package_fast_deposit` — momentary view of the latest durable deposit result;
- `sensor.package_fast_heartbeat`, `sensor.package_fast_fetch_p95_ms`, `sensor.package_fast_state`, and `sensor.package_fast_cpu_ms_per_frame_p95`;
- daily poll-gap, duplicate, detection, suspension, HA-start, interrupted-restart, system-log-warning, and frame-write-skip sensors;
- `switch.package_fast_detector` — fast-specific poller switch. The poller also idles whenever `input_boolean.package_detection_enabled` is off.

`input_boolean.package_fast_promoted` is deliberately external control-plane state, not created or toggled by this integration. Missing/off means shadow-only.

## Authenticated read surfaces

Version 0.2.1 provides two read-only WebSocket commands. Both require an authenticated Home Assistant administrator, accept no filesystem path or media identifier, and read only the configured runtime's own store:

- `package_fast/journal` accepts optional `since_seq`, optional `episode_id`, and `limit` (default 500, maximum 5,000). Each call examines at most `limit` physical candidates after the cursor and returns the matching, structurally valid additive ERR-07 envelopes plus `next_seq`, `truncated`, and `skipped`. Because envelope `seq` restarts per episode, `since_seq`/`next_seq` are an exclusive file-order cursor; each returned envelope keeps its per-episode `seq` unchanged. Unknown record types and schema versions are returned when their envelope structure is valid. A malformed complete line increments `skipped` and blocks the cursor at that line instead of consuming it.
- `package_fast/health` accepts only an optional bounded health-note `limit`. It returns current FSM/suspension state, up to 50 suspension reasons found in the bounded recent-note window (with `recent_suspensions_complete` saying whether that bounded scan reached the full requested history), active suppression masks with normalized boxes/creation times/hit counts/remaining TTL, the latest fetch/freshness/error/poll-gap SLO snapshot, and the requested tail of health notes. `system_log` sources are reduced to `basename:lineno`; messages remain third-party diagnostic text and are truncated when captured.

Neither command prunes masks, invokes the journal reducer, rewrites derived files, or exposes a caller-selected path. Both readers snapshot the file size, ignore any non-newline-terminated append, and do bounded page/tail work without taking the detector's per-frame lock. JSON journal/health data travels over the authenticated WebSocket; sparse JPEGs remain on Home Assistant's authenticated local-media path for export. Boolean limits are rejected, and both schemas enforce their hard maxima before dispatching to the executor.

## ERR-08 privacy gate and retention

`persist_frames` ships **false**. Journal records, derived metrics, health notes, and entities continue regardless, but no camera frame bytes are written until the owner explicitly changes the option. Before doing so, P2-PRIV must pass in full:

1. unauthenticated LAN requests through every candidate `/media/package_fast/` route return only 401/404, never 200;
2. the owner records the remote-access posture and verifies every remote media route requires HA authentication; and
3. backup scope is established; if `/media` enters an off-LAN backup, exclude `/media/package_fast/` or obtain explicit owner sign-off.

When enabled after that gate, the shell stores baseline, first-seen, confirm, decision crop, per-trip, and final frames under `episodes/YYYY/MM/DD/<episode_id>/`. Defaults are 256 MB and 3 days: HA's nightly backup includes `/media`, so this is deliberately a short-lived spool while `package_eval.py export-shadow` archives it in the corpus on razorback. Pruning is oldest-first; an episode directory containing a `keep` marker is exempt. If only exempt/fixed data remains at the cap, the write is skipped and counted instead of deleting labeled media or suspending the detector. A missing frame-cache entry is handled the same way. Only a distinct real filesystem failure takes the fatal suspension path.

## Modules

- `core/envelopes.py` defines the frame, signal, and staged detection/lifecycle contracts with dual clocks.
- `core/config.py` holds every field-math, FSM, cadence, and margin value and derives `config_digest` from canonical JSON.
- `core/pipeline.py` implements 2× grayscale field math, reference-pixel normalization, illumination and motion guards, morphology, Sobel edge budgets, component polarity, IoU/SAD stationarity, and REBASE-only edge-profile shift estimation. NumPy is optional; Pillow is the complete fallback.
- `core/detector.py` implements the DISABLED/IDLE/ARMED/EPISODE_OPEN/DETECTED/CLOSING/CLOSED/REBASE/SUSPENDED FSM, distinct-hash confirmation, frozen-baseline selection, person context, two output tiers, moved/removal handling, and timing quarantine.
- `core/journal.py` implements the ERR-07 typed writer, deterministic reducer, restart recovery, derived `meta.json`, byte accounting, and frame-write/skip hooks.
- `custom_components/package_fast/shell_logic.py` contains HA-free SLO, retention, suppression-mask, feed-suspect, and system-log filtering policy.
- `custom_components/package_fast/runtime.py` owns the HA listeners, bounded poller/worker, executor boundary, durable event publication, metrics, and clean unload.
- `custom_components/package_fast/storage.py` owns sparse frame/crop writes, `keep`-aware retention, daily metrics, health notes, and restart-persistent suppression policy.
- `custom_components/package_fast/paging.py` defines HA-free, bounded JSONL paging, health-tail redaction, and the lossless journal cursor.
- `custom_components/package_fast/websocket.py` registers the two admin-gated read commands and dispatches their disk work to the executor.
- `tests/synth.py` provides fixed-seed 480×360 scenes, parcels, couriers, soft shadows, illumination changes, IR flips, camera shifts, and sub-threshold sensor variation.
- `tests/test_scenarios.py` is the complete S1–S14 sequence suite; the other test modules pin primitives, FSM boundaries, journal failure behavior, shell policies, and vendored-core parity.

## Run offline

Required: Python and Pillow. NumPy is optional. The verified local runtime is `$PACKAGE_CORPUS_ROOT/.venv-tools`: Python 3.14.6 with Pillow 12.3.0 (and NumPy 2.5.2 for the accelerated run). The host's system Python interpreters do **not** have Pillow, so no system-interpreter pass is claimed. Python 3.12.13 remains only the verified floor from the sandbox run (Pillow 12.3.0, NumPy 2.3.5); no claim is made for older interpreters.

From the repository root, with the desired interpreter/environment on `PATH`:

```sh
python3 -m unittest discover -v homeassistant/package_fast/tests
```

To prove the Pillow path while using an environment that also has NumPy, run discovery behind an import guard:

```sh
python3 -c $'import importlib.abc\nimport sys\nimport unittest\nclass BlockNumpy(importlib.abc.MetaPathFinder):\n    def find_spec(self, fullname, path=None, target=None):\n        if fullname == "numpy" or fullname.startswith("numpy."):\n            raise ModuleNotFoundError("numpy blocked for Pillow fallback test", name=fullname)\n        return None\nsys.meta_path.insert(0, BlockNumpy())\nsuite = unittest.defaultTestLoader.discover("homeassistant/package_fast/tests")\nresult = unittest.TextTestRunner(verbosity=2).run(suite)\nraise SystemExit(0 if result.wasSuccessful() else 1)'
```

Both commands are offline and use no HA state.

## HA-coupled validation boundary

There is intentionally no `pytest-homeassistant-custom-component` dependency in this work order. `shell_logic.py`, retention decisions, core parity, and the frozen core run offline; HA-coupled setup, camera fetch behavior, entity registration/restoration, event-bus delivery, system-log subscription, executor/blocking-call behavior, unload, and nightly restart recovery are qualified by the ERR-11 live co-residency soak after owner installation. The versioned `automation_package_fast_watchdog.json` is not deployed here; it provides the Pixel heartbeat-age check once the owner deploys and verifies it.

## Explicit interpretations

The design fixes the structural thresholds but describes “strong margins” qualitatively. The defaults make that tier reproducible as edge gain ≥0.22, stability SAD ≤6/255, and component area at least 0.001 inside either area bound. These are config values, not hidden constants, and remain shadow hypotheses pending the design’s promotion evidence.

The initial ROI is the full frame. G4 is the episode-opening person signal; G6 is arm/context only, matching the converged architecture’s “valid G4” wording. A cold detector with no quiet frame at least two seconds old may open the lifecycle record but makes no visual decision for that episode, preserving the fail-closed startup posture; the shell’s documented startup-stabilization period is expected to age the ring before normal signals are admitted.

ERR-07 supplies five canonical record types while §5c/§5h also require `moved_object`, `rebase`, and `camera_shift` to be journaled. They are encoded as non-announceable `detection` payloads with additive `kind` values, preserving the five-type envelope and persistence ordering. Reducer metrics retain unique `(episode_id, detection_id)` identity and separately report kind counts.

Because `sol_result` has no explicit result ID in ERR-07, its reducer idempotency key is interpreted as `(lane, decided_at_wall)`. Label disagreement is computed as the additive reduced value `disagree`; it is never stored in a journal payload.
