# Architecture

## Overview

Motor Characterization Test Bench is a Python application for replaying and analyzing motor controller test data. It synchronizes data from three instruments (motor controller, torque sensor, PSU) and executes a YAML-driven test state machine.

## Directory Structure

```
├── main.py                      # Entry point (CLI or GUI)
├── requirements.txt             # Python dependencies (PyYAML, matplotlib, pytest)
├── requirements_system.txt      # System-level dependencies (apt packages)
├── README.md                    # Project overview and usage
├── ARCHITECTURE.md              # This file
├── config/
│   ├── motor_protocol.yaml      # Binary packet structure definition
│   └── test_config.yaml         # Test phases, safety limits, data columns
├── drivers/
│   ├── __init__.py              # Module exports
│   ├── motor.py                 # MotorDataSource, MotorBinaryReader, MotorCSVReader
│   ├── sensor.py                # SensorDataSource, SensorCSVReader
│   └── psu.py                   # PSUDataSource, PSUCSVReader
├── automation/
│   ├── __init__.py              # Module exports
│   ├── state_machine.py         # Test phase execution
│   ├── synchronization.py       # Multi-rate data alignment
│   └── safety.py                # Safety threshold checks
├── ui/
│   ├── __init__.py
│   └── app.py                   # Tkinter GUI application
├── tests/
│   ├── __init__.py
│   ├── conftest.py              # Shared fixtures and packet builder
│   ├── README.md                # Test documentation
│   ├── test_binary_parser.py
│   ├── test_csv_parsers.py
│   ├── test_state_machine.py
│   ├── test_synchronization.py
│   └── test_yaml_construction.py
├── data/                        # Input data files (.bin, .csv)
├── output/                      # Generated output CSV
├── logs/                        # All log files (see note below)
│   ├── AI_LOG.md                # AI tool usage log
│   └── run.log                  # Runtime log (generated at startup)
└── spec/
    └── Tool_writer_home_assignment.pdf
```

> **Note on `logs/` placement:** The assignment spec places `AI_LOG.md` at the repository root. Here it is kept under `logs/` together with `run.log`, since both are log artifacts and grouping them in one directory is cleaner than scattering log files at the root level.

## Driver Classes

Each driver module provides classes for the evaluation harness:

```python
# Import classes from drivers module
from drivers import MotorDataSource, SensorCSVReader, PSUCSVReader

# Load motor data (auto-detects CSV vs binary)
motor = MotorDataSource(path, test_config, motor_protocol)
data, errors = motor.load()

# Load sensor data
sensor = SensorCSVReader(path, test_config)
data, errors = sensor.load()

# Load PSU data
psu = PSUCSVReader(path, test_config)
data, errors = psu.load()
```

Each class has:
- `__init__(filepath, config, ...)` - Store file path and config
- `load()` - Parse file, return `(data, errors)` tuple
- `data` property - Access loaded data (auto-loads if needed)
- `errors` property - Access loading errors

## Threading / Concurrency Model

The application uses a **producer-consumer pattern** with thread-safe queues to ensure GUI responsiveness during long-running operations.

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        MAIN THREAD (GUI)                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ File Picker │  │   Buttons   │  │   Matplotlib Canvas     │  │
│  │   Dialogs   │  │ Start/Abort │  │   (plot updates)        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│                           │                    ▲                │
│                           │                    │                │
│                    ┌──────▼──────┐      ┌──────┴───────┐        │
│                    │ Start Test  │      │ _poll_updates│        │
│                    │   Button    │      │  (20 Hz)     │        │
│                    └──────┬──────┘      └──────▲───────┘        │
└───────────────────────────┼────────────────────┼────────────────┘
                            │                    │
                            │              update_queue
                            │         (thread-safe Queue)
                            │                    │
┌───────────────────────────▼────────────────────┼────────────────┐
│                    PLAYBACK THREAD (daemon)                     │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  1. synchronize_data()                                   │   │
│  │  2. run_current_ramp_phase()  ──┐                        │   │
│  │  3. run_torque_hold_phase()     ├─► Playback with timing │   │
│  │  4. run_voltage_decrease_phase()┘                        │   │
│  │  5. run_complete_phase()        ──► Write output CSV     │   │
│  └──────────────────────────────────────────────────────────┘   │
│                              │                                  │
│                              ▼                                  │
│                    update_queue.put(...)                        │
│                    - ('phase', 'CURRENT_RAMP')                  │
│                    - ('progress', 'Sample 100/5000')            │
│                    - ('plot', None)                             │
│                    - ('summary', {...})                         │
│                    - ('done', None)                             │
└─────────────────────────────────────────────────────────────────┘
```

### Thread Responsibilities

| Thread              | Responsibilities                          | Blocking Operations                  |
|---------------------|-------------------------------------------|--------------------------------------|
| **Main Thread**     | GUI event loop, widget updates, plotting  | File dialogs (~1s for typical files) |
| **Playback Thread** | Sync, state machine, playback timing      | Output CSV I/O, `time.sleep()`       |

### Synchronization Mechanisms

1. **`threading.Event`** - Control flags for playback state:
   - `playback_running`: Set when test is active
   - `abort_requested`: Set when user clicks Abort button

2. **`queue.Queue`** - Thread-safe message passing:
   - Playback thread produces update messages
   - Main thread consumes and applies to GUI at 20 Hz

3. **`root.after(50, callback)`** - Non-blocking timer for GUI polling:
   - Polls `update_queue` every 50ms (20 Hz)
   - Never blocks the main event loop

### Message Types

| Message             | Producer        | Consumer Action          |
|---------------------|-----------------|--------------------------|
| `('phase', str)`    | Playback thread | Update phase label       |
| `('progress', str)` | Playback thread | Update progress label    |
| `('plot', None)`    | Playback thread | Redraw matplotlib canvas |
| `('summary', dict)` | Playback thread | Populate summary panel   |
| `('error', str)`    | Playback thread | Show error dialog        |
| `('done', None)`    | Playback thread | Re-enable Start button   |

### File Loading

Currently, file loading (`_load_motor_data`, `_load_sensor_data`, `_load_psu_data`) runs on the **main thread**. This is acceptable because:
- Files are typically small (< 1MB)
- Loading completes in < 1 second
- User must wait for load to complete before starting test anyway

For very large files, this could be moved to a background thread with a loading indicator.

### Playback Speed Control

Playback respects data timestamps with adjustable speed:

```python
# Calculate delay based on playback speed
elapsed_data_time = sample_ts - start_ts
if playback_speed > 0:
    target_real_time = elapsed_data_time / playback_speed
    # Sleep if ahead of schedule
    time.sleep(max(0, target_real_time - current_real_time))
```

| Speed | Behavior                                           |
|-------|----------------------------------------------------|
| 1×    | Real-time (1 second of data = 1 second wall clock) |
| 5×    | 5× faster (1 second of data = 0.2 seconds)         |
| 10×   | 10× faster                                         |
| Max   | No delays, process as fast as possible             |

## Data Flow

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│ Motor File   │   │ Sensor File  │   │  PSU File    │
│ (.bin/.csv)  │   │   (.csv)     │   │   (.csv)     │
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │                  │                  │
       ▼                  ▼                  ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│ load_motor_  │   │ load_sensor_ │   │ load_psu_    │
│ data()       │   │ csv()        │   │ csv()        │
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │                  │                  │
       └────────┬─────────┴─────────┬────────┘
                │                   │
                ▼                   │
        ┌───────────────┐           │
        │ synchronize_  │◄──────────┘
        │ data()        │  (nearest-prior join)
        └───────┬───────┘
                │
                ▼
        ┌───────────────┐
        │ State Machine │
        │ Phases        │
        └───────┬───────┘
                │
                ▼
        ┌───────────────┐
        │  output.csv   │
        └───────────────┘
```

## Safety Model

Safety checks run synchronously within the playback thread:

1. **Before each sample**: `check_safety_limits()` validates:
   - `|torque| <= max_torque_nm` (200 Nm)
   - `commanded_current <= max_current_a` (34 A)

2. **On violation**: Immediate transition to COMPLETE phase with logged reason.

3. **Manual abort**: User clicks Abort → `abort_requested.set()` → playback loop checks flag → graceful transition to COMPLETE.

## YAML-Driven Design

All data structures and test parameters are defined in YAML:

- **motor_protocol.yaml**: Binary packet framing, field types, response/command definitions
- **test_config.yaml**: Phase parameters, safety thresholds, output columns, synchronization method

Parsers read these files at runtime and build unpacking logic dynamically. No field names or packet structures are hardcoded.
