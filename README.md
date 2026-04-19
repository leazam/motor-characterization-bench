# Motor Characterization Test Bench

A Python GUI application for replaying and analyzing BLDC motor test data. Synchronizes three instrument streams, runs a YAML-driven state machine, and produces a unified output log.

> See [ARCHITECTURE.md](ARCHITECTURE.md) for the threading model, data flow diagram, and design decisions.

---

## Installation

**Requires Python 3.6+**

```bash
# System deps (Linux)
sudo apt install python3-tk python3-matplotlib

# Python deps
pip install -r requirements.txt
```

---

## Usage

**GUI (default)**
```bash
python3 main.py
```

**CLI (headless / batch)**
```bash
python3 main.py --not-gui \
    --motor-data  data/test_motor_1000hz.bin \
    --sensor-data data/test_sensor_4800hz.csv \
    --psu-data    data/test_psu_10hz.csv \
    --output      output/result.csv \
    --verbose 1
```

| Flag            | Description                                    |   
|-----------------|------------------------------------------------|
| `--not-gui`     | Run without GUI                                |
| `--motor-data`  | `.bin` or `.csv` motor file                    |
| `--sensor-data` | Torque sensor CSV                              |
| `--psu-data`    | PSU CSV                                        |
| `--output`      | Output CSV path (default: `output/output.csv`) |
| `--verbose`     | `0`=warnings, `1`=info, `2`=debug              |

---

## Data Files

Place files in `data/`:

| File                     | Rate     | Description |
|--------------------------|----------|-------------------------|
| `test_motor_1000hz.bin`  | 1,000 Hz | Binary motor telemetry  |
| `test_motor_1000hz.csv`  | 1,000 Hz | Same data pre-decoded   |
| `test_sensor_4800hz.csv` | 4,800 Hz | Torque sensor readings  |
| `test_psu_10hz.csv`      | ~10 Hz   | PSU voltage and current |

---

## Tests

```bash
pytest tests/ -v
```

104 tests covering binary parsing, CSV parsing, YAML-driven construction, data synchronization, and state machine transitions.

---

## Project Structure

```
config/       # motor_protocol.yaml, test_config.yaml  (do not modify)
drivers/      # MotorDataSource, SensorCSVReader, PSUCSVReader
automation/   # State machine, synchronization, safety checks
ui/           # Tkinter GUI + live matplotlib plots
tests/        # Unit tests
data/         # Input data files
output/       # Generated output CSV
```

All protocol details, packet layouts, phase parameters, and safety thresholds are read from the YAML files at startup — nothing is hardcoded.
