# AI Usage Log

This document records all AI tool usage during development, highlighting manual corrections to AI-generated logic.

---

## Session: April 17, 2026

### Tool Used: GitHub Copilot (Claude Opus 4.5)

---

### 1. Manual Correction - CLI Arguments Enhancement

**Description:** Modified the argument parser to add shorter argument options, make arguments optional with defaults, and add a no-GUI flag.

**File name:** main.py

**Action:** 
- Added short options (e.g., `-mp` for `--motor-protocol`, `-tc` for `--test-config`, `-md` for `--motor-data`, etc.)
- Changed `required=True` to `required=False` for most arguments
- Added `default=Path(...)` with sensible default paths
- Added `-ng`/`--not-gui` flag to run the application without GUI

---

### 2. Manual Correction - Hardcoded Little Endian Byte Order

**Description:** AI hardcoded 'little' endian in multiple places instead of reading byte order from YAML configuration.

**File name:** drivers/motor.py

**Action:** 
- AI originally hardcoded `byteorder='little'` in `int.from_bytes()` for checksum comparison
- AI also initially only handled little endian in header format prefix
- Changed to read `byte_order` from `protocol['protocol'].get('byte_order')` in YAML
- Created `byte_order_python` variable that converts YAML value ('little_endian'/'big_endian') to Python's format ('little'/'big')
- Now supports both big and little endian based on YAML configuration

---

### 3. Manual Correction - Jitter Handling in Data Synchronization (Conservative Approach)

**Description:** AI implemented jitter handling incorrectly by ADDING jitter margin to motor timestamp, when it should SUBTRACT to ensure safety.

**File name:** automation/synchronization.py

**Problem:**
- AI initially implemented: `effective_target = target_ts + jitter_margin_s`
- This would accept sensor samples with timestamps slightly AFTER motor_ts
- Risk: A sensor sample with recorded timestamp 0.9998s could have ACTUALLY been captured at 1.0003s (due to +0.5ms jitter) - using it as "prior" would be WRONG

**Correction:**
- Changed to: `effective_target = target_ts - jitter_margin_s`
- Only accept samples with timestamp <= (motor_ts - jitter_margin)
- Example: Motor ts=1.000s, jitter=0.5ms → effective target=0.9995s
  - Sensor at 0.999s → ACCEPT (even with worst-case +0.5ms jitter = 0.9995s, still prior)
  - Sensor at 0.9998s → REJECT (with +0.5ms jitter = 1.0003s, could be AFTER motor!)

**Lesson:** When handling jitter, prefer MISSING valid samples over USING samples that might actually be from the future. Conservative approach ensures data integrity.

---

### 4. Manual Correction - Safety Checks vs Phase Transition Current Logic

**Description:** AI confused which type of current to check for safety limits versus phase transitions.

**File name:** automation/safety.py

**Problem:**
- AI implemented safety check using MEASURED current (what the motor reports)
- AI used the same current field for both safety and phase transition checks

**PDF Requirements:**
- **Safety check**: Commanded current must never exceed max_current_a → abort to COMPLETE
- **Phase transition**: |measured_current| >= max_current_a → transition to TORQUE_HOLD

**Correction:**
- Safety check (`check_safety_limits`): Now checks `commanded_current_a` (what we tell the motor)
- Phase transition (`check_phase_transition_current_ramp`): Checks `motor_measured_current_a` (motor's response)

**Lesson:** Safety limits apply to what we COMMAND (our control), phase transitions react to what we MEASURE (motor's behavior). These are fundamentally different - commanded current is our output, measured current is the motor's feedback.