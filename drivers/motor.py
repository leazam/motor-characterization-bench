"""
Motor Data Parser.

Parses motor data from both CSV and binary formats.
- CSV: Column definitions from test_config.yaml
- Binary: Packet structure from motor_protocol.yaml

Error Handling:
- Empty file or missing headers: Raises error at startup
- Malformed rows/packets: Logs error, skips, continues
- Non-monotonic timestamps: Dismisses, logs warning, continues
- Binary checksum failure: Discards packet, increments counter, continues
- Binary missing start marker: Scans forward to resync
- Binary truncated packet: Discards partial, reports in summary
- Binary unknown message code: Skips payload, logs warning
"""

import csv
import struct
import logging
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

logger = logging.getLogger(__name__)


def detect_motor_file_type(filepath: Path) -> str:
    """
    Detect motor data file type based on extension.
    
    Args:
        filepath: Path to motor data file
        
    Returns:
        'bin' or 'csv'
        
    Raises:
        ValueError: If extension is not .bin or .csv
    """
    ext = filepath.suffix.lower()
    if ext == '.bin':
        return 'bin'
    elif ext == '.csv':
        return 'csv'
    else:
        logger.error(f"Unsupported motor data file type: '{ext}'. Only .bin and .csv are supported.")
        raise ValueError(f"Unsupported motor data file type: '{ext}'. Only .bin and .csv are supported.")


def load_motor_data(
    filepath: Path,
    test_config: Dict[str, Any],
    motor_protocol: Dict[str, Any] 
) -> Tuple[List[Dict[str, float]], List[str]]:
    """
    Load motor data from either CSV or binary file.
    
    Auto-detects file type from extension and calls appropriate loader.
    
    Args:
        filepath: Path to motor data file
        test_config: Parsed test_config.yaml
        motor_protocol: Parsed motor_protocol.yaml
        
    Returns:
        Tuple of (data, errors)
    """
    file_type = detect_motor_file_type(filepath)
    
    if file_type == 'csv':
        return load_motor_csv(filepath, test_config)
    else:
        return load_motor_bin(filepath, motor_protocol)


# ─────────────────────────────────────────────────────────────
# CSV Parser
# ─────────────────────────────────────────────────────────────

def load_motor_csv(filepath: Path, config: Dict[str, Any]) -> Tuple[List[Dict[str, float]], List[str]]:
    """
    Load motor CSV file into a list of dictionaries.
    
    Args:
        filepath: Path to motor CSV file
        config: Parsed test_config.yaml dictionary
        
    Returns:
        Tuple of (data, errors):
        - data: List of dicts with strictly increasing timestamps
        - errors: List of error messages for skipped rows
        
    Raises:
        ValueError: If file is empty or missing required headers
    """
    # Get column definitions from YAML
    columns = config['data_sources']['motor']['formats']['csv']['columns']
    column_names = [col['name'] for col in columns]
    
    # Find timestamp column
    timestamp_col = column_names[0]
    for name in column_names:
        if 'timestamp' in name.lower():
            timestamp_col = name
            break
    
    result = []
    errors = []
    
    with open(filepath, 'r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        
        # Check for empty file or missing headers
        if reader.fieldnames is None:
            raise ValueError(f"Empty CSV file: {filepath}")
        
        missing_cols = set(column_names) - set(reader.fieldnames)
        if missing_cols:
            raise ValueError(
                f"Missing required columns in {filepath}: {missing_cols}. "
                f"Expected: {column_names}, Found: {list(reader.fieldnames)}"
            )
        
        # Parse rows, skip malformed ones
        for line_num, row in enumerate(reader, start=2):
            try:
                # Check for missing fields
                missing_fields = [col for col in column_names if row.get(col) is None]
                if missing_fields:
                    error_msg = f"Line {line_num}: Missing fields {missing_fields}"
                    errors.append(error_msg)
                    logger.warning(error_msg)
                    continue
                
                # Convert values to float
                sample = {}
                for col in column_names:
                    try:
                        sample[col] = float(row[col])
                    except (ValueError, TypeError):
                        raise ValueError(f"Non-numeric value '{row[col]}' in column '{col}'")
                
                result.append(sample)
                
            except ValueError as e:
                error_msg = f"Line {line_num}: {e}"
                errors.append(error_msg)
                logger.warning(f"Skipping malformed row - {error_msg}")
                continue
    
    if not result:
        raise ValueError(f"No valid data rows in {filepath}")
    
    # Filter out non-monotonic timestamps
    filtered = []
    last_ts = float('-inf')
    
    for sample in result:
        ts = sample[timestamp_col]
        if ts > last_ts:
            filtered.append(sample)
            last_ts = ts
        else:
            error_msg = f"Non-monotonic timestamp {ts} (previous: {last_ts}) - skipped"
            errors.append(error_msg)
            logger.warning(error_msg)
    
    result = filtered
    
    if not result:
        raise ValueError(f"No valid data rows after filtering non-monotonic timestamps in {filepath}")
    
    if errors:
        logger.warning(f"Motor CSV: {len(errors)} rows skipped, {len(result)} valid rows loaded")
    else:
        logger.info(f"Motor CSV: {len(result)} rows loaded successfully")
    
    return result, errors


# ─────────────────────────────────────────────────────────────
# Binary Parser
# ─────────────────────────────────────────────────────────────

def load_motor_bin(filepath: Path, protocol: Dict[str, Any]) -> Tuple[List[Dict[str, float]], List[str]]:
    """
    Load motor binary file into a list of dictionaries.
    
    Builds unpacking logic from motor_protocol.yaml at runtime.
    
    Args:
        filepath: Path to motor binary file
        protocol: Parsed motor_protocol.yaml dictionary
        
    Returns:
        Tuple of (data, errors):
        - data: List of dicts with strictly increasing timestamps
        - errors: List of error messages for skipped packets
        
    Raises:
        ValueError: If file is empty or no valid packets found
    """
    # Build parser configuration from YAML
    framing = protocol['framing']
    types = protocol['types']
    
    # Build separate lookups for responses and commands (codes may overlap)
    messages_responses = {}
    messages_commands = {}
    if 'responses' in protocol:
        for r in protocol['responses']:
            messages_responses[r['code']] = r
    if 'commands' in protocol:
        for c in protocol['commands']:
            messages_commands[c['code']] = c
    
    # Extract framing info
    start_marker = bytes(framing['start_marker']['bytes'])
    end_marker = bytes(framing['end_marker']['bytes'])
    header_fields = framing['header']['fields']
    checksum_size = framing['checksum']['size']
    
    # Build header struct format and field names from YAML types
    header_format = ''
    header_size = 0
    header_field_names = []
    for field in header_fields:
        field_type = types[field['type']]
        header_format += field_type['format'].lstrip('<>')
        header_size += field_type['size']
        header_field_names.append(field['name'])
    
    # Prepend byte order from YAML
    byte_order = protocol['protocol'].get('byte_order', 'little_endian')
    if byte_order == 'little_endian':
        header_format = '<' + header_format
        byte_order_python = 'little'
    elif byte_order == 'big_endian':
        header_format = '>' + header_format
        byte_order_python = 'big'
    else:
        logger.warning(f"Unknown byte_order '{byte_order}', defaulting to little endian")
        header_format = '<' + header_format
        byte_order_python = 'little'
    
    result = []
    errors = []
    error_counts = {
        'checksum_failures': 0,
        'resync_events': 0,
        'truncated_packets': 0,
        'unknown_message_codes': 0,
        'non_monotonic': 0
    }
    
    with open(filepath, 'rb') as f:
        data = f.read()
    
    if not data:
        raise ValueError(f"Empty binary file: {filepath}")
    
    pos = 0
    last_ts = float('-inf')
    
    while pos < len(data):
        # Look for start marker
        if pos + len(start_marker) > len(data):
            # Not enough data for start marker
            error_counts['truncated_packets'] += 1
            errors.append(f"Truncated data at end of file (pos {pos})")
            logger.warning(f"Truncated data at end of file (pos {pos})")
            break
        
        if data[pos:pos + len(start_marker)] != start_marker:
            # Resync: scan forward for start marker
            error_counts['resync_events'] += 1
            next_pos = data.find(start_marker, pos + 1)
            if next_pos == -1:
                errors.append(f"No valid start marker found after pos {pos}")
                logger.warning(f"No valid start marker found after pos {pos}, stopping")
                break
            errors.append(f"Missing start marker at pos {pos}, resynced at {next_pos}")
            logger.warning(f"Missing start marker at pos {pos}, resynced at {next_pos}")
            pos = next_pos
            continue
        
        # Check if we have enough data for header
        header_start = pos + len(start_marker)
        if header_start + header_size > len(data):
            error_counts['truncated_packets'] += 1
            errors.append(f"Truncated header at end of file (pos {pos})")
            logger.warning(f"Truncated header at end of file (pos {pos})")
            break
        
        # Parse header into dict using field names from YAML
        header_data = data[header_start:header_start + header_size]
        header_values = struct.unpack(header_format, header_data)
        header = dict(zip(header_field_names, header_values))
        
        # Get payload_size and timestamp from header (using YAML field names)
        payload_size = header.get('payload_size', 0)
        timestamp_ms = header.get('timestamp_ms', 0)
        
        # Calculate total packet size
        packet_size = len(start_marker) + header_size + payload_size + checksum_size + len(end_marker)
        
        # Check if we have complete packet
        if pos + packet_size > len(data):
            error_counts['truncated_packets'] += 1
            errors.append(f"Truncated packet at pos {pos} (need {packet_size} bytes, have {len(data) - pos})")
            logger.warning(f"Truncated packet at pos {pos}")
            break
        
        # Extract payload
        payload_start = header_start + header_size
        payload_data = data[payload_start:payload_start + payload_size]
        
        # Verify checksum (XOR of all bytes from start marker to end of payload)
        checksum_pos = payload_start + payload_size
        expected_checksum = data[checksum_pos:checksum_pos + checksum_size]
        computed_checksum = 0
        for byte in data[pos:checksum_pos]:
            computed_checksum ^= byte
        
        # Compare computed checksum with expected (handle multi-byte if needed)
        expected_checksum_value = int.from_bytes(expected_checksum, byteorder=byte_order_python)
        if computed_checksum != expected_checksum_value:
            error_counts['checksum_failures'] += 1
            errors.append(f"Checksum failure at pos {pos} (expected {expected_checksum_value:#x}, got {computed_checksum:#x})")
            logger.warning(f"Checksum failure at pos {pos}, discarding packet")
            pos += packet_size
            continue
        
        # Verify end marker
        end_marker_pos = checksum_pos + checksum_size
        if data[end_marker_pos:end_marker_pos + len(end_marker)] != end_marker:
            error_counts['resync_events'] += 1
            errors.append(f"Invalid end marker at pos {end_marker_pos}")
            logger.warning(f"Invalid end marker at pos {end_marker_pos}, resyncing")
            pos += 1  # Move forward and resync
            continue
        
        # Parse payload - first byte is message code (response or command)
        if payload_size < 1:
            errors.append(f"Empty payload at pos {pos}")
            logger.warning(f"Empty payload at pos {pos}")
            pos += packet_size
            continue
        
        message_code = payload_data[0] # Response or command code is first byte of payload
        
        # Look up in both responses and commands
        is_response = message_code in messages_responses
        is_command = message_code in messages_commands
        
        if not is_response and not is_command:
            error_counts['unknown_message_codes'] += 1
            errors.append(f"Unknown message code {message_code:#x} at pos {pos}")
            logger.warning(f"Unknown message code {message_code:#x} at pos {pos}, skipping")
            pos += packet_size
            continue
        
        # Determine which definition to use
        # If code exists in both, default is responses (common for status codes), but log a debug message
        if is_response and is_command:
            logger.debug(f"Message code {message_code:#x} exists in both responses and commands, using response definition")
        
        if is_response:
            message_def = messages_responses[message_code]
            message_type = 'response'
        else:
            message_def = messages_commands[message_code]
            message_type = 'command'
        
        field_data = payload_data[1:]  # Skip message code byte
        
        sample = parse_message_fields(field_data, message_def, types, timestamp_ms, message_type)
        
        if sample is None:
            errors.append(f"Failed to parse message fields at pos {pos}")
            logger.warning(f"Failed to parse message fields at pos {pos}")
            pos += packet_size
            continue
        
        # Check for non-monotonic timestamp
        ts = sample['timestamp_s']
        if ts > last_ts:
            result.append(sample)
            last_ts = ts
        else:
            error_counts['non_monotonic'] += 1
            errors.append(f"Non-monotonic timestamp {ts} (previous: {last_ts}) - skipped")
            logger.warning(f"Non-monotonic timestamp {ts} - skipped")
        
        pos += packet_size
    
    if not result:
        raise ValueError(f"No valid packets found in {filepath}")
    
    # Log summary
    total_errors = sum(error_counts.values())
    if total_errors > 0:
        logger.warning(
            f"Motor BIN: {len(result)} valid packets, "
            f"{error_counts['checksum_failures']} checksum failures, "
            f"{error_counts['resync_events']} resync events, "
            f"{error_counts['truncated_packets']} truncated, "
            f"{error_counts['unknown_message_codes']} unknown codes, "
            f"{error_counts['non_monotonic']} non-monotonic"
        )
    else:
        logger.info(f"Motor BIN: {len(result)} packets loaded successfully")
    
    return result, errors


def parse_message_fields(
    field_data: bytes,
    message_def: Dict[str, Any],
    types: Dict[str, Any],
    timestamp_ms: int,
    message_type: str
) -> Optional[Dict[str, Any]]:
    """
    Parse message fields from binary payload.
    
    Field names are normalized to match CSV convention by appending units.
    Example: 'velocity' with unit 'rad/s' becomes 'velocity_rad_s'
    
    Args:
        field_data: Raw bytes after message code
        message_def: Response or command definition from YAML
        types: Type definitions from YAML
        timestamp_ms: Timestamp in milliseconds from header
        message_type: 'response' or 'command'
        
    Returns:
        Dict with parsed fields, or None on error
    """
    sample = {
        'timestamp_s': timestamp_ms / 1000.0,  # Convert ms to seconds
        'message_type': message_type,
        'message_name': message_def.get('name', 'unknown')
    }
    
    offset = 0
    for field in message_def['fields']:
        field_name = field['name']
        field_type = types[field['type']]
        field_size = field_type['size']
        field_format = field_type['format']
        
        # Normalize field name to include unit (matching CSV convention)
        # 'velocity' + 'rad/s' -> 'velocity_rad_s'
        # 'measured_current' + 'A' -> 'measured_current_a'
        field_unit = field.get('unit', '')
        normalized_name = normalize_field_name(field_name, field_unit)
        
        if offset + field_size > len(field_data):
            logger.warning(f"Not enough data for field '{field_name}'")
            return None
        
        try:
            value = struct.unpack(field_format, field_data[offset:offset + field_size])[0]
            sample[normalized_name] = float(value)
        except struct.error as e:
            logger.warning(f"Failed to unpack field '{field_name}': {e}")
            return None
        
        offset += field_size
    
    return sample


def normalize_field_name(name: str, unit: str) -> str:
    """
    Normalize field name by appending unit suffix.
    
    Converts unit to underscore format:
    - 'rad/s' -> '_rad_s'
    - 'A' -> '_a'
    - '' (no unit) -> '' (unchanged)
    
    Args:
        name: Original field name (e.g., 'velocity')
        unit: Unit string (e.g., 'rad/s')
        
    Returns:
        Normalized name (e.g., 'velocity_rad_s')
    """
    if not unit:
        return name
    
    # Convert unit to lowercase, replace '/' with '_'
    unit_suffix = unit.lower().replace('/', '_')
    
    return f"{name}_{unit_suffix}"


# ─────────────────────────────────────────────────────────────────────────────
# Class Wrappers (for evaluation harness compatibility)
# ─────────────────────────────────────────────────────────────────────────────

class MotorDataSource:
    """
    Motor data loader that handles both CSV and binary files.
    
    Example:
        motor = MotorDataSource(path, test_config, motor_protocol)
        data, errors = motor.load()
    """
    
    def __init__(self, filepath: Path, test_config: Dict[str, Any], motor_protocol: Dict[str, Any]):
        self.filepath = filepath
        self.test_config = test_config
        self.motor_protocol = motor_protocol
        self._data: Optional[List[Dict[str, float]]] = None
        self._errors: Optional[List[str]] = None
    
    def load(self) -> Tuple[List[Dict[str, float]], List[str]]:
        """Load data from file. Returns (data, errors)."""
        self._data, self._errors = load_motor_data(
            self.filepath, self.test_config, self.motor_protocol
        )
        return self._data, self._errors
    
    @property
    def data(self) -> List[Dict[str, float]]:
        """Get loaded data (loads if not already loaded)."""
        if self._data is None:
            self.load()
        return self._data
    
    @property
    def errors(self) -> List[str]:
        """Get loading errors (loads if not already loaded)."""
        if self._errors is None:
            self.load()
        return self._errors


class MotorCSVReader:
    """
    Motor CSV file reader.
    
    Example:
        reader = MotorCSVReader(path, config)
        data, errors = reader.load()
    """
    
    def __init__(self, filepath: Path, config: Dict[str, Any]):
        self.filepath = filepath
        self.config = config
        self._data: Optional[List[Dict[str, float]]] = None
        self._errors: Optional[List[str]] = None
    
    def load(self) -> Tuple[List[Dict[str, float]], List[str]]:
        """Load data from CSV file. Returns (data, errors)."""
        self._data, self._errors = load_motor_csv(self.filepath, self.config)
        return self._data, self._errors
    
    @property
    def data(self) -> List[Dict[str, float]]:
        """Get loaded data (loads if not already loaded)."""
        if self._data is None:
            self.load()
        return self._data
    
    @property
    def errors(self) -> List[str]:
        """Get loading errors (loads if not already loaded)."""
        if self._errors is None:
            self.load()
        return self._errors


class MotorBinaryReader:
    """
    Motor binary file reader.
    
    Example:
        reader = MotorBinaryReader(path, protocol)
        data, errors = reader.load()
    """
    
    def __init__(self, filepath: Path, protocol: Dict[str, Any]):
        self.filepath = filepath
        self.protocol = protocol
        self._data: Optional[List[Dict[str, float]]] = None
        self._errors: Optional[List[str]] = None
    
    def load(self) -> Tuple[List[Dict[str, float]], List[str]]:
        """Load data from binary file. Returns (data, errors)."""
        self._data, self._errors = load_motor_bin(self.filepath, self.protocol)
        return self._data, self._errors
    
    @property
    def data(self) -> List[Dict[str, float]]:
        """Get loaded data (loads if not already loaded)."""
        if self._data is None:
            self.load()
        return self._data
    
    @property
    def errors(self) -> List[str]:
        """Get loading errors (loads if not already loaded)."""
        if self._errors is None:
            self.load()
        return self._errors
