"""
Motor Characterization Test Bench - GUI Application

Tkinter-based GUI with:
- Connection Panel: File pickers for motor/sensor/PSU with status
- Test Control: Start, Abort, Speed control
- Live Display: Real-time plots of current, torque, voltage
- Summary Panel: Post-test statistics
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import queue
import time
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import yaml

# Matplotlib embedding in Tkinter
import matplotlib
matplotlib.use('TkAgg')
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# Import application modules
from drivers.motor import load_motor_data
from drivers.sensor import load_sensor_csv
from drivers.psu import load_psu_csv
from automation.synchronization import synchronize_data
from automation.state_machine import (
    run_current_ramp_phase,
    run_torque_hold_phase,
    run_voltage_decrease_phase,
    run_complete_phase
)

logger = logging.getLogger(__name__)

# Path to script directory (for finding config files)
SCRIPT_DIR = Path(__file__).resolve().parent.parent


class MotorCharacterizationApp:
    """Main GUI application for motor characterization test bench."""
    
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Motor Characterization Test Bench")
        self.root.geometry("1200x800")
        self.root.minsize(1000, 600)
        
        # Load YAML configs
        self.test_config = self._load_yaml(SCRIPT_DIR / "config" / "test_config.yaml")
        self.motor_protocol = self._load_yaml(SCRIPT_DIR / "config" / "motor_protocol.yaml")
        
        # Data storage
        self.motor_data: List[Dict] = []
        self.sensor_data: List[Dict] = []
        self.psu_data: List[Dict] = []
        self.synchronized_data: List[Dict] = []
        self.processed_samples: List[Dict] = []
        
        # File paths
        self.motor_path: Optional[Path] = None
        self.sensor_path: Optional[Path] = None
        self.psu_path: Optional[Path] = None
        self.output_path: Path = SCRIPT_DIR / "output" / "output.csv"
        
        # Data loading stats (for summary)
        self.motor_stats: Dict = {}
        self.sensor_stats: Dict = {}
        self.psu_stats: Dict = {}
        self.motor_errors: List[str] = []
        self.sensor_errors: List[str] = []
        self.psu_errors: List[str] = []
        
        # Playback state
        self.playback_thread: Optional[threading.Thread] = None
        self.playback_running = threading.Event()
        self.abort_requested = threading.Event()
        self.current_phase = "IDLE"
        self.playback_speed = 1.0  # 1x, 5x, 10x, or 0 for Max
        
        # Queue for thread-safe GUI updates
        self.update_queue = queue.Queue()
        
        # Phase timing (for summary)
        self.phase_start_times: Dict[str, float] = {}
        self.phase_durations: Dict[str, float] = {}
        
        # Plot data (circular buffers for live display)
        self.plot_timestamps: List[float] = []
        self.plot_measured_current: List[float] = []
        self.plot_commanded_current: List[float] = []
        self.plot_torque: List[float] = []
        self.plot_voltage: List[float] = []
        self.plot_max_points = 1000  # Keep last N points for display
        
        # Build GUI
        self._create_widgets()
        
        # Start update polling
        self._poll_updates()
    
    def _load_yaml(self, path: Path) -> Dict[str, Any]:
        """Load a YAML configuration file."""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            messagebox.showerror("Config Error", f"Failed to load {path.name}: {e}")
            return {}
    
    # ─────────────────────────────────────────────────────────────
    # GUI Creation
    # ─────────────────────────────────────────────────────────────
    
    def _create_widgets(self):
        """Create all GUI widgets."""
        # Main paned window for resizable layout
        self.main_pane = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        self.main_pane.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Left panel: Connection + Summary
        left_frame = ttk.Frame(self.main_pane, width=300)
        self.main_pane.add(left_frame, weight=1)
        
        # Right panel: Control + Live Display
        right_frame = ttk.Frame(self.main_pane)
        self.main_pane.add(right_frame, weight=3)
        
        # Create sub-panels
        self._create_connection_panel(left_frame)
        self._create_summary_panel(left_frame)
        self._create_control_panel(right_frame)
        self._create_live_display(right_frame)
    
    def _create_connection_panel(self, parent):
        """Create the connection panel with file pickers."""
        frame = ttk.LabelFrame(parent, text="Data Sources", padding=10)
        frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Motor data
        motor_frame = ttk.Frame(frame)
        motor_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(motor_frame, text="Motor Data:").pack(anchor=tk.W)
        
        motor_row = ttk.Frame(motor_frame)
        motor_row.pack(fill=tk.X)
        
        self.motor_entry = ttk.Entry(motor_row, width=25)
        self.motor_entry.pack(side=tk.LEFT, padx=(0, 5))
        
        ttk.Button(motor_row, text="Browse...", command=self._browse_motor).pack(side=tk.LEFT)
        
        ttk.Label(motor_row, text="Format:").pack(side=tk.LEFT, padx=(10, 5))
        self.motor_format = tk.StringVar(value="CSV")
        motor_format_combo = ttk.Combobox(motor_row, textvariable=self.motor_format, 
                                           values=["CSV", "Binary"], width=8, state="readonly")
        motor_format_combo.pack(side=tk.LEFT)
        
        self.motor_status = ttk.Label(motor_frame, text="Status: Not loaded", foreground="gray")
        self.motor_status.pack(anchor=tk.W, pady=(2, 0))
        
        # Sensor data
        sensor_frame = ttk.Frame(frame)
        sensor_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(sensor_frame, text="Sensor Data (CSV):").pack(anchor=tk.W)
        
        sensor_row = ttk.Frame(sensor_frame)
        sensor_row.pack(fill=tk.X)
        
        self.sensor_entry = ttk.Entry(sensor_row, width=30)
        self.sensor_entry.pack(side=tk.LEFT, padx=(0, 5))
        
        ttk.Button(sensor_row, text="Browse...", command=self._browse_sensor).pack(side=tk.LEFT)
        
        self.sensor_status = ttk.Label(sensor_frame, text="Status: Not loaded", foreground="gray")
        self.sensor_status.pack(anchor=tk.W, pady=(2, 0))
        
        # PSU data
        psu_frame = ttk.Frame(frame)
        psu_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(psu_frame, text="PSU Data (CSV):").pack(anchor=tk.W)
        
        psu_row = ttk.Frame(psu_frame)
        psu_row.pack(fill=tk.X)
        
        self.psu_entry = ttk.Entry(psu_row, width=30)
        self.psu_entry.pack(side=tk.LEFT, padx=(0, 5))
        
        ttk.Button(psu_row, text="Browse...", command=self._browse_psu).pack(side=tk.LEFT)
        
        self.psu_status = ttk.Label(psu_frame, text="Status: Not loaded", foreground="gray")
        self.psu_status.pack(anchor=tk.W, pady=(2, 0))
    
    def _create_control_panel(self, parent):
        """Create the test control panel."""
        frame = ttk.LabelFrame(parent, text="Test Control", padding=10)
        frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Button row
        btn_row = ttk.Frame(frame)
        btn_row.pack(fill=tk.X)
        
        self.start_btn = ttk.Button(btn_row, text="Start Test", command=self._start_test, state=tk.DISABLED)
        self.start_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        self.abort_btn = ttk.Button(btn_row, text="Abort", command=self._abort_test, state=tk.DISABLED)
        self.abort_btn.pack(side=tk.LEFT, padx=(0, 20))
        
        ttk.Label(btn_row, text="Speed:").pack(side=tk.LEFT, padx=(10, 5))
        self.speed_var = tk.StringVar(value="1x")
        speed_combo = ttk.Combobox(btn_row, textvariable=self.speed_var,
                                    values=["1x", "5x", "10x", "Max"], width=6, state="readonly")
        speed_combo.pack(side=tk.LEFT)
        speed_combo.bind("<<ComboboxSelected>>", self._on_speed_change)
        
        # Phase display
        phase_row = ttk.Frame(frame)
        phase_row.pack(fill=tk.X, pady=(10, 0))
        
        ttk.Label(phase_row, text="Current Phase:").pack(side=tk.LEFT)
        self.phase_label = ttk.Label(phase_row, text="IDLE", font=("TkDefaultFont", 12, "bold"))
        self.phase_label.pack(side=tk.LEFT, padx=10)
        
        # Progress
        self.progress_label = ttk.Label(phase_row, text="")
        self.progress_label.pack(side=tk.LEFT, padx=10)
    
    def _create_live_display(self, parent):
        """Create the live display with matplotlib plots."""
        frame = ttk.LabelFrame(parent, text="Live Display", padding=5)
        frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Create matplotlib figure with 3 subplots
        self.fig = Figure(figsize=(8, 6), dpi=100)
        self.fig.set_tight_layout(True)
        
        # Current subplot (measured + commanded)
        self.ax_current = self.fig.add_subplot(3, 1, 1)
        self.ax_current.set_ylabel("Current (A)")
        self.ax_current.set_title("Motor Current")
        self.line_measured, = self.ax_current.plot([], [], 'b-', label='Measured', linewidth=1)
        self.line_commanded, = self.ax_current.plot([], [], 'r--', label='Commanded', linewidth=1)
        self.ax_current.legend(loc='upper right', fontsize=8)
        self.ax_current.grid(True, alpha=0.3)
        
        # Torque subplot
        self.ax_torque = self.fig.add_subplot(3, 1, 2)
        self.ax_torque.set_ylabel("Torque (Nm)")
        self.ax_torque.set_title("Sensor Torque")
        self.line_torque, = self.ax_torque.plot([], [], 'g-', linewidth=1)
        self.ax_torque.grid(True, alpha=0.3)
        
        # Voltage subplot
        self.ax_voltage = self.fig.add_subplot(3, 1, 3)
        self.ax_voltage.set_xlabel("Time (s)")
        self.ax_voltage.set_ylabel("Voltage (V)")
        self.ax_voltage.set_title("PSU Voltage")
        self.line_voltage, = self.ax_voltage.plot([], [], 'm-', linewidth=1)
        self.ax_voltage.grid(True, alpha=0.3)
        
        # Embed in Tkinter
        self.canvas = FigureCanvasTkAgg(self.fig, master=frame)
        self.canvas.draw()
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
    
    def _create_summary_panel(self, parent):
        """Create the summary panel (shown after test completion)."""
        self.summary_frame = ttk.LabelFrame(parent, text="Summary", padding=10)
        self.summary_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.summary_text = tk.Text(self.summary_frame, height=15, width=35, state=tk.DISABLED,
                                     font=("Courier", 11))
        self.summary_text.pack(fill=tk.BOTH, expand=True)
        
        # Initially show placeholder
        self._update_summary_text("Run a test to see summary.")
    
    # ─────────────────────────────────────────────────────────────
    # File Browsing
    # ─────────────────────────────────────────────────────────────
    
    def _browse_motor(self):
        """Browse for motor data file."""
        fmt = self.motor_format.get()
        if fmt == "Binary":
            filetypes = [("Binary files", "*.bin"), ("All files", "*.*")]
        else:
            filetypes = [("CSV files", "*.csv"), ("All files", "*.*")]
        
        filepath = filedialog.askopenfilename(
            title="Select Motor Data File",
            filetypes=filetypes,
            initialdir=SCRIPT_DIR / "data"
        )
        if filepath:
            self.motor_entry.delete(0, tk.END)
            self.motor_entry.insert(0, filepath)
            self._load_motor_data(Path(filepath))
    
    def _browse_sensor(self):
        """Browse for sensor data file."""
        filepath = filedialog.askopenfilename(
            title="Select Sensor Data File",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialdir=SCRIPT_DIR / "data"
        )
        if filepath:
            self.sensor_entry.delete(0, tk.END)
            self.sensor_entry.insert(0, filepath)
            self._load_sensor_data(Path(filepath))
    
    def _browse_psu(self):
        """Browse for PSU data file."""
        filepath = filedialog.askopenfilename(
            title="Select PSU Data File",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialdir=SCRIPT_DIR / "data"
        )
        if filepath:
            self.psu_entry.delete(0, tk.END)
            self.psu_entry.insert(0, filepath)
            self._load_psu_data(Path(filepath))
    
    # ─────────────────────────────────────────────────────────────
    # Data Loading
    # ─────────────────────────────────────────────────────────────
    
    def _load_motor_data(self, filepath: Path):
        """Start background thread to load motor data file."""
        self.motor_status.config(text="Loading...", foreground="blue")
        self.motor_data = []
        self.motor_path = None
        self._update_start_button()
        
        def load_task():
            try:
                data, errors = load_motor_data(
                    filepath, self.test_config, self.motor_protocol
                )
                self.update_queue.put(('motor_loaded', {
                    'data': data,
                    'errors': errors,
                    'path': filepath,
                    'stats': self._compute_stats(data)
                }))
            except Exception as e:
                self.update_queue.put(('motor_error', str(e)))
        
        threading.Thread(target=load_task, daemon=True).start()
    
    def _load_sensor_data(self, filepath: Path):
        """Start background thread to load sensor data file."""
        self.sensor_status.config(text="Loading...", foreground="blue")
        self.sensor_data = []
        self.sensor_path = None
        self._update_start_button()
        
        def load_task():
            try:
                data, errors = load_sensor_csv(filepath, self.test_config)
                self.update_queue.put(('sensor_loaded', {
                    'data': data,
                    'errors': errors,
                    'path': filepath,
                    'stats': self._compute_stats(data)
                }))
            except Exception as e:
                self.update_queue.put(('sensor_error', str(e)))
        
        threading.Thread(target=load_task, daemon=True).start()
    
    def _load_psu_data(self, filepath: Path):
        """Start background thread to load PSU data file."""
        self.psu_status.config(text="Loading...", foreground="blue")
        self.psu_data = []
        self.psu_path = None
        self._update_start_button()
        
        def load_task():
            try:
                data, errors = load_psu_csv(filepath, self.test_config)
                self.update_queue.put(('psu_loaded', {
                    'data': data,
                    'errors': errors,
                    'path': filepath,
                    'stats': self._compute_stats(data)
                }))
            except Exception as e:
                self.update_queue.put(('psu_error', str(e)))
        
        threading.Thread(target=load_task, daemon=True).start()
    
    def _compute_stats(self, data: List[Dict]) -> Dict:
        """Compute basic statistics for loaded data."""
        if not data:
            return {'row_count': 0, 'time_span_s': 0}
        
        # Find timestamp key
        ts_key = None
        for key in data[0].keys():
            if 'timestamp' in key.lower():
                ts_key = key
                break
        
        if ts_key is None:
            return {'row_count': len(data), 'time_span_s': 0}
        
        first_ts = data[0][ts_key]
        last_ts = data[-1][ts_key]
        
        return {
            'row_count': len(data),
            'time_span_s': last_ts - first_ts,
            'first_ts': first_ts,
            'last_ts': last_ts
        }
    
    def _update_start_button(self):
        """Enable Start button only when all sources are loaded."""
        if self.motor_data and self.sensor_data and self.psu_data:
            self.start_btn.config(state=tk.NORMAL)
        else:
            self.start_btn.config(state=tk.DISABLED)
    
    # ─────────────────────────────────────────────────────────────
    # Test Control
    # ─────────────────────────────────────────────────────────────
    
    def _on_speed_change(self, event=None):
        """Handle speed selection change."""
        speed_str = self.speed_var.get()
        if speed_str == "Max":
            self.playback_speed = 0  # 0 = no delay
        else:
            self.playback_speed = float(speed_str.replace("x", ""))
    
    def _start_test(self):
        """Start the test playback."""
        if self.playback_thread and self.playback_thread.is_alive():
            return  # Already running
        
        # Reset state
        self.processed_samples = []
        self.plot_timestamps = []
        self.plot_measured_current = []
        self.plot_commanded_current = []
        self.plot_torque = []
        self.plot_voltage = []
        self.phase_start_times = {}
        self.phase_durations = {}
        self.abort_requested.clear()
        self.playback_running.set()
        
        # Update UI
        self.start_btn.config(state=tk.DISABLED)
        self.abort_btn.config(state=tk.NORMAL)
        self._update_phase("SETUP")
        
        # Start playback thread
        self.playback_thread = threading.Thread(target=self._run_playback, daemon=True)
        self.playback_thread.start()
    
    def _abort_test(self):
        """Abort the test and transition to COMPLETE."""
        self.abort_requested.set()
        self._update_phase("ABORTING...")
    
    def _run_playback(self):
        """Run the test playback in a background thread."""
        try:
            # Synchronize data
            self._update_phase("SETUP")
            self.phase_start_times["SETUP"] = time.time()
            
            self.synchronized_data, sync_stats = synchronize_data(
                motor_data=self.motor_data,
                sensor_data=self.sensor_data,
                psu_data=self.psu_data,
                config=self.test_config
            )
            
            self.phase_durations["SETUP"] = time.time() - self.phase_start_times["SETUP"]
            
            if not self.synchronized_data:
                self.update_queue.put(('error', "Synchronization failed: no data"))
                return
            
            if self.abort_requested.is_set():
                self._finish_playback("Aborted during SETUP")
                return
            
            # Run CURRENT_RAMP
            self._update_phase("CURRENT_RAMP")
            self.phase_start_times["CURRENT_RAMP"] = time.time()
            
            ramp_result = self._run_phase_with_playback(
                run_current_ramp_phase,
                self.synchronized_data,
                self.test_config,
                start_index=0
            )
            
            self.phase_durations["CURRENT_RAMP"] = time.time() - self.phase_start_times["CURRENT_RAMP"]
            self.processed_samples.extend(ramp_result['processed_samples'])
            
            if self.abort_requested.is_set() or ramp_result['safety_violation'] or ramp_result['data_exhausted']:
                reason = "Aborted" if self.abort_requested.is_set() else ramp_result['transition_reason']
                self._finish_playback(reason)
                return
            
            # Get hold current for next phases
            hold_current = ramp_result['processed_samples'][-1]['commanded_current_a'] if ramp_result['processed_samples'] else 0
            current_index = ramp_result['end_index']
            
            # Run TORQUE_HOLD
            self._update_phase("TORQUE_HOLD")
            self.phase_start_times["TORQUE_HOLD"] = time.time()
            
            hold_result = self._run_phase_with_playback(
                lambda data, config, start_index: run_torque_hold_phase(data, config, hold_current, start_index),
                self.synchronized_data,
                self.test_config,
                start_index=current_index
            )
            
            self.phase_durations["TORQUE_HOLD"] = time.time() - self.phase_start_times["TORQUE_HOLD"]
            self.processed_samples.extend(hold_result['processed_samples'])
            
            if self.abort_requested.is_set() or hold_result['safety_violation'] or hold_result['data_exhausted']:
                reason = "Aborted" if self.abort_requested.is_set() else hold_result['transition_reason']
                self._finish_playback(reason)
                return
            
            current_index = hold_result['end_index']
            
            # Run VOLTAGE_DECREASE
            self._update_phase("VOLTAGE_DECREASE")
            self.phase_start_times["VOLTAGE_DECREASE"] = time.time()
            
            decrease_result = self._run_phase_with_playback(
                lambda data, config, start_index: run_voltage_decrease_phase(data, config, hold_current, start_index),
                self.synchronized_data,
                self.test_config,
                start_index=current_index
            )
            
            self.phase_durations["VOLTAGE_DECREASE"] = time.time() - self.phase_start_times["VOLTAGE_DECREASE"]
            self.processed_samples.extend(decrease_result['processed_samples'])
            
            # Finish
            self._finish_playback(decrease_result['transition_reason'])
            
        except Exception as e:
            logger.exception("Playback error")
            self.update_queue.put(('error', str(e)))
            self._finish_playback(f"Error: {e}")
    
    def _run_phase_with_playback(self, phase_func, data, config, start_index):
        """
        Run a phase function while respecting playback speed.
        
        Instead of running the phase all at once, we simulate playback by
        processing samples with timing based on timestamps.
        """
        # For simplicity, run the phase to get all results, then replay for visualization
        result = phase_func(data, config, start_index)
        
        # Playback the processed samples with timing
        samples = result['processed_samples']
        if not samples:
            return result
        
        start_ts = samples[0]['timestamp_s']
        playback_start = time.time()
        last_update = playback_start
        
        for i, sample in enumerate(samples):
            if self.abort_requested.is_set():
                break
            
            # Calculate target time based on playback speed
            sample_ts = sample['timestamp_s']
            elapsed_data_time = sample_ts - start_ts
            
            if self.playback_speed > 0:
                target_real_time = elapsed_data_time / self.playback_speed
                current_real_time = time.time() - playback_start
                
                if target_real_time > current_real_time:
                    time.sleep(target_real_time - current_real_time)
            
            # Update plot data
            self._add_plot_point(sample)
            
            # Update GUI at max 20 Hz
            now = time.time()
            if now - last_update >= 0.05:  # 50ms = 20 Hz
                progress = f"Sample {i+1}/{len(samples)}"
                self.update_queue.put(('progress', progress))
                self.update_queue.put(('plot', None))  # Signal to redraw plot
                last_update = now
        
        return result
    
    def _add_plot_point(self, sample: Dict):
        """Add a sample to the plot data buffers."""
        self.plot_timestamps.append(sample.get('timestamp_s', 0))
        self.plot_measured_current.append(sample.get('motor_measured_current_a', 0) or 0)
        self.plot_commanded_current.append(sample.get('commanded_current_a', 0) or 0)
        self.plot_torque.append(sample.get('sensor_torque_nm', 0) or 0)
        self.plot_voltage.append(sample.get('psu_voltage_v', 0) or 0)
        
        # Trim to max points
        if len(self.plot_timestamps) > self.plot_max_points:
            self.plot_timestamps = self.plot_timestamps[-self.plot_max_points:]
            self.plot_measured_current = self.plot_measured_current[-self.plot_max_points:]
            self.plot_commanded_current = self.plot_commanded_current[-self.plot_max_points:]
            self.plot_torque = self.plot_torque[-self.plot_max_points:]
            self.plot_voltage = self.plot_voltage[-self.plot_max_points:]
    
    def _finish_playback(self, reason: str):
        """Finish playback and run COMPLETE phase."""
        self._update_phase("COMPLETE")
        self.phase_start_times["COMPLETE"] = time.time()
        
        # Run COMPLETE phase to write output
        try:
            complete_result = run_complete_phase(
                all_processed_samples=self.processed_samples,
                config=self.test_config,
                output_path=self.output_path
            )
            self.phase_durations["COMPLETE"] = time.time() - self.phase_start_times["COMPLETE"]
            
            # Update summary
            self.update_queue.put(('summary', {
                'reason': reason,
                'complete_result': complete_result,
                'phase_durations': self.phase_durations.copy()
            }))
        except Exception as e:
            logger.exception("Error in COMPLETE phase")
            self.update_queue.put(('error', f"COMPLETE failed: {e}"))
        
        self.playback_running.clear()
        self.update_queue.put(('done', None))
    
    def _update_phase(self, phase: str):
        """Update the current phase (thread-safe)."""
        self.current_phase = phase
        self.update_queue.put(('phase', phase))
    
    # ─────────────────────────────────────────────────────────────
    # GUI Update Polling
    # ─────────────────────────────────────────────────────────────
    
    def _poll_updates(self):
        """Poll the update queue and apply changes to GUI."""
        try:
            while True:
                msg_type, msg_data = self.update_queue.get_nowait()
                
                if msg_type == 'phase':
                    self.phase_label.config(text=msg_data)
                elif msg_type == 'progress':
                    self.progress_label.config(text=msg_data)
                elif msg_type == 'plot':
                    self._update_plot()
                elif msg_type == 'summary':
                    self._show_summary(msg_data)
                elif msg_type == 'error':
                    messagebox.showerror("Error", msg_data)
                elif msg_type == 'done':
                    self.start_btn.config(state=tk.NORMAL)
                    self.abort_btn.config(state=tk.DISABLED)
                    self.progress_label.config(text="")
                    self._update_plot()  # Final plot update
                
                # File loading results (threaded)
                elif msg_type == 'motor_loaded':
                    self._on_motor_loaded(msg_data)
                elif msg_type == 'motor_error':
                    self.motor_status.config(text=f"Error: {msg_data}", foreground="red")
                    self.motor_data = []
                    self.motor_path = None
                    self._update_start_button()
                elif msg_type == 'sensor_loaded':
                    self._on_sensor_loaded(msg_data)
                elif msg_type == 'sensor_error':
                    self.sensor_status.config(text=f"Error: {msg_data}", foreground="red")
                    self.sensor_data = []
                    self.sensor_path = None
                    self._update_start_button()
                elif msg_type == 'psu_loaded':
                    self._on_psu_loaded(msg_data)
                elif msg_type == 'psu_error':
                    self.psu_status.config(text=f"Error: {msg_data}", foreground="red")
                    self.psu_data = []
                    self.psu_path = None
                    self._update_start_button()
                    
        except queue.Empty:
            pass
        
        # Schedule next poll
        self.root.after(50, self._poll_updates)  # 50ms = 20 Hz
    
    def _on_motor_loaded(self, result: Dict):
        """Handle motor data loaded from background thread."""
        self.motor_data = result['data']
        self.motor_errors = result['errors']
        self.motor_path = result['path']
        self.motor_stats = result['stats']
        
        rows = len(self.motor_data)
        span = self.motor_stats.get('time_span_s', 0)
        errors = len(self.motor_errors)
        status = f"Loaded: {rows} rows, {span:.2f}s span, {errors} errors"
        self.motor_status.config(text=status, foreground="green" if errors == 0 else "orange")
        self._update_start_button()
    
    def _on_sensor_loaded(self, result: Dict):
        """Handle sensor data loaded from background thread."""
        self.sensor_data = result['data']
        self.sensor_errors = result['errors']
        self.sensor_path = result['path']
        self.sensor_stats = result['stats']
        
        rows = len(self.sensor_data)
        span = self.sensor_stats.get('time_span_s', 0)
        errors = len(self.sensor_errors)
        status = f"Loaded: {rows} rows, {span:.2f}s span, {errors} errors"
        self.sensor_status.config(text=status, foreground="green" if errors == 0 else "orange")
        self._update_start_button()
    
    def _on_psu_loaded(self, result: Dict):
        """Handle PSU data loaded from background thread."""
        self.psu_data = result['data']
        self.psu_errors = result['errors']
        self.psu_path = result['path']
        self.psu_stats = result['stats']
        
        rows = len(self.psu_data)
        span = self.psu_stats.get('time_span_s', 0)
        errors = len(self.psu_errors)
        status = f"Loaded: {rows} rows, {span:.2f}s span, {errors} errors"
        self.psu_status.config(text=status, foreground="green" if errors == 0 else "orange")
        self._update_start_button()
    
    def _update_plot(self):
        """Update the matplotlib plot with current data."""
        if not self.plot_timestamps:
            return
        
        # Update line data
        self.line_measured.set_data(self.plot_timestamps, self.plot_measured_current)
        self.line_commanded.set_data(self.plot_timestamps, self.plot_commanded_current)
        self.line_torque.set_data(self.plot_timestamps, self.plot_torque)
        self.line_voltage.set_data(self.plot_timestamps, self.plot_voltage)
        
        # Adjust axes
        for ax, y_data in [
            (self.ax_current, self.plot_measured_current + self.plot_commanded_current),
            (self.ax_torque, self.plot_torque),
            (self.ax_voltage, self.plot_voltage)
        ]:
            ax.set_xlim(min(self.plot_timestamps), max(self.plot_timestamps))
            if y_data:
                y_min, y_max = min(y_data), max(y_data)
                margin = (y_max - y_min) * 0.1 if y_max != y_min else 1
                ax.set_ylim(y_min - margin, y_max + margin)
        
        # Redraw
        self.canvas.draw_idle()
    
    def _update_summary_text(self, text: str):
        """Update the summary text widget."""
        self.summary_text.config(state=tk.NORMAL)
        self.summary_text.delete(1.0, tk.END)
        self.summary_text.insert(tk.END, text)
        self.summary_text.config(state=tk.DISABLED)
    
    def _show_summary(self, data: Dict):
        """Display the test summary."""
        reason = data.get('reason', 'Unknown')
        complete_result = data.get('complete_result', {})
        phase_durations = data.get('phase_durations', {})
        
        stats = complete_result.get('stats', {})
        
        lines = [
            "=" * 35,
            "TEST SUMMARY",
            "=" * 35,
            "",
            f"Completion: {reason}",
            f"Output: {complete_result.get('output_path', 'N/A')}",
            "",
            "--- Samples Processed ---",
            f"Motor:  {self.motor_stats.get('row_count', 0)}",
            f"Sensor: {self.sensor_stats.get('row_count', 0)}",
            f"PSU:    {self.psu_stats.get('row_count', 0)}",
            f"Output: {complete_result.get('row_count', 0)}",
            "",
            "--- Parse Errors ---",
            f"Motor:  {len(self.motor_errors)}",
            f"Sensor: {len(self.sensor_errors)}",
            f"PSU:    {len(self.psu_errors)}",
            "",
            "--- Phase Durations ---",
        ]
        
        for phase, duration in phase_durations.items():
            lines.append(f"{phase}: {duration:.2f}s")
        
        lines.extend([
            "",
            "--- Peak Values ---",
            f"Peak Torque:  {stats.get('max_torque_nm', 'N/A')} Nm",
            f"Peak Current: {stats.get('max_measured_current_a', 'N/A')} A",
            f"Peak Cmd Current: {stats.get('max_commanded_current_a', 'N/A')} A",
            "",
            "=" * 35,
        ])
        
        self._update_summary_text("\n".join(lines))


def run_gui():
    """Launch the GUI application."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    root = tk.Tk()
    app = MotorCharacterizationApp(root)
    root.mainloop()


if __name__ == "__main__":
    run_gui()
