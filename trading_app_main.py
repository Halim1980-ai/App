import sys # <--- تم التعديل هنا
import os
import traceback
from threading import Thread, Lock
from datetime import datetime, timezone, timedelta
import time
import logging
import pandas as pd
import numpy as np

from PyQt6 import QtWidgets, QtCore, QtGui
from PyQt6.QtCore import Qt, QTime, QDate, QThread, pyqtSignal
from PyQt6.QtWidgets import QStyleFactory, QMessageBox, QDialog

# --- Matplotlib for performance plot ---
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
# --- End Matplotlib --

# Assuming these files are in the same directory or accessible via PYTHONPATH
try:
    from mt5_manager import MT5Manager
    from data_manager import DataManager
    from settings_dialog import SettingsDialog
    from news_manager import NewsManager
    from utils import (
        add_technical_indicators,
        create_target_variable,
        train_and_save_model,
        load_model_and_predict,
        format_price_display
    )
    import performance_metrics
except ImportError as e:
    print(f"Critical Import Error: {e}. Please ensure all required .py files are present.", file=sys.stderr)
    try:
        critical_error_logger = logging.getLogger(__name__)
        critical_error_logger.critical(f"Import Error: {e}. Application cannot start.", exc_info=True)
    except Exception:
        pass
    sys.exit(1)


AUTO_TRADE_LOCK = Lock()
MAX_SIGNALS_TO_SHOW_IN_TABLE = 50 # MODIFIED: Max signals to display (increased from 2 for more visibility)


# --- Matplotlib Canvas Widget ---
class MplCanvas(FigureCanvas):
    def __init__(self, parent=None, width=5, height=4, dpi=100):
        self.fig = Figure(figsize=(width, height), dpi=dpi)
        self.axes = self.fig.add_subplot(111)
        super(MplCanvas, self).__init__(self.fig)
        self.setParent(parent)
        FigureCanvas.setSizePolicy(self,
                                   QtWidgets.QSizePolicy.Policy.Expanding,
                                   QtWidgets.QSizePolicy.Policy.Expanding)
        FigureCanvas.updateGeometry(self)
# --- End Matplotlib Canvas Widget ---

# --- QThread for MT5 Connection ---
class MT5ConnectThread(QThread):
    connection_signal = pyqtSignal(bool, str, bool) # (connected_status, message, is_manual_attempt)

    def __init__(self, mt5_manager_instance, is_manual_attempt: bool, parent=None):
        super().__init__(parent)
        self.mt5_manager = mt5_manager_instance
        self.is_manual_attempt = is_manual_attempt
        self.logger = logging.getLogger(__name__ + ".MT5ConnectThread")

    def run(self):
        self.logger.debug(f"MT5ConnectThread started (Manual: {self.is_manual_attempt}).")
        connected = False
        message = "Connection attempt failed."
        try:
            connected = self.mt5_manager.connect() # This now sets mt5_manager.last_raw_error_message
            if connected:
                acc_info = self.mt5_manager.get_account_info()
                if acc_info:
                    message = f"الحساب: {acc_info.login}, الوسيط: {acc_info.company}"
                else:
                    message = "متصل ولكن لا يمكن جلب معلومات الحساب."
                    self.logger.warning("MT5ConnectThread: Connected but failed to get account info.")
            else:
                # Use the last_raw_error_message from mt5_manager
                message = getattr(self.mt5_manager, 'last_raw_error_message', "فشل الاتصال بـ MT5. راجع السجلات.")
                # Add more user-friendly messages based on common error parts
                if "Authorization failed" in message:
                     message = f"فشل الاتصال: خطأ في التفويض (بيانات اعتماد غير صحيحة؟)\nتفاصيل: {message}"
                elif "Terminal: Connect failed" in message or "connection refused" in message.lower():
                    message = f"فشل الاتصال: لا يمكن الاتصال بالخادم (تحقق من الخادم أو اتصال الإنترنت).\nتفاصيل: {message}"
                elif "login, server, path" in message: # From our own check in mt5_manager
                    message = f"فشل الاتصال: بيانات الاتصال (تسجيل الدخول، الخادم، المسار) غير مكتملة.\nتفاصيل: {message}"

                self.logger.warning(f"MT5ConnectThread: Connection returned False. Manager's last error: {getattr(self.mt5_manager, 'last_raw_error_message', 'N/A')}")

        except Exception as e:
            message = f"Exception during connection: {str(e)}"
            self.logger.error(f"MT5ConnectThread: Exception during connect: {e}", exc_info=True)

        self.logger.debug(f"MT5ConnectThread finished. Connected: {connected}, Message: {message}")
        self.connection_signal.emit(connected, message, self.is_manual_attempt)

class TradingApp(QtWidgets.QMainWindow):
    log_signal_ui = QtCore.pyqtSignal(str, str)
    signals_loaded_signal = QtCore.pyqtSignal(pd.DataFrame)

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__ + ".TradingApp")

        self.user_login_display = "Halim1980-ai" # Set by main() or keep default
        self.utc_start_time_for_title = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        self.setWindowTitle(f"تطبيق التداول الآلي MT5 - {self.user_login_display} - UTC Start: {self.utc_start_time_for_title}")
        self.resize(1450, 900)

        self.data_manager = DataManager(log_callback=self.log_to_ui_and_logger_wrapper)
        self.mt5_manager = MT5Manager(log_callback=self.log_to_ui_and_logger_wrapper, data_manager=self.data_manager)

        try:
            self.news_manager = NewsManager(log_callback=self.log_to_ui_and_logger_wrapper, data_manager=self.data_manager)
        except Exception as e_nm_init:
            self.log_to_ui_and_logger_wrapper(f"Failed to initialize NewsManager: {e_nm_init}. News functionality will be limited.", "ERROR")
            self.logger.error(f"NewsManager Init Traceback: {traceback.format_exc()}")
            self.news_manager = None

        self.df_signals_columns = ["time", "Symbol", "signal", "confidence_%", "close", "spread_pips",
                                   "take_profit_price", "stop_loss_price", "take_profit_pips",
                                   "stop_loss_pips", "executed", "notes"]
        self.df_signals = pd.DataFrame(columns=self.df_signals_columns)
        if 'time' in self.df_signals.columns: # Ensure time column is datetime if df is somehow pre-populated
            self.df_signals['time'] = pd.to_datetime(self.df_signals['time'], errors='coerce')

        self.df_deals_history = pd.DataFrame()
        self.last_trade_time = {} # {symbol: datetime_utc}

        self.trading_allowed_by_news = True
        self.current_model_filename = "model_XAUUSD.joblib" # Default, will be updated by settings
        self.current_btc_model_filename = "model_BTCUSD.joblib" # Default
        self.manual_filter_min_confidence = 70 # Default, will be updated by settings
        self.model_trained_this_session_flags = {"GOLD_MODEL": False, "BITCOIN_MODEL": False}
        self.mt5_connect_thread = None # For MT5 connection QThread

        self._setup_ui()
        self._connect_signals_slots()
        self.load_app_settings()     # Load settings (this might set the checkbox state for deals logging)

        # Initial calls after all setup and settings are loaded
        self.connect_to_mt5_on_startup() # This is async, non-blocking
        self.refresh_all_signals_display() # This is async, non-blocking
        self.refresh_performance_stats() # Call it once here explicitly
        self.update_trading_session_display()

        self.log_to_ui_and_logger_wrapper("TradingApp initialized successfully.", "INFO")

    def _update_signal_status_in_df(self, signal_time_identifier, signal_symbol: str, executed_status: bool, note_text: str):
        if self.df_signals.empty or 'time' not in self.df_signals.columns or 'Symbol' not in self.df_signals.columns:
            # self.log_to_ui_and_logger_wrapper(f"Cannot update signal status: df_signals empty or key columns missing.", "DEBUG")
            return

        signal_time_dt = None
        if isinstance(signal_time_identifier, str):
            try: signal_time_dt = pd.to_datetime(signal_time_identifier)
            except ValueError:
                self.log_to_ui_and_logger_wrapper(f"Could not parse signal_time_identifier '{signal_time_identifier}' to datetime for update.", "ERROR")
                return
        elif isinstance(signal_time_identifier, datetime): # Includes pd.Timestamp
            signal_time_dt = pd.to_datetime(signal_time_identifier) # Ensure it's a pd.Timestamp for consistency
        else:
            self.log_to_ui_and_logger_wrapper(f"Unsupported signal_time_identifier type: {type(signal_time_identifier)} for update.", "ERROR")
            return

        if pd.isna(signal_time_dt):
            self.log_to_ui_and_logger_wrapper(f"signal_time_identifier resulted in NaT. Cannot update signal status.", "ERROR")
            return

        # Ensure df_signals['time'] is datetime64[ns, UTC] for comparison
        if not pd.api.types.is_datetime64_any_dtype(self.df_signals['time']):
            self.df_signals['time'] = pd.to_datetime(self.df_signals['time'], errors='coerce')
        
        if self.df_signals['time'].dt.tz is None:
            self.df_signals['time'] = self.df_signals['time'].dt.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
        else:
            self.df_signals['time'] = self.df_signals['time'].dt.tz_convert('UTC')
        
        # Ensure signal_time_dt is UTC for comparison
        if signal_time_dt.tzinfo is None:
            signal_time_dt = signal_time_dt.tz_localize('UTC')
        else:
            signal_time_dt = signal_time_dt.tz_convert('UTC')

        match_condition = (self.df_signals['time'] == signal_time_dt) & (self.df_signals['Symbol'] == signal_symbol)
        indices_to_update = self.df_signals.index[match_condition].tolist()

        if indices_to_update:
            for idx in indices_to_update:
                self.df_signals.loc[idx, 'executed'] = executed_status
                self.df_signals.loc[idx, 'notes'] = note_text
            # self.log_to_ui_and_logger_wrapper(f"Updated signal status for {signal_symbol} at {signal_time_dt.strftime('%Y-%m-%d %H:%M:%S %Z')} to executed={executed_status}, note='{note_text}'.", "DEBUG")
        # else:
            # self.log_to_ui_and_logger_wrapper(f"No signal found in df_signals to update for {signal_symbol} at {signal_time_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}", "DEBUG")


    def log_to_ui_and_logger_wrapper(self, message: str, level: str = "INFO"):
        level_upper = level.upper()
        if not isinstance(message, str):
            try: message = str(message)
            except Exception as e_str_conv:
                message = f"Error converting log message to string. Original type: {type(message)}. Error: {e_str_conv}"
                level_upper = "ERROR"

        # Prepend class/module name if not already present for non-TradingApp originated logs (e.g. from MT5Manager)
        # For TradingApp's own logs, the logger name will handle it.
        # This logic might be better placed in the individual managers or handled by logger naming.
        # For now, keep it simple.

        if level_upper == "INFO": self.logger.info(message)
        elif level_upper == "WARNING": self.logger.warning(message)
        elif level_upper == "ERROR": self.logger.error(message)
        elif level_upper == "CRITICAL": self.logger.critical(message)
        elif level_upper == "DEBUG": self.logger.debug(message)
        else: self.logger.info(f"[{level_upper}] {message}") # Fallback for custom levels

        self.log_signal_ui.emit(message, level_upper) # Emit to UI

    def get_user_login_display(self): # Used by main() to set window title
        return self.user_login_display

    def _setup_ui(self):
        central_widget = QtWidgets.QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QtWidgets.QVBoxLayout(central_widget)

        # Account Summary Label
        self.account_summary_label = QtWidgets.QLabel("جارٍ تحميل ملخص الحساب...")
        self.account_summary_label.setStyleSheet("font-weight: bold; font-size: 13px; padding: 6px; background-color: #34495e; color: #ecf0f1; border: 1px solid #2c3e50; border-radius: 4px; qproperty-alignment: 'AlignCenter';")
        main_layout.addWidget(self.account_summary_label)

        # News Alert Label
        self.news_alert_label = QtWidgets.QLabel("") # Initially empty
        self.news_alert_label.setWordWrap(True)
        self.news_alert_label.hide() # Hidden by default
        self.news_alert_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(self.news_alert_label)

        # Tab Widget
        self.tab_widget = QtWidgets.QTabWidget()
        main_layout.addWidget(self.tab_widget)

        # Signals Tab
        self.signals_tab = QtWidgets.QWidget()
        self.tab_widget.addTab(self.signals_tab, "📊 الإشارات (Signals)")
        self._init_signals_tab_ui()

        # Performance Tab
        self.performance_tab = QtWidgets.QWidget()
        self.tab_widget.addTab(self.performance_tab, "📈 أداء الصفقات (Performance)")
        self._init_performance_tab_ui()

        # Logs Tab
        self.logs_tab = QtWidgets.QWidget()
        self.tab_widget.addTab(self.logs_tab, "سجل النظام (System Log)")
        self._init_logs_tab_ui()

        # Bottom Bar for Trading Session
        bottom_bar_layout = QtWidgets.QHBoxLayout()
        self.trading_session_label = QtWidgets.QLabel("الجلسة: جاري التحديد...")
        self.trading_session_label.setStyleSheet("font-size: 11px; padding: 4px; color: #ecf0f1; background-color: #2c3e50; border-radius: 3px; margin: 2px;")
        bottom_bar_layout.addStretch()
        bottom_bar_layout.addWidget(self.trading_session_label)
        main_layout.addLayout(bottom_bar_layout)

        # Timers
        self.refresh_signals_timer = QtCore.QTimer(self)
        self.refresh_signals_timer.setObjectName("RefreshSignalsTimer")

        self.position_monitor_timer = QtCore.QTimer(self)
        self.position_monitor_timer.setObjectName("PositionMonitorTimer")

        self.account_summary_timer = QtCore.QTimer(self)
        self.account_summary_timer.setObjectName("AccountSummaryTimer")
        self.account_summary_timer.timeout.connect(self.refresh_account_summary)
        # Start interval set in load_app_settings

        self.session_update_timer = QtCore.QTimer(self)
        self.session_update_timer.setObjectName("TradingSessionUpdateTimer")
        self.session_update_timer.timeout.connect(self.update_trading_session_display)
        self.session_update_timer.start(60 * 1000) # Update trading session every minute

        self.statusBar().showMessage("جاهز.", 3000)

    def _init_signals_tab_ui(self):
        layout = QtWidgets.QVBoxLayout(self.signals_tab)
        # Controls for confidence filter
        controls_layout = QtWidgets.QHBoxLayout()
        layout.addLayout(controls_layout)
        controls_layout.addWidget(QtWidgets.QLabel("حد الثقة للعرض:"))
        self.confidence_slider = QtWidgets.QSlider(Qt.Orientation.Horizontal)
        self.confidence_slider.setMinimum(0); self.confidence_slider.setMaximum(100)
        self.confidence_slider.setValue(self.manual_filter_min_confidence) # Set from default
        self.confidence_slider.setTickInterval(10); self.confidence_slider.setTickPosition(QtWidgets.QSlider.TickPosition.TicksBelow)
        controls_layout.addWidget(self.confidence_slider)
        self.confidence_label = QtWidgets.QLabel(f"{self.confidence_slider.value()}%")
        controls_layout.addWidget(self.confidence_label)
        controls_layout.addStretch()
        self.refresh_signals_btn = QtWidgets.QPushButton("🔄 تحديث كل الإشارات")
        self.refresh_signals_btn.setIcon(self.style().standardIcon(QtWidgets.QStyle.StandardPixmap.SP_BrowserReload))
        controls_layout.addWidget(self.refresh_signals_btn)

        # Buttons layout
        btn_layout = QtWidgets.QHBoxLayout()
        self.buy_btn = QtWidgets.QPushButton("⬆️ شراء (BUY)")
        self.buy_btn.setStyleSheet("background-color: #2ecc71; color: white; font-weight: bold; padding: 8px 12px; border-radius:4px;")
        self.sell_btn = QtWidgets.QPushButton("⬇️ بيع (SELL)")
        self.sell_btn.setStyleSheet("background-color: #e74c3c; color: white; font-weight: bold; padding: 8px 12px; border-radius:4px;")
        self.connect_mt5_btn = QtWidgets.QPushButton("🔗 الاتصال بـ MT5")
        self.connect_mt5_btn.setStyleSheet("background-color: #3498db; color: white; font-weight: bold; padding: 8px 12px; border-radius:4px;")
        self.settings_btn = QtWidgets.QPushButton("⚙️ الإعدادات")
        self.settings_btn.setStyleSheet("background-color: #95a5a6; color: white; font-weight: bold; padding: 8px 12px; border-radius:4px;")
        self.close_all_btn = QtWidgets.QPushButton("🛑 إغلاق الكل")
        self.close_all_btn.setStyleSheet("background-color: #f39c12; color: white; font-weight: bold; padding: 8px 12px; border-radius:4px;")

        btn_layout.addWidget(self.buy_btn); btn_layout.addWidget(self.sell_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(self.connect_mt5_btn); btn_layout.addWidget(self.settings_btn)
        btn_layout.addWidget(self.close_all_btn)
        layout.addLayout(btn_layout)

        # Signals Table
        self.signals_table = QtWidgets.QTableWidget(); self.signals_table.setColumnCount(len(self.df_signals_columns))
        self.signals_table.setHorizontalHeaderLabels(["وقت الإشارة", "الرمز", "نوع الإشارة", "الثقة %", "سعر المصدر", "السبريد(نقطة)",
                                                      "TP السعر", "SL السعر", "TP (نقاط)", "SL (نقاط)", "تم التنفيذ؟", "ملاحظات"])
        self.signals_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows); self.signals_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.signals_table.verticalHeader().setVisible(False); self.signals_table.setAlternatingRowColors(True); self.signals_table.setSortingEnabled(True)
        header = self.signals_table.horizontalHeader(); header.setSectionResizeMode(QtWidgets.QHeaderView.ResizeMode.ResizeToContents); header.setStretchLastSection(True); layout.addWidget(self.signals_table)

    def _init_performance_tab_ui(self):
        layout = QtWidgets.QVBoxLayout(self.performance_tab)
        # Top controls (checkbox for logging deals)
        top_controls_layout = QtWidgets.QHBoxLayout()
        self.log_closed_deals_checkbox = QtWidgets.QCheckBox("تفعيل جلب سجل الصفقات من MT5 (لتحليل الأداء)")
        # Checked state will be set in load_app_settings
        top_controls_layout.addWidget(self.log_closed_deals_checkbox)
        top_controls_layout.addStretch()
        layout.addLayout(top_controls_layout)

        # Date and symbol filters
        date_filter_layout = QtWidgets.QHBoxLayout()
        date_filter_layout.addWidget(QtWidgets.QLabel("من:"))
        self.deals_from_date_edit = QtWidgets.QDateEdit(calendarPopup=True)
        self.deals_from_date_edit.setDate(QDate.currentDate().addMonths(-1)) # Default to 1 month ago
        self.deals_from_date_edit.setDisplayFormat("yyyy-MM-dd")
        date_filter_layout.addWidget(self.deals_from_date_edit)
        date_filter_layout.addWidget(QtWidgets.QLabel("إلى:"))
        self.deals_to_date_edit = QtWidgets.QDateEdit(calendarPopup=True)
        self.deals_to_date_edit.setDate(QDate.currentDate()) # Default to today
        self.deals_to_date_edit.setDisplayFormat("yyyy-MM-dd")
        date_filter_layout.addWidget(self.deals_to_date_edit)

        date_filter_layout.addWidget(QtWidgets.QLabel("الرمز:"))
        self.performance_symbol_filter_combo = QtWidgets.QComboBox()
        self.performance_symbol_filter_combo.addItem("الكل") # Default, more added in load_app_settings
        date_filter_layout.addWidget(self.performance_symbol_filter_combo)

        date_filter_layout.addWidget(QtWidgets.QLabel("الرقم السحري:"))
        self.performance_magic_filter_combo = QtWidgets.QComboBox()
        self.performance_magic_filter_combo.addItem("الكل") # Default, more added in load_app_settings
        date_filter_layout.addWidget(self.performance_magic_filter_combo)

        self.apply_date_filter_btn = QtWidgets.QPushButton("🔄 تحديث وعرض الصفقات")
        self.apply_date_filter_btn.setIcon(self.style().standardIcon(QtWidgets.QStyle.StandardPixmap.SP_BrowserReload))
        date_filter_layout.addWidget(self.apply_date_filter_btn)
        date_filter_layout.addStretch()
        self.export_deals_btn = QtWidgets.QPushButton("تصدير إلى CSV")
        self.export_deals_btn.setIcon(self.style().standardIcon(QtWidgets.QStyle.StandardPixmap.SP_DriveHDIcon))
        date_filter_layout.addWidget(self.export_deals_btn)
        layout.addLayout(date_filter_layout)

        # Main splitter for summary/plot and deals table
        main_splitter = QtWidgets.QSplitter(Qt.Orientation.Vertical)
        layout.addWidget(main_splitter)

        # Top part of splitter (summary text and plot)
        top_widget = QtWidgets.QWidget()
        top_layout = QtWidgets.QHBoxLayout(top_widget)

        self.performance_summary_text = QtWidgets.QPlainTextEdit()
        self.performance_summary_text.setReadOnly(True)
        self.performance_summary_text.setFont(QtGui.QFont("Consolas", 10)) # Monospaced font
        self.performance_summary_text.setMinimumWidth(350)
        top_layout.addWidget(self.performance_summary_text, 1) # Proportion 1

        self.performance_plot_canvas = MplCanvas(self, width=7, height=5, dpi=100)
        top_layout.addWidget(self.performance_plot_canvas, 2) # Proportion 2

        main_splitter.addWidget(top_widget)

        # Bottom part of splitter (deals table)
        self.deals_table = QtWidgets.QTableWidget()
        self.deals_table.setColumnCount(11) # Number of columns for deals display
        self.deals_table.setHorizontalHeaderLabels(["وقت الإغلاق", "رقم التذكرة", "الرمز", "نوع الصفقة/الدخول", "الحجم", "سعر الدخول", "سعر الإغلاق", "العمولة", "المبادلة", "الربح", "التعليق"])
        self.deals_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.deals_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.deals_table.verticalHeader().setVisible(False)
        self.deals_table.setAlternatingRowColors(True)
        deals_header = self.deals_table.horizontalHeader()
        deals_header.setSectionResizeMode(QtWidgets.QHeaderView.ResizeMode.ResizeToContents)
        deals_header.setStretchLastSection(True)
        self.deals_table.setSortingEnabled(True)
        main_splitter.addWidget(self.deals_table)

        # Set initial sizes for the splitter
        main_splitter.setSizes([self.height() // 3, (self.height() * 2) // 3])


    def _init_logs_tab_ui(self):
        layout = QtWidgets.QVBoxLayout(self.logs_tab)
        self.log_text_edit = QtWidgets.QTextEdit()
        self.log_text_edit.setReadOnly(True)
        self.log_text_edit.setFont(QtGui.QFont("Consolas", 9)) # Monospaced font for logs
        layout.addWidget(self.log_text_edit)

    def _connect_signals_slots(self):
        self.log_signal_ui.connect(self.append_log_message_to_ui)
        self.refresh_signals_btn.clicked.connect(self.refresh_all_signals_display)
        self.confidence_slider.valueChanged.connect(self.on_confidence_slider_changed)
        self.buy_btn.clicked.connect(lambda: self.execute_trade_from_signal_data(order_type_override="buy"))
        self.sell_btn.clicked.connect(lambda: self.execute_trade_from_signal_data(order_type_override="sell"))
        self.settings_btn.clicked.connect(self.show_settings_dialog)
        self.close_all_btn.clicked.connect(self.confirm_close_all_positions)
        self.apply_date_filter_btn.clicked.connect(self.refresh_performance_stats)
        self.export_deals_btn.clicked.connect(self.export_deals_to_csv)
        self.log_closed_deals_checkbox.stateChanged.connect(self.on_log_closed_deals_toggled)
        self.signals_loaded_signal.connect(self.on_signals_loaded_processed)

        if self.news_manager and hasattr(self.news_manager, 'news_updated'):
            self.news_manager.news_updated.connect(self.on_news_updated_ui)
        else:
            self.log_to_ui_and_logger_wrapper("NewsManager or 'news_updated' signal not available for connection.", "WARNING")

        self.connect_mt5_btn.clicked.connect(lambda: self.toggle_mt5_connection(is_manual_attempt=True))
        self.refresh_signals_timer.timeout.connect(self.refresh_all_signals_display)
        self.position_monitor_timer.timeout.connect(self.monitor_positions_for_auto_close)
        # Connect performance filter changes to refresh stats
        self.performance_symbol_filter_combo.currentTextChanged.connect(self.refresh_performance_stats)
        self.performance_magic_filter_combo.currentTextChanged.connect(self.refresh_performance_stats)

    @QtCore.pyqtSlot(str, str)
    def append_log_message_to_ui(self, msg_from_signal: str, level: str):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        # The logger itself will add the [LEVEL] prefix to the file/console log.
        # For UI, we add it here for clarity if the msg_from_signal doesn't already have it.
        # However, our logger wrapper in TradingApp now calls self.logger which uses the formatter.
        # So msg_from_signal is the raw message.
        full_log_message_for_ui = f"{timestamp} - [{level.upper()}] {msg_from_signal}"


        color_map = {
            "DEBUG": "#7f8c8d",    # Grey
            "INFO": "#2c3e50",     # Dark Blue/Grey
            "WARNING": "#f39c12",  # Orange
            "ERROR": "#e74c3c",    # Red
            "CRITICAL": "#c0392b"  # Dark Red
        }
        text_color = color_map.get(level.upper(), "#2c3e50") # Default to INFO color

        html_message = f'<span style="color:{text_color};">{full_log_message_for_ui}</span>'
        self.log_text_edit.append(html_message)
        # Auto-scroll to the bottom
        self.log_text_edit.verticalScrollBar().setValue(self.log_text_edit.verticalScrollBar().maximum())

        # Status bar message update (optional, can be noisy)
        status_bar_color = ""
        level_upper = level.upper() # Already have this
        if "ERROR" in level_upper or "FAILED" in level_upper or "EXCEPTION" in level_upper or "CRITICAL" in level_upper:
            status_bar_color = "color: #D8000C; background-color: #FFBABA;" # Light red background, dark red text
        elif "WARNING" in level_upper:
            status_bar_color = "color: #9F6000; background-color: #FEEFB3;" # Light yellow background, dark yellow text
        elif "SUCCESS" in level_upper or "CONNECTED" in level_upper or "COMPLETED" in level_upper or "EXECUTED" in level_upper:
            status_bar_color = "color: #4F8A10; background-color: #DFF2BF;" # Light green background, dark green text
        self.statusBar().setStyleSheet(status_bar_color)

        # Display a concise version of the message in the status bar
        core_msg_for_status = msg_from_signal
        # Remove common prefixes if they exist, to make status bar message shorter
        prefixes_to_remove = ["MT5Manager: ", "DataManager: ", "NewsManager: ", f"[{level_upper}] "]
        for prefix in prefixes_to_remove:
            if core_msg_for_status.startswith(prefix):
                core_msg_for_status = core_msg_for_status[len(prefix):]
        self.statusBar().showMessage(core_msg_for_status[:150], 7000) # Show for 7 seconds

    def load_app_settings(self):
        self.data_manager.load_settings() # Load from JSON file
        if hasattr(self.mt5_manager, '_load_config_from_data_manager'): # Ensure MT5Manager gets updated settings
            self.mt5_manager._load_config_from_data_manager()

        # Update UI elements and app behavior based on loaded settings
        self.manual_filter_min_confidence = self.data_manager.get_setting("manual_filter_min_confidence", 70)
        self.confidence_slider.setValue(self.manual_filter_min_confidence) # Update slider position

        # Signals Refresh Timer
        signals_refresh_minutes = self.data_manager.get_setting("signals_refresh_interval_minutes", 15)
        if self.refresh_signals_timer.isActive(): self.refresh_signals_timer.stop()
        self.refresh_signals_timer.start(signals_refresh_minutes * 60 * 1000)
        self.log_to_ui_and_logger_wrapper(f"Signals refresh timer interval set to: {signals_refresh_minutes} min.")

        # Position Monitor Timer
        pos_monitor_interval_sec = self.data_manager.get_setting("position_monitor_interval_seconds", 10) # Default 10s
        if self.position_monitor_timer.isActive(): self.position_monitor_timer.stop()
        self.position_monitor_timer.start(pos_monitor_interval_sec * 1000)
        self.log_to_ui_and_logger_wrapper(f"Position monitor timer interval set to: {pos_monitor_interval_sec} sec.")

        # Account Summary Timer (ensure it's running, default 20s)
        if not self.account_summary_timer.isActive():
            self.account_summary_timer.start(20 * 1000)

        # Model filenames
        self.current_model_filename = self.data_manager.get_setting("current_model_filename", "model_XAUUSD.joblib")
        self.current_btc_model_filename = self.data_manager.get_setting("current_btc_model_filename", "model_BTCUSD.joblib")

        # Deals logging checkbox
        # This will trigger on_log_closed_deals_toggled if the state changes, which calls refresh_performance_stats
        self.log_closed_deals_checkbox.setChecked(self.data_manager.get_setting("log_closed_deals_enabled", True))

        # News Manager settings
        if self.news_manager:
            if hasattr(self.news_manager, '_update_timer_interval'): self.news_manager._update_timer_interval()
            if hasattr(self.news_manager, 'set_enabled'):
                self.news_manager.set_enabled(self.data_manager.get_setting("news_check_enabled", True))

        # Populate performance filter dropdowns
        gold_s = self.data_manager.get_setting("gold_symbol", "XAUUSD")
        btc_s = self.data_manager.get_setting("bitcoin_symbol", "BTCUSD")

        current_symbol_selection = self.performance_symbol_filter_combo.currentText()
        self.performance_symbol_filter_combo.blockSignals(True)
        self.performance_symbol_filter_combo.clear()
        self.performance_symbol_filter_combo.addItem("الكل")
        perf_symbols_to_add = []
        if gold_s: perf_symbols_to_add.append(gold_s)
        if btc_s: perf_symbols_to_add.append(btc_s)
        # Add any other symbols that might be in deals history (more advanced, skip for now)
        for sym_item in sorted(list(set(perf_symbols_to_add))):
            if self.performance_symbol_filter_combo.findText(sym_item) == -1: # Avoid duplicates
                self.performance_symbol_filter_combo.addItem(sym_item)
        idx_sym = self.performance_symbol_filter_combo.findText(current_symbol_selection)
        self.performance_symbol_filter_combo.setCurrentIndex(idx_sym if idx_sym != -1 else 0)
        self.performance_symbol_filter_combo.blockSignals(False)


        current_magic_selection = self.performance_magic_filter_combo.currentText()
        self.performance_magic_filter_combo.blockSignals(True)
        self.performance_magic_filter_combo.clear()
        self.performance_magic_filter_combo.addItem("الكل")
        primary_magic_str = str(self.data_manager.get_setting("mt5_magic_number", 234000))
        if self.performance_magic_filter_combo.findText(primary_magic_str) == -1:
            self.performance_magic_filter_combo.addItem(primary_magic_str)
        # Add other magic numbers from deals history (more advanced, skip for now)
        idx_magic = self.performance_magic_filter_combo.findText(current_magic_selection)
        if idx_magic != -1 and current_magic_selection != "الكل": self.performance_magic_filter_combo.setCurrentText(current_magic_selection)
        elif self.performance_magic_filter_combo.findText(primary_magic_str) != -1 : self.performance_magic_filter_combo.setCurrentText(primary_magic_str)
        else: self.performance_magic_filter_combo.setCurrentIndex(0) # Default to "All" or primary if "All" was selected
        self.performance_magic_filter_combo.blockSignals(False)


        self.log_to_ui_and_logger_wrapper("Application settings loaded/reloaded into TradingApp.", "INFO")


    def connect_to_mt5_on_startup(self):
        self.log_to_ui_and_logger_wrapper("Attempting to connect to MT5 on startup (threaded)...")
        self.toggle_mt5_connection(is_manual_attempt=False)

    def toggle_mt5_connection(self, is_manual_attempt: bool = False):
        if self.mt5_manager.is_connected():
            self.log_to_ui_and_logger_wrapper("Disconnecting from MT5...", "INFO")
            self.mt5_manager.disconnect()
            self.update_mt5_connection_button_state(False)
            self.account_summary_label.setText("🔴 غير متصل بـ MT5.")
            self.account_summary_label.setStyleSheet("font-weight: bold; font-size: 13px; padding: 6px; background-color: #e74c3c; color: white; border: 1px solid #c0392b; border-radius: 4px; qproperty-alignment: 'AlignCenter';")
        else:
            if self.mt5_connect_thread and self.mt5_connect_thread.isRunning():
                self.log_to_ui_and_logger_wrapper("MT5 connection attempt already in progress.", "INFO")
                return

            log_msg = "Attempting to connect to MT5 (manual - threaded)..." if is_manual_attempt else "Attempting to connect to MT5 on startup (threaded)..."
            self.log_to_ui_and_logger_wrapper(log_msg, "INFO")

            self.connect_mt5_btn.setEnabled(False)
            self.connect_mt5_btn.setText("🔗 جارٍ الاتصال...")

            self.mt5_connect_thread = MT5ConnectThread(self.mt5_manager, is_manual_attempt, self)
            self.mt5_connect_thread.connection_signal.connect(self._on_mt5_connection_result)
            self.mt5_connect_thread.finished.connect(self._on_mt5_thread_finished)
            self.mt5_connect_thread.start()

    @QtCore.pyqtSlot(bool, str, bool)
    def _on_mt5_connection_result(self, connected: bool, message: str, is_manual_attempt: bool):
        self.log_to_ui_and_logger_wrapper(f"MT5 Connection result: Connected={connected}, Message='{message}', ManualAttempt={is_manual_attempt}", "DEBUG")
        self.update_mt5_connection_button_state(connected) # Update button text/style
        if connected:
            self.log_to_ui_and_logger_wrapper(f"MT5 connected successfully: {message}", "INFO")
            self.refresh_account_summary() # Update account summary label
            # After successful connection, try to train/check models and refresh signals
            self.test_fetch_historical_data_indicators_and_train_model_threaded() # This will eventually call refresh_all_signals_display
        else:
            self.log_to_ui_and_logger_wrapper(f"Failed to connect to MT5: {message}", "ERROR")
            self.account_summary_label.setText("🔴 فشل الاتصال بـ MT5.") # Update account summary label
            self.account_summary_label.setStyleSheet("font-weight: bold; font-size: 13px; padding: 6px; background-color: #e74c3c; color: white; border: 1px solid #c0392b; border-radius: 4px; qproperty-alignment: 'AlignCenter';")

            if is_manual_attempt: # Only show popup for manual attempts
                # The 'message' from MT5ConnectThread should now be more detailed
                QMessageBox.critical(self, "خطأ اتصال MT5", f"فشل الاتصال بـ MT5.\n{message}")

    @QtCore.pyqtSlot()
    def _on_mt5_thread_finished(self):
        self.log_to_ui_and_logger_wrapper("MT5ConnectThread finished.", "DEBUG")
        if not self.mt5_manager.is_connected(): # Re-enable button if still not connected
            self.connect_mt5_btn.setEnabled(True)
            self.update_mt5_connection_button_state(False) # Ensure button reflects disconnected state
        self.mt5_connect_thread = None # Allow new thread creation

    def update_mt5_connection_button_state(self, connected: bool):
        if connected:
            self.connect_mt5_btn.setText("❌ قطع الاتصال")
            self.connect_mt5_btn.setStyleSheet("background-color: #e74c3c; color: white; font-weight: bold; padding: 8px 12px; border-radius:4px;")
        else:
            self.connect_mt5_btn.setText("🔗 الاتصال بـ MT5")
            self.connect_mt5_btn.setStyleSheet("background-color: #3498db; color: white; font-weight: bold; padding: 8px 12px; border-radius:4px;")
        self.connect_mt5_btn.setEnabled(True) # Always enable after attempt, unless another is running

    def calculate_lot_size_advanced(self, symbol: str, risk_percent: float, sl_pips_for_lot_calc: float) -> float:
        log_prefix = f"LotCalc ({symbol})"
        if not self.mt5_manager or not self.mt5_manager.is_connected() or risk_percent <= 0:
            self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Cannot calculate. MT5 not connected or risk_percent ({risk_percent}) invalid.", "WARNING")
            s_info_min_fallback = self.mt5_manager.get_symbol_info(symbol) if self.mt5_manager and self.mt5_manager.is_connected() else None
            return s_info_min_fallback.volume_min if s_info_min_fallback and hasattr(s_info_min_fallback, 'volume_min') and s_info_min_fallback.volume_min > 0 else 0.01

        account_info = self.mt5_manager.get_account_info()
        if not account_info:
            self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Account info unavailable.", "WARNING")
            s_info_min_fallback = self.mt5_manager.get_symbol_info(symbol)
            return s_info_min_fallback.volume_min if s_info_min_fallback and hasattr(s_info_min_fallback, 'volume_min') and s_info_min_fallback.volume_min > 0 else 0.01

        balance = account_info.balance
        account_currency = account_info.currency
        symbol_info = self.mt5_manager.get_symbol_info(symbol)

        if not symbol_info:
            self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Symbol info unavailable for {symbol}.", "ERROR")
            return 0.01 # Default small lot

        volume_min = getattr(symbol_info, 'volume_min', 0.01)
        volume_max = getattr(symbol_info, 'volume_max', float('inf'))
        volume_step = getattr(symbol_info, 'volume_step', 0.01)
        trade_contract_size = getattr(symbol_info, 'trade_contract_size', 0)
        point_val = getattr(symbol_info, 'point', 0) # Size of one point (e.g., 0.00001 for EURUSD)
        symbol_profit_currency = getattr(symbol_info, 'currency_profit', account_currency) # Currency of profit for this symbol

        # Determine volume precision (digits for lot size)
        if hasattr(symbol_info, 'volume_digits') and isinstance(symbol_info.volume_digits, int) and symbol_info.volume_digits >= 0:
            volume_digits_precision = symbol_info.volume_digits
        else: # Fallback if volume_digits is not available or invalid
            step_str = f"{volume_step:.10f}".rstrip('0') # Format step to string and remove trailing zeros
            volume_digits_precision = len(step_str.split('.')[1]) if '.' in step_str and len(step_str.split('.')[1]) > 0 else 2


        if volume_min <= 0: volume_min = 0.01 # Ensure min_vol is at least 0.01
        if volume_step <= 0: volume_step = 0.01 # Ensure step is valid

        if sl_pips_for_lot_calc <= 0:
            self.log_to_ui_and_logger_wrapper(f"{log_prefix}: SL pips ({sl_pips_for_lot_calc}) non-positive. Using min lot: {volume_min:.{volume_digits_precision}f}.", "WARNING")
            return round(max(volume_min, 0.01), volume_digits_precision)

        risk_amount_in_account_currency = (balance * risk_percent) / 100.0

        if trade_contract_size == 0 or point_val == 0:
            self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Missing or zero contract_size/point for {symbol} (CS: {trade_contract_size}, Pt: {point_val}). Using min lot.", "ERROR")
            return round(max(volume_min, 0.01), volume_digits_precision)

        # Value of 1 point for 1 lot of the symbol, in the symbol's profit currency
        value_one_lot_one_point_profit_curr = trade_contract_size * point_val
        if value_one_lot_one_point_profit_curr == 0:
             self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Calculated point value in profit currency is zero. Using min lot.", "ERROR")
             return round(max(volume_min, 0.01), volume_digits_precision)


        # Convert this value to account currency if different
        value_one_lot_one_point_account_curr = value_one_lot_one_point_profit_curr
        if symbol_profit_currency != account_currency:
            self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Profit currency ({symbol_profit_currency}) differs from account currency ({account_currency}). Attempting conversion.", "DEBUG")
            conversion_rate = 1.0
            # Try ProfitCurrencyAccountCurrency first (e.g., JPYUSD if profit is JPY, account is USD)
            pair1_name = symbol_profit_currency + account_currency
            tick1 = self.mt5_manager.get_tick(pair1_name)
            if tick1 and hasattr(tick1, 'bid') and tick1.bid > 0: # Use bid if we are "selling" profit currency to get account currency
                conversion_rate = tick1.bid
                self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Conversion rate {pair1_name} (bid): {conversion_rate}", "DEBUG")
            else:
                # Try AccountCurrencyProfitCurrency (e.g., USDJPY)
                pair2_name = account_currency + symbol_profit_currency
                tick2 = self.mt5_manager.get_tick(pair2_name)
                if tick2 and hasattr(tick2, 'ask') and tick2.ask > 0: # Use ask if we are "buying" profit currency with account currency (so 1/ask)
                    conversion_rate = 1.0 / tick2.ask
                    self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Conversion rate {pair2_name} (1/ask): {conversion_rate}", "DEBUG")
                else:
                     self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Cannot find direct conversion rate for {symbol_profit_currency} to {account_currency} (tried {pair1_name}, {pair2_name}). Using 1.0 as fallback (may be incorrect).", "WARNING")
            value_one_lot_one_point_account_curr *= conversion_rate

        if value_one_lot_one_point_account_curr == 0:
            self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Point value in account currency is zero after conversion. Using min lot.", "ERROR")
            return round(max(volume_min, 0.01), volume_digits_precision)


        # Total value of the SL in account currency for 1 lot
        sl_value_in_account_currency_for_one_lot = sl_pips_for_lot_calc * value_one_lot_one_point_account_curr
        if sl_value_in_account_currency_for_one_lot == 0:
            self.log_to_ui_and_logger_wrapper(f"{log_prefix}: SL value per lot is zero (SL pips: {sl_pips_for_lot_calc}, point value in acc curr: {value_one_lot_one_point_account_curr:.5f}). Using min lot.", "ERROR")
            return round(max(volume_min, 0.01), volume_digits_precision)


        # Calculate lot size
        calculated_lot = risk_amount_in_account_currency / sl_value_in_account_currency_for_one_lot

        # Adjust to min/max and step
        lot_adjusted = max(calculated_lot, volume_min)
        lot_adjusted = min(lot_adjusted, volume_max)

        if volume_step > 0: # Ensure step is positive
            lot_adjusted = round(lot_adjusted / volume_step) * volume_step # Round to nearest step

        # Final rounding to volume_digits_precision
        final_lot = round(lot_adjusted, volume_digits_precision)

        # Final check against absolute minimum if rounding resulted in zero or less than min_vol
        if final_lot < volume_min : final_lot = volume_min
        if final_lot == 0 and volume_min > 0 : final_lot = volume_min # If rounding to 0, use min_vol


        self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Balance={balance:.2f}{account_currency}, Risk%={risk_percent}, RiskAmt={risk_amount_in_account_currency:.2f}{account_currency}, SLPips={sl_pips_for_lot_calc}, SLValuePerLot={sl_value_in_account_currency_for_one_lot:.2f}{account_currency} -> CalcLot={calculated_lot:.4f} -> FinalLot={final_lot:.{volume_digits_precision}f}", "DEBUG")
        return final_lot


    def refresh_performance_stats(self):
        self.log_to_ui_and_logger_wrapper("Refreshing performance statistics...", "INFO")

        selected_magic_text = self.performance_magic_filter_combo.currentText()
        magic_number_to_filter = None # None means "All" for DataManager.load_deals_history if MT5 is not used
        mt5_magic_filter_param = None # None means "Any" for MT5Manager.get_deals_history

        if selected_magic_text != "الكل":
            if selected_magic_text.isdigit():
                magic_number_to_filter = int(selected_magic_text)
                mt5_magic_filter_param = int(selected_magic_text)
            else: # Invalid text, default to primary magic from settings
                primary_magic_from_settings = self.data_manager.get_setting("mt5_magic_number", 234000)
                magic_number_to_filter = primary_magic_from_settings
                mt5_magic_filter_param = primary_magic_from_settings
                self.log_to_ui_and_logger_wrapper(f"Performance: Magic filter '{selected_magic_text}' invalid, using primary: {magic_number_to_filter}", "WARNING")

        self.log_to_ui_and_logger_wrapper(f"Performance: Using magic number filter: {magic_number_to_filter if magic_number_to_filter is not None else 'All'}", "DEBUG")

        deals_data_source_info = ""
        mt5_deals_df = None

        # Try to fetch from MT5 if connected and logging enabled
        if self.mt5_manager and self.mt5_manager.is_connected() and self.data_manager.get_setting("log_closed_deals_enabled", True):
            self.log_to_ui_and_logger_wrapper(f"Fetching deals history from MT5 for magic: {mt5_magic_filter_param if mt5_magic_filter_param is not None else 'Any'}...", "DEBUG")
            from_date_q = self.deals_from_date_edit.date()
            to_date_q = self.deals_to_date_edit.date()
            # Ensure time part covers the whole day for date range
            from_date_dt = datetime(from_date_q.year(), from_date_q.month(), from_date_q.day(), 0, 0, 0, tzinfo=timezone.utc)
            to_date_dt = datetime(to_date_q.year(), to_date_q.month(), to_date_q.day(), 23, 59, 59, 999999, tzinfo=timezone.utc)

            mt5_deals_df = self.mt5_manager.get_deals_history(from_date=from_date_dt, to_date=to_date_dt, magic=mt5_magic_filter_param)

            if mt5_deals_df is not None:
                self.log_to_ui_and_logger_wrapper(f"MT5Manager.get_deals_history DF Shape: {mt5_deals_df.shape}", "DEBUG")
            else: # mt5_deals_df is None means an error occurred during fetch
                self.log_to_ui_and_logger_wrapper("MT5Manager.get_deals_history returned None (error during fetch).", "WARNING")

            if mt5_deals_df is not None and not mt5_deals_df.empty:
                self.df_deals_history = mt5_deals_df.copy() # Use fresh data from MT5
                deals_data_source_info = f"Using {len(self.df_deals_history)} deals from MT5 (magic: {mt5_magic_filter_param if mt5_magic_filter_param is not None else 'Any'})."
            elif mt5_deals_df is not None and mt5_deals_df.empty: # No deals from MT5 in range/filter
                 deals_data_source_info = f"No deals from MT5 for magic {mt5_magic_filter_param if mt5_magic_filter_param is not None else 'Any'} in range. Using local as fallback."
                 self.df_deals_history = self.data_manager.load_deals_history(magic_filter=magic_number_to_filter) # Load local with appropriate filter
                 deals_data_source_info += f" Loaded {len(self.df_deals_history)} from local." if not self.df_deals_history.empty else " Local empty."
            else: # Error fetching from MT5 (mt5_deals_df is None)
                 deals_data_source_info = f"Failed to fetch from MT5. Using local as fallback."
                 self.df_deals_history = self.data_manager.load_deals_history(magic_filter=magic_number_to_filter)
                 deals_data_source_info += f" Loaded {len(self.df_deals_history)} from local." if not self.df_deals_history.empty else " Local empty."
        else: # MT5 not connected or logging disabled, use local file
            self.df_deals_history = self.data_manager.load_deals_history(magic_filter=magic_number_to_filter)
            deals_data_source_info = f"Using {len(self.df_deals_history)} deals from local (magic: {magic_number_to_filter if magic_number_to_filter is not None else 'All'}). MT5 not connected or logging disabled."

        self.log_to_ui_and_logger_wrapper(deals_data_source_info, "INFO")

        if self.df_deals_history.empty:
            self.log_to_ui_and_logger_wrapper("Deals history empty after fetch/load. Cannot calculate performance.", "INFO")
            self.calculate_and_display_performance_metrics(pd.DataFrame()) # Pass empty DF
            self._populate_deals_table_with_data(pd.DataFrame()) # Clear table
            return

        # Filter by UI date range and symbol for display and calculation
        from_date_ui = datetime(self.deals_from_date_edit.date().year(), self.deals_from_date_edit.date().month(), self.deals_from_date_edit.date().day(), 0,0,0, tzinfo=timezone.utc)
        to_date_ui = datetime(self.deals_to_date_edit.date().year(), self.deals_to_date_edit.date().month(), self.deals_to_date_edit.date().day(), 23,59,59,999999, tzinfo=timezone.utc)

        filtered_df_for_display_and_calc = self.df_deals_history.copy()

        # Determine the time column to use for date filtering (prefer 'close_time', fallback to 'open_time')
        time_col_for_filter = 'close_time'
        primary_time_col_for_perf = 'close_time' # For performance_metrics module
        fallback_time_col_for_perf = 'open_time' # For performance_metrics module

        # Check if primary time_col_for_filter ('close_time') is valid
        if time_col_for_filter not in filtered_df_for_display_and_calc.columns or \
           (pd.api.types.is_datetime64_any_dtype(filtered_df_for_display_and_calc[time_col_for_filter]) and filtered_df_for_display_and_calc[time_col_for_filter].isnull().all()) or \
           (not pd.api.types.is_datetime64_any_dtype(filtered_df_for_display_and_calc[time_col_for_filter]) and pd.to_datetime(filtered_df_for_display_and_calc[time_col_for_filter], errors='coerce').isnull().all()):

            self.log_to_ui_and_logger_wrapper(f"Performance: Primary time column '{time_col_for_filter}' invalid for date filtering. Trying fallback '{fallback_time_col_for_perf}'.", "WARNING")
            if fallback_time_col_for_perf in filtered_df_for_display_and_calc.columns and \
               not pd.to_datetime(filtered_df_for_display_and_calc[fallback_time_col_for_perf], errors='coerce').isnull().all():
                time_col_for_filter = fallback_time_col_for_perf
            else: # No suitable time column at all
                self.log_to_ui_and_logger_wrapper("No suitable time column for date filtering. Displaying all loaded deals after symbol filter (if any).", "ERROR")
                selected_symbol_filter = self.performance_symbol_filter_combo.currentText()
                if selected_symbol_filter != "الكل" and 'symbol' in filtered_df_for_display_and_calc.columns:
                    filtered_df_for_display_and_calc = filtered_df_for_display_and_calc[filtered_df_for_display_and_calc['symbol'] == selected_symbol_filter]
                self._populate_deals_table_with_data(filtered_df_for_display_and_calc)
                self.calculate_and_display_performance_metrics(filtered_df_for_display_and_calc, primary_time_col_for_perf, fallback_time_col_for_perf)
                return

        # Ensure the chosen time_col_for_filter is datetime and UTC
        if not pd.api.types.is_datetime64_any_dtype(filtered_df_for_display_and_calc[time_col_for_filter]):
            filtered_df_for_display_and_calc[time_col_for_filter] = pd.to_datetime(filtered_df_for_display_and_calc[time_col_for_filter], errors='coerce')
        filtered_df_for_display_and_calc.dropna(subset=[time_col_for_filter], inplace=True) # Drop rows where this time column is NaT

        if filtered_df_for_display_and_calc.empty:
            self.log_to_ui_and_logger_wrapper(f"Performance: All deals dropped after NaN removal from chosen time column '{time_col_for_filter}'.", "WARNING")
            self.calculate_and_display_performance_metrics(pd.DataFrame(), primary_time_col_for_perf, fallback_time_col_for_perf)
            self._populate_deals_table_with_data(pd.DataFrame())
            return

        # Convert to UTC if naive, or convert existing timezone to UTC
        if filtered_df_for_display_and_calc[time_col_for_filter].dt.tz is None:
            filtered_df_for_display_and_calc[time_col_for_filter] = filtered_df_for_display_and_calc[time_col_for_filter].dt.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
        else:
            filtered_df_for_display_and_calc[time_col_for_filter] = filtered_df_for_display_and_calc[time_col_for_filter].dt.tz_convert('UTC')
        
        filtered_df_for_display_and_calc.dropna(subset=[time_col_for_filter], inplace=True) # Drop again if tz conversion created NaT
        if filtered_df_for_display_and_calc.empty:
            self.log_to_ui_and_logger_wrapper(f"Performance: All deals dropped after TZ localization/conversion of '{time_col_for_filter}'.", "WARNING")
            self.calculate_and_display_performance_metrics(pd.DataFrame(), primary_time_col_for_perf, fallback_time_col_for_perf)
            self._populate_deals_table_with_data(pd.DataFrame())
            return

        # Apply date range filter
        date_mask = (filtered_df_for_display_and_calc[time_col_for_filter] >= from_date_ui) & \
                    (filtered_df_for_display_and_calc[time_col_for_filter] <= to_date_ui)
        filtered_df_for_display_and_calc = filtered_df_for_display_and_calc[date_mask]

        # Apply symbol filter
        selected_symbol_filter = self.performance_symbol_filter_combo.currentText()
        if selected_symbol_filter != "الكل" and 'symbol' in filtered_df_for_display_and_calc.columns:
            filtered_df_for_display_and_calc = filtered_df_for_display_and_calc[filtered_df_for_display_and_calc['symbol'] == selected_symbol_filter]

        self.log_to_ui_and_logger_wrapper(f"Filtered deals: {len(filtered_df_for_display_and_calc)} for display and calculation (Time col for date filter: '{time_col_for_filter}').", "INFO")

        self._populate_deals_table_with_data(filtered_df_for_display_and_calc)
        self.calculate_and_display_performance_metrics(filtered_df_for_display_and_calc, primary_time_col_for_perf, fallback_time_col_for_perf)


    def _populate_deals_table_with_data(self, df: pd.DataFrame):
        self.deals_table.setUpdatesEnabled(False) # Performance improvement for large updates
        self.deals_table.setRowCount(0) # Clear existing rows

        df_sorted = df.copy()
        # Determine sort column (prefer close_time, then open_time)
        time_col_for_sort = None
        if 'close_time' in df_sorted.columns and pd.api.types.is_datetime64_any_dtype(df_sorted['close_time']) and not df_sorted['close_time'].isnull().all():
            time_col_for_sort = 'close_time'
        elif 'open_time' in df_sorted.columns and pd.api.types.is_datetime64_any_dtype(df_sorted['open_time']) and not df_sorted['open_time'].isnull().all():
            time_col_for_sort = 'open_time'

        if time_col_for_sort and not df_sorted.empty:
            # Ensure it's datetime before sorting
            if not pd.api.types.is_datetime64_any_dtype(df_sorted[time_col_for_sort]):
                 df_sorted[time_col_for_sort] = pd.to_datetime(df_sorted[time_col_for_sort], errors='coerce')
            df_sorted.dropna(subset=[time_col_for_sort], inplace=True) # Remove rows where sort key is NaT
            if not df_sorted.empty:
                df_sorted.sort_values(by=time_col_for_sort, ascending=False, inplace=True, na_position='last')

        for _, deal_row_series in df_sorted.iterrows():
            row_idx = self.deals_table.rowCount()
            self.deals_table.insertRow(row_idx)

            # Column 0: Time (use the sort column, or fallback)
            time_val_utc_to_display = deal_row_series.get(time_col_for_sort) if time_col_for_sort else deal_row_series.get('close_time', deal_row_series.get('open_time'))
            time_str = "N/A"
            if pd.notna(time_val_utc_to_display) and isinstance(time_val_utc_to_display, datetime):
                try:
                    if time_val_utc_to_display.tzinfo is None: # Should be UTC from processing
                        time_val_utc_to_display = time_val_utc_to_display.replace(tzinfo=timezone.utc)
                    time_val_local = time_val_utc_to_display.astimezone(datetime.now().astimezone().tzinfo) # Convert to local
                    time_str = time_val_local.strftime('%Y-%m-%d %H:%M:%S')
                except Exception as e_tz_conv: # Fallback if timezone conversion fails
                    time_str = time_val_utc_to_display.strftime('%Y-%m-%d %H:%M:%S (UTC)')
            elif pd.notna(time_val_utc_to_display): # If it's already a string or other type
                time_str = str(time_val_utc_to_display)
            self.deals_table.setItem(row_idx, 0, QtWidgets.QTableWidgetItem(time_str))

            # Column 1: Ticket
            self.deals_table.setItem(row_idx, 1, QtWidgets.QTableWidgetItem(str(deal_row_series.get('ticket', ''))))

            # Column 2: Symbol
            raw_symbol_value_from_df = deal_row_series.get('symbol')
            symbol_str_deal = str(raw_symbol_value_from_df).strip() if pd.notna(raw_symbol_value_from_df) else "N/A"
            if not symbol_str_deal or symbol_str_deal.lower() in ['nan', 'none', 'deal_no_symbol', '']: # Handle various "empty" representations
                symbol_str_deal = "N/A"
            self.deals_table.setItem(row_idx, 2, QtWidgets.QTableWidgetItem(symbol_str_deal))

            # Column 3: Type/Entry
            deal_type_int = pd.to_numeric(deal_row_series.get('type'), errors='coerce')
            entry_type_int = pd.to_numeric(deal_row_series.get('entry'), errors='coerce')
            type_str = "N/A"; entry_str = "N/A"
            if pd.notna(deal_type_int):
                if deal_type_int == self.mt5_manager._ORDER_TYPE_BUY: type_str = "شراء"
                elif deal_type_int == self.mt5_manager._ORDER_TYPE_SELL: type_str = "بيع"
                elif deal_type_int == 2: type_str = "رصيد" # Balance operation
                # Add other types if necessary
            if pd.notna(entry_type_int):
                if entry_type_int == self.mt5_manager._DEAL_ENTRY_IN: entry_str = "دخول"
                elif entry_type_int == self.mt5_manager._DEAL_ENTRY_OUT: entry_str = "خروج"
                elif entry_type_int == self.mt5_manager._DEAL_ENTRY_INOUT: entry_str = "دخول/خروج"
                elif entry_type_int == getattr(self.mt5_manager, '_DEAL_ENTRY_OUT_BY', 3) : entry_str = "خروج بـ" # Closed by another position
            type_display = f"{type_str} / {entry_str}"
            self.deals_table.setItem(row_idx, 3, QtWidgets.QTableWidgetItem(type_display))

            # Column 4: Volume
            self.deals_table.setItem(row_idx, 4, QtWidgets.QTableWidgetItem(f"{pd.to_numeric(deal_row_series.get('volume', 0.0), errors='coerce'):.2f}"))

            # Column 5: Entry Price (use 'price' from deal)
            deal_price_digits = 5 # Default
            if self.mt5_manager and self.mt5_manager.is_connected() and symbol_str_deal != "N/A":
                deal_symbol_info = self.mt5_manager.get_symbol_info(symbol_str_deal)
                if deal_symbol_info:
                    deal_price_digits = getattr(deal_symbol_info, 'digits', 5)
            deal_execution_price = pd.to_numeric(deal_row_series.get('price'), errors='coerce')
            self.deals_table.setItem(row_idx, 5, QtWidgets.QTableWidgetItem(format_price_display(deal_execution_price, deal_price_digits)))

            # Column 6: Close Price (This is tricky for a single deal row. 'price' is the execution price of THIS deal)
            # If this deal is an 'OUT' deal, its 'price' is effectively the close price of the position.
            # If it's an 'IN' deal, there's no 'close_price' on this row.
            close_price_display = "N/A"
            if pd.notna(entry_type_int) and entry_type_int == self.mt5_manager._DEAL_ENTRY_OUT:
                 close_price_display = format_price_display(deal_execution_price, deal_price_digits) # Price of the OUT deal
            self.deals_table.setItem(row_idx, 6, QtWidgets.QTableWidgetItem(close_price_display))


            # Column 7: Commission
            self.deals_table.setItem(row_idx, 7, QtWidgets.QTableWidgetItem(f"{pd.to_numeric(deal_row_series.get('commission', 0.0), errors='coerce'):.2f}"))
            # Column 8: Swap
            self.deals_table.setItem(row_idx, 8, QtWidgets.QTableWidgetItem(f"{pd.to_numeric(deal_row_series.get('swap', 0.0), errors='coerce'):.2f}"))

            # Column 9: Profit
            profit_val = pd.to_numeric(deal_row_series.get('profit', 0.0), errors='coerce')
            item_profit = QtWidgets.QTableWidgetItem(f"{profit_val:.2f}")
            if pd.notna(profit_val): # Color based on profit value
                if profit_val > 0: item_profit.setForeground(QtGui.QColor("#2ecc71")) # Green
                elif profit_val < 0: item_profit.setForeground(QtGui.QColor("#e74c3c")) # Red
            self.deals_table.setItem(row_idx, 9, item_profit)
            # Column 10: Comment
            self.deals_table.setItem(row_idx, 10, QtWidgets.QTableWidgetItem(str(deal_row_series.get('comment', ''))))

        self.deals_table.setUpdatesEnabled(True) # Re-enable updates


    def calculate_and_display_performance_metrics(self, df_deals_filtered: pd.DataFrame, primary_ts_col: str = 'close_time', fallback_ts_col: str = 'open_time'):
        # Clear previous plot / show loading message
        if hasattr(self, 'performance_plot_canvas') and self.performance_plot_canvas.figure:
            self.performance_plot_canvas.figure.clear()
            ax = self.performance_plot_canvas.figure.add_subplot(111)
            ax.text(0.5, 0.5, "جاري حساب الأداء...", ha='center', va='center', fontsize=12, color='grey')
            ax.set_xticks([]); ax.set_yticks([]) # Remove ticks for loading message
            try: self.performance_plot_canvas.figure.tight_layout(pad=1.0)
            except Exception: pass # Ignore if tight_layout fails here
            self.performance_plot_canvas.draw()

        if hasattr(self, 'performance_summary_text'):
            self.performance_summary_text.setPlainText("جاري حساب ملخص الأداء...")

        if df_deals_filtered.empty:
            self.log_to_ui_and_logger_wrapper("Performance metrics: No deals to analyze.", "INFO")
            if hasattr(self, 'performance_summary_text'):
                self.performance_summary_text.setPlainText("لا توجد صفقات في الفترة المحددة لتحليل الأداء.")
            if hasattr(self, 'performance_plot_canvas') and self.performance_plot_canvas.figure:
                self.performance_plot_canvas.figure.clear()
                ax = self.performance_plot_canvas.figure.add_subplot(111)
                ax.text(0.5, 0.5, "لا توجد بيانات لعرض الرسم البياني", ha='center', va='center', fontsize=12, color='grey')
                ax.set_xticks([]); ax.set_yticks([])
                try: self.performance_plot_canvas.figure.tight_layout(pad=1.0)
                except Exception: pass
                self.performance_plot_canvas.draw()
            return

        # Prepare DataFrame for performance_metrics module
        # It expects deals that represent closed trades (typically DEAL_ENTRY_OUT)
        df_for_summary = df_deals_filtered.copy()
        profit_col = 'profit' # Column name for profit in df_deals_filtered

        if 'entry' in df_for_summary.columns:
            df_for_summary['entry'] = pd.to_numeric(df_for_summary['entry'], errors='coerce')
            # Filter for deals that represent the closing of a trade
            df_for_summary = df_for_summary[df_for_summary['entry'] == self.mt5_manager._DEAL_ENTRY_OUT].copy()
        else:
            self.log_to_ui_and_logger_wrapper("Perf metrics: 'entry' column not found in deals. Summary might include partials or be inaccurate if data is not structured as individual OUT deals.", "WARNING")
            # If no 'entry' column, we assume all deals in df_deals_filtered are relevant as is.
            # This might happen if the input df is already processed.

        self.log_to_ui_and_logger_wrapper(f"Perf metrics: df_for_summary (after filtering for OUT deals if 'entry' col exists) Shape: {df_for_summary.shape}", "DEBUG")

        if df_for_summary.empty:
            self.log_to_ui_and_logger_wrapper("Perf metrics: No 'OUT' deals (or no deals at all after filtering) for summary calculation.", "INFO")
            if hasattr(self, 'performance_summary_text'):
                self.performance_summary_text.setPlainText("لا توجد صفقات خروج (OUT) لتحليل الأداء (أو لا صفقات إطلاقًا بعد الفلترة).")
            if hasattr(self, 'performance_plot_canvas') and self.performance_plot_canvas.figure: # Clear plot or show message
                self.performance_plot_canvas.figure.clear()
                ax = self.performance_plot_canvas.figure.add_subplot(111)
                ax.text(0.5, 0.5, "لا صفقات خروج (OUT) للرسم", ha='center', va='center', fontsize=12, color='orange')
                ax.set_xticks([]); ax.set_yticks([])
                try: self.performance_plot_canvas.figure.tight_layout(pad=1.0)
                except Exception: pass
                self.performance_plot_canvas.draw()
            return

        initial_balance_for_calc = float(self.data_manager.get_setting("default_initial_balance_for_analysis", 10000.0))

        # Call the performance metrics calculation
        summary_dict = performance_metrics.get_performance_summary(
            deals_history_df=df_for_summary.copy(), # Pass the filtered (OUT deals) DataFrame
            initial_balance=initial_balance_for_calc,
            primary_timestamp_col=primary_ts_col, # e.g., 'close_time'
            fallback_timestamp_col=fallback_ts_col, # e.g., 'open_time'
            profit_col=profit_col,
            periods_per_year_for_sharpe=self.data_manager.get_setting("sharpe_periods_per_year", 252) # Example: 252 trading days
        )

        # Display summary text
        summary_text_parts = ["ملخص الأداء:"]
        summary_text_parts.append(f"  الرصيد الأولي للتحليل: {initial_balance_for_calc:.2f}")
        summary_text_parts.append(f"  الرصيد النهائي المحسوب: {summary_dict.get('final_equity', 0.0):.2f}")
        summary_text_parts.append(f"  عمود الوقت المستخدم للملكية: {summary_dict.get('actual_timestamp_col_used', 'غير معروف')}")
        summary_text_parts.append(f"  إجمالي صفقات الخروج (OUT): {summary_dict.get('total_trades', 0)}")
        summary_text_parts.append(f"  صافي الربح الإجمالي: {summary_dict.get('net_profit_total', 0.0):.2f}")
        summary_text_parts.append(f"  صافي الربح (٪ من رصيد التحليل): {summary_dict.get('net_profit_pct', 0.0):.2f}%")
        summary_text_parts.append(f"  إجمالي الربح (Gross Profit): {summary_dict.get('gross_profit', 0.0):.2f}")
        summary_text_parts.append(f"  إجمالي الخسارة (Gross Loss): {summary_dict.get('gross_loss', 0.0):.2f}")
        summary_text_parts.append(f"  عامل الربح: {summary_dict.get('profit_factor', 0.0):.2f}")
        summary_text_parts.append(f"  الصفقات الرابحة: {summary_dict.get('winning_trades', 0)}")
        summary_text_parts.append(f"  الصفقات الخاسرة: {summary_dict.get('losing_trades', 0)}")
        summary_text_parts.append(f"  معدل الربح (٪): {summary_dict.get('win_rate_pct', 0.0):.2f}%")
        summary_text_parts.append(f"  متوسط الربح لكل صفقة خروج: {summary_dict.get('average_profit_per_trade', 0.0):.2f}")
        summary_text_parts.append(f"  متوسط ربح الصفقة الرابحة: {summary_dict.get('average_profit_per_winning_trade', 0.0):.2f}")
        summary_text_parts.append(f"  متوسط خسارة الصفقة الخاسرة: {summary_dict.get('average_loss_per_losing_trade', 0.0):.2f}")
        summary_text_parts.append(f"  أقصى تراجع (٪): {summary_dict.get('max_drawdown_pct', 0.0):.2f}%")
        summary_text_parts.append(f"  نسبة شارب: {summary_dict.get('sharpe_ratio', 0.0):.2f}")
        summary_text_parts.append(f"  أطول سلسلة رابحة: {summary_dict.get('longest_winning_streak', 0)}")
        summary_text_parts.append(f"  أطول سلسلة خاسرة: {summary_dict.get('longest_losing_streak', 0)}")

        if hasattr(self, 'performance_summary_text'):
            self.performance_summary_text.setPlainText("\n".join(summary_text_parts))
        self.log_to_ui_and_logger_wrapper("Performance summary calculated.", "INFO")

        # Plot equity curve and drawdown
        if hasattr(self, 'performance_plot_canvas') and self.performance_plot_canvas.figure and not df_for_summary.empty:
            self.log_to_ui_and_logger_wrapper(f"Generating performance plot (Primary TS for equity: '{primary_ts_col}', Fallback: '{fallback_ts_col}')...", "DEBUG")

            self.performance_plot_canvas.figure.clear() # Clear previous figure content

            equity_curve_for_plot = summary_dict.get('equity_curve_series')
            drawdown_series_for_plot = summary_dict.get('drawdown_percentage_series')

            if equity_curve_for_plot is not None and not equity_curve_for_plot.empty and len(equity_curve_for_plot) >= 2: # Need at least 2 points to plot a line
                if drawdown_series_for_plot is None: # Ensure drawdown series exists even if empty
                    drawdown_series_for_plot = pd.Series(dtype=float, index=equity_curve_for_plot.index)

                performance_metrics.plot_performance_curves(
                    equity_curve=equity_curve_for_plot,
                    drawdown_pct_series=drawdown_series_for_plot,
                    figure_to_plot_on=self.performance_plot_canvas.figure,
                    title_suffix=f"لـ {self.performance_symbol_filter_combo.currentText()}" # Add symbol to title
                )
                try: # Adjust layout to prevent overlap
                    self.performance_plot_canvas.figure.tight_layout(pad=1.5, rect=[0, 0.03, 1, 0.95])
                except Exception as e_tl:
                    self.log_to_ui_and_logger_wrapper(f"Tight layout failed for performance plot: {e_tl}", "WARNING")
                self.performance_plot_canvas.draw()
                self.log_to_ui_and_logger_wrapper("Performance plot generated.", "DEBUG")
            elif equity_curve_for_plot is not None and not equity_curve_for_plot.empty: # Only 1 point, show it
                ax = self.performance_plot_canvas.figure.add_subplot(111)
                ax.plot(equity_curve_for_plot.index, equity_curve_for_plot.values, label='حقوق الملكية (بيانات محدودة)', color='blue', marker='o')
                ax.set_ylabel('حقوق الملكية'); ax.set_title('منحنى حقوق الملكية (بيانات محدودة)'); ax.legend()
                try: self.performance_plot_canvas.figure.tight_layout(pad=1.0)
                except Exception: pass
                self.performance_plot_canvas.draw()
            else: # No data for plot
                ax = self.performance_plot_canvas.figure.add_subplot(111)
                ax.text(0.5, 0.5, "لا توجد بيانات كافية للرسم", ha='center', va='center', fontsize=12, color='grey')
                ax.set_xticks([]); ax.set_yticks([])
                try: self.performance_plot_canvas.figure.tight_layout(pad=1.0)
                except Exception: pass
                self.performance_plot_canvas.draw()
        elif not df_for_summary.empty: # Data exists but no canvas
            self.log_to_ui_and_logger_wrapper("Performance plot canvas not available for plotting.", "WARNING")


    def refresh_account_summary(self):
        if not self.mt5_manager or not self.mt5_manager.is_connected():
            self.account_summary_label.setText("🔴 غير متصل بـ MT5.")
            self.account_summary_label.setStyleSheet("font-weight: bold; font-size: 13px; padding: 6px; background-color: #e74c3c; color: white; border: 1px solid #c0392b; border-radius: 4px; qproperty-alignment: 'AlignCenter';")
            return

        account_info = self.mt5_manager.get_account_info()
        if not account_info:
            self.account_summary_label.setText("⚠️ تعذر جلب بيانات الحساب من MT5.")
            self.account_summary_label.setStyleSheet("font-weight: bold; font-size: 13px; padding: 6px; background-color: #f39c12; color: black; border: 1px solid #d35400; border-radius: 4px; qproperty-alignment: 'AlignCenter';")
            return

        primary_magic = self.data_manager.get_setting("mt5_magic_number", 234000)
        open_pos_count = self.mt5_manager.get_open_positions_count(magic=primary_magic)

        # Determine profit color
        profit_color = "#ecf0f1" # Default (white/light grey) for zero profit
        if account_info.profit > 0: profit_color = "#2ecc71" # Green for positive
        elif account_info.profit < 0: profit_color = "#e74c3c" # Red for negative

        summary_parts = [
            f"الحساب: {account_info.login}", f"الخادم: {account_info.server}",
            f"الرصيد: {account_info.balance:.2f} {account_info.currency}",
            f"الإنصاف: {account_info.equity:.2f} {account_info.currency}",
            f"الربح العائم: <span style='color:{profit_color}; font-weight:bold;'>{account_info.profit:.2f} {account_info.currency}</span>",
            f"الهامش: {account_info.margin:.2f}", f"الهامش المتاح: {account_info.margin_free:.2f}",
            f"مستوى الهامش: {account_info.margin_level:.2f}%" if hasattr(account_info, 'margin_level') and account_info.margin_level and account_info.margin_level > 0 else "N/A",
            f"صفقات البرنامج (Magic {primary_magic}): {open_pos_count}"
        ]
        summary_html = " | ".join(summary_parts)
        self.account_summary_label.setText(f"<span style='color:#2ecc71;'>🟢</span> {summary_html}") # Green dot for connected
        self.account_summary_label.setToolTip(summary_html.replace(" | ", "\n").replace("<span>", "").replace("</span>","")) # Tooltip for details
        self.account_summary_label.setStyleSheet("font-weight: bold; font-size: 13px; padding: 6px; background-color: #34495e; color: #ecf0f1; border: 1px solid #2c3e50; border-radius: 4px; qproperty-alignment: 'AlignCenter';")

    def on_log_closed_deals_toggled(self, state_value):
        is_checked = (state_value == Qt.CheckState.Checked.value)
        
        current_setting_value = self.data_manager.get_setting("log_closed_deals_enabled", True)
        if current_setting_value != is_checked:
            self.data_manager.update_setting("log_closed_deals_enabled", is_checked)
            self.log_to_ui_and_logger_wrapper(f"جلب سجل الصفقات من MT5 الآن {'مفعل' if is_checked else 'معطل'}.", "INFO")
        
        # Always refresh stats when toggled, ensures UI reflects the change
        # This is called by user interaction or programmatically if setChecked triggers it.
        self.refresh_performance_stats()

    def show_settings_dialog(self):
        dlg = SettingsDialog(self.data_manager, parent=self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self.log_to_ui_and_logger_wrapper("Settings dialog accepted. Reloading settings.", "INFO")
            self.load_app_settings() # Reload all settings

            # Reconnect to MT5 if it was connected, to apply new connection settings
            if self.mt5_manager.is_connected():
                self.log_to_ui_and_logger_wrapper("Disconnecting MT5 to apply new connection settings...", "INFO")
                self.mt5_manager.disconnect()
                self.update_mt5_connection_button_state(False) # Update button UI

            # Attempt to reconnect with potentially new settings
            # Treat as manual for user feedback on failure via popup
            self.log_to_ui_and_logger_wrapper("Attempting (re)connect to MT5 with new settings (threaded)...", "INFO")
            self.toggle_mt5_connection(is_manual_attempt=True)

            # Update NewsManager if it exists
            if self.news_manager:
                if hasattr(self.news_manager, '_load_config'): self.news_manager._load_config()
                if hasattr(self.news_manager, '_update_timer_interval'): self.news_manager._update_timer_interval()
                if hasattr(self.news_manager, 'set_enabled'):
                    self.news_manager.set_enabled(self.data_manager.get_setting("news_check_enabled", True))

            # Refresh displays that might depend on new settings
            self.refresh_all_signals_display() # Signals might depend on symbols
            self.refresh_performance_stats() # Performance might depend on magic number, symbols
        else:
            self.log_to_ui_and_logger_wrapper("Settings dialog cancelled.", "INFO")

    def auto_analysis_summary_for_logs(self):
        try:
            if self.df_signals.empty:
                self.log_to_ui_and_logger_wrapper("Auto-analysis: No signals available for summary.", "DEBUG")
                return

            df_signals_copy = self.df_signals.copy()
            if 'executed' not in df_signals_copy.columns:
                df_signals_copy['executed'] = False # Assume not executed if column missing
            else:
                # Ensure 'executed' is boolean
                df_signals_copy['executed'] = df_signals_copy['executed'].apply(
                    lambda x: x if isinstance(x, bool) else str(x).strip().lower() in ['true', 'yes', '1', 'نعم', '1.0']
                )
            total_signals = len(df_signals_copy)
            executed_signals_df = df_signals_copy[df_signals_copy["executed"] == True]
            executed_count = len(executed_signals_df)

            self.log_to_ui_and_logger_wrapper(f"Auto-analysis Summary: Total Signals Processed/Available: {total_signals} | Signals Marked as Executed: {executed_count}", "INFO")
        except Exception as e:
            self.log_to_ui_and_logger_wrapper(f"Error in auto_analysis_summary: {e}\n{traceback.format_exc()}", "ERROR")


    def export_deals_to_csv(self):
        magic_filter_export_text = self.performance_magic_filter_combo.currentText()
        magic_filter_export = None
        if magic_filter_export_text != "الكل" and magic_filter_export_text.isdigit():
            magic_filter_export = int(magic_filter_export_text)

        # Load base deals for export - always from local file to ensure all history is available
        # regardless of MT5 connection or UI date range for display.
        # The UI date range and symbol filter will be applied AFTER loading.
        base_deals_for_export = self.data_manager.load_deals_history(magic_filter=magic_filter_export)

        if base_deals_for_export.empty:
            QMessageBox.information(self, "لا توجد بيانات", f"لا صفقات مسجلة (للرقم السحري: {magic_filter_export_text}) للتصدير من الملف المحلي.")
            return

        # Apply UI date filters
        from_date_ui = datetime(self.deals_from_date_edit.date().year(), self.deals_from_date_edit.date().month(), self.deals_from_date_edit.date().day(), 0,0,0, tzinfo=timezone.utc)
        to_date_ui = datetime(self.deals_to_date_edit.date().year(), self.deals_to_date_edit.date().month(), self.deals_to_date_edit.date().day(), 23,59,59,999999, tzinfo=timezone.utc)

        export_df = base_deals_for_export.copy()

        # Determine time column for filtering (prefer close_time, fallback to open_time)
        time_col_export = 'close_time'
        if time_col_export not in export_df.columns or pd.to_datetime(export_df[time_col_export], errors='coerce').isnull().all():
            if 'open_time' in export_df.columns and not pd.to_datetime(export_df['open_time'], errors='coerce').isnull().all():
                time_col_export = 'open_time'
            else:
                time_col_export = None # No valid time column

        if time_col_export and not export_df.empty:
            # Ensure datetime and UTC
            if not pd.api.types.is_datetime64_any_dtype(export_df[time_col_export]):
                export_df[time_col_export] = pd.to_datetime(export_df[time_col_export], errors='coerce')
            export_df.dropna(subset=[time_col_export], inplace=True)

            if not export_df.empty: # Check again after dropna
                if export_df[time_col_export].dt.tz is None:
                    export_df[time_col_export] = export_df[time_col_export].dt.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
                else:
                    export_df[time_col_export] = export_df[time_col_export].dt.tz_convert('UTC')
                export_df.dropna(subset=[time_col_export], inplace=True) # Drop again if tz conversion created NaT

            if not export_df.empty: # Check again
                date_mask_export = (export_df[time_col_export] >= from_date_ui) & (export_df[time_col_export] <= to_date_ui)
                export_df = export_df[date_mask_export]

        # Apply symbol filter
        symbol_filter_export = self.performance_symbol_filter_combo.currentText()
        if symbol_filter_export != "الكل" and 'symbol' in export_df.columns:
            export_df = export_df[export_df['symbol'] == symbol_filter_export]

        if export_df.empty:
            QMessageBox.information(self, "لا توجد بيانات", "لا صفقات في النطاق/الفلاتر المحددة للتصدير.")
            return

        # Suggest filename
        magic_for_filename = str(magic_filter_export) if magic_filter_export is not None else "all"
        symbol_for_filename = symbol_filter_export if symbol_filter_export != "الكل" else "all"
        default_filename = f"deals_export_{from_date_ui.strftime('%Y%m%d')}_{to_date_ui.strftime('%Y%m%d')}_sym_{symbol_for_filename}_magic_{magic_for_filename}.csv"
        filePath, _ = QtWidgets.QFileDialog.getSaveFileName(self, "تصدير سجل الصفقات إلى CSV", default_filename, "CSV Files (*.csv);;All Files (*)")

        if filePath:
            try:
                # Format datetime columns for better readability in CSV if they are timezone-aware
                export_df_display = export_df.copy()
                if 'open_time' in export_df_display.columns and pd.api.types.is_datetime64_any_dtype(export_df_display['open_time']):
                    export_df_display['open_time'] = export_df_display['open_time'].dt.strftime('%Y-%m-%d %H:%M:%S %Z')
                if 'close_time' in export_df_display.columns and pd.api.types.is_datetime64_any_dtype(export_df_display['close_time']):
                     export_df_display['close_time'] = export_df_display['close_time'].dt.strftime('%Y-%m-%d %H:%M:%S %Z')
                # Add other datetime columns if needed

                export_df_display.to_csv(filePath, index=False, encoding='utf-8-sig') # utf-8-sig for Excel compatibility with Arabic
                self.log_to_ui_and_logger_wrapper(f"Exported {len(export_df_display)} deals to: {filePath}", "INFO")
                QMessageBox.information(self, "تم التصدير", f"تم تصدير الصفقات إلى:\n{filePath}")
            except Exception as e:
                self.log_to_ui_and_logger_wrapper(f"Failed to export deals: {e}", "ERROR")
                self.logger.error(traceback.format_exc())
                QMessageBox.critical(self, "خطأ في التصدير", f"فشل التصدير: {e}")


    def test_fetch_historical_data_indicators_and_train_model_threaded(self):
        self.model_trained_this_session_flags = {"GOLD_MODEL": False, "BITCOIN_MODEL": False} # Reset flags
        thread = Thread(target=self._run_data_fetch_and_model_training_test, name="ModelTrainingThread", daemon=True)
        thread.start()

    def _run_data_fetch_and_model_training_test(self):
        self.log_to_ui_and_logger_wrapper("START: Model Check/Train (Threaded)", "DEBUG")
        if not self.mt5_manager or not self.mt5_manager.is_connected():
            self.log_to_ui_and_logger_wrapper("MT5 not connected. Cannot train/check models.", "WARNING")
            # Even if MT5 is not connected, we should still try to load existing signals
            # and proceed with on_signals_loaded_processed.
            # The generate_signal_from_model will handle the MT5 not connected state.
            QtCore.QMetaObject.invokeMethod(self, "refresh_all_signals_display_slot", Qt.ConnectionType.QueuedConnection)
            return

        self.log_to_ui_and_logger_wrapper("--- Starting Model Check/Training (MT5 Connected) ---", "INFO")
        symbols_to_process = {
            "GOLD_MODEL": {"symbol_key": "gold_symbol", "default_symbol": "XAUUSD", "model_file_key": "current_model_filename"},
            "BITCOIN_MODEL": {"symbol_key": "bitcoin_symbol", "default_symbol": "BTCUSD", "model_file_key": "current_btc_model_filename"}
        }

        for model_key, config in symbols_to_process.items():
            symbol_code = self.data_manager.get_setting(config["symbol_key"], config["default_symbol"])
            self.log_to_ui_and_logger_wrapper(f"Processing model: {model_key} (Symbol: {symbol_code})", "DEBUG")

            if not symbol_code: # Skip if symbol is not configured
                self.log_to_ui_and_logger_wrapper(f"Skipping {model_key}: symbol '{config['symbol_key']}' not set or empty.", "WARNING")
                continue

            model_filename_for_symbol = self.data_manager.get_setting(config["model_file_key"], f"model_{symbol_code.replace('/', '_')}.joblib")

            # Fetch data for training/checking
            self.log_to_ui_and_logger_wrapper(f"Fetching 300 M15 candles for {symbol_code} ({model_key})...", "DEBUG")
            hist_df_original = self.mt5_manager.get_historical_data(symbol=symbol_code, timeframe_str="M15", count=300)

            if hist_df_original.empty:
                self.log_to_ui_and_logger_wrapper(f"No historical data for {symbol_code} ({model_key}). Skipping model processing for this symbol.", "WARNING")
                continue
            self.log_to_ui_and_logger_wrapper(f"Fetched {len(hist_df_original)} candles for {symbol_code}.", "DEBUG")

            df_with_indicators = add_technical_indicators(hist_df_original.copy(), log_callback=self.log_to_ui_and_logger_wrapper)
            if df_with_indicators.empty:
                self.log_to_ui_and_logger_wrapper(f"Failed to add indicators for {symbol_code} ({model_key}). Skipping.", "WARNING")
                continue

            df_with_target = create_target_variable(df_with_indicators.copy(), log_callback=self.log_to_ui_and_logger_wrapper)
            df_for_training = df_with_target.dropna(subset=['Target']).copy() # Ensure Target exists and is not NaN for training

            if df_for_training.empty or 'Target' not in df_for_training.columns:
                self.log_to_ui_and_logger_wrapper(f"Failed to create target or data empty after target creation for {symbol_code} ({model_key}). Skipping.", "WARNING")
                continue

            self.log_to_ui_and_logger_wrapper(f"Data prepared for training {symbol_code}. Shape: {df_for_training.shape}", "DEBUG")

            train_this_model_now = False
            if not os.path.exists(model_filename_for_symbol):
                self.log_to_ui_and_logger_wrapper(f"Model {model_filename_for_symbol} for {model_key} not found. Forcing training.", "INFO")
                train_this_model_now = True
            else:
                 # Could add a check for model age here if re-training periodically is desired
                 self.log_to_ui_and_logger_wrapper(f"Model {model_filename_for_symbol} for {model_key} EXISTS. Will use existing.", "DEBUG")

            if train_this_model_now:
                self.log_to_ui_and_logger_wrapper(f"Training model for {model_key} ({symbol_code}), saving to {model_filename_for_symbol}...", "INFO")
                saved_model_path = train_and_save_model( # This function is from utils.py
                    df_for_training.copy(),
                    model_filename=model_filename_for_symbol,
                    log_callback=self.log_to_ui_and_logger_wrapper
                )
                if saved_model_path and os.path.exists(saved_model_path):
                    self.log_to_ui_and_logger_wrapper(f"Model for {model_key} ({symbol_code}) trained/saved as {saved_model_path}.", "INFO")
                    # Update the active model filename in settings if it was just trained
                    if model_key == "GOLD_MODEL":
                        self.current_model_filename = saved_model_path # Update instance variable
                        self.data_manager.update_setting("current_model_filename", saved_model_path) # Update settings file
                    elif model_key == "BITCOIN_MODEL":
                        self.current_btc_model_filename = saved_model_path
                        self.data_manager.update_setting("current_btc_model_filename", saved_model_path)
                    self.model_trained_this_session_flags[model_key] = True
                else:
                    self.log_to_ui_and_logger_wrapper(f"Model training/saving FAILED for {model_key} ({symbol_code}). Path: {saved_model_path}", "ERROR")

            self.log_to_ui_and_logger_wrapper(f"--- Finished processing model {model_key} ({symbol_code}) ---", "INFO")

        self.log_to_ui_and_logger_wrapper("--- Model Check/Training Finished (MT5 Connected) ---", "INFO")
        # After model checks/training, refresh signals which will use these models
        QtCore.QMetaObject.invokeMethod(self, "refresh_all_signals_display_slot", Qt.ConnectionType.QueuedConnection)
        self.log_to_ui_and_logger_wrapper("END: Model Check/Train (Threaded)", "DEBUG")


    def generate_signal_from_model(self, symbol="XAUUSD", model_type="GOLD", timeframe_str="M15", candles_to_fetch=300) -> dict | None:
        log_prefix = f"ModelSignalGen ({model_type} - {symbol})"
        if not self.mt5_manager or not self.mt5_manager.is_connected():
            self.log_to_ui_and_logger_wrapper(f"{log_prefix}: MT5 not connected. Cannot generate signal.", "WARNING")
            return None

        active_model_filename_to_use = None
        if model_type == "GOLD":
            active_model_filename_to_use = self.data_manager.get_setting("current_model_filename", f"model_{symbol.replace('/', '_')}.joblib")
        elif model_type == "BITCOIN":
            active_model_filename_to_use = self.data_manager.get_setting("current_btc_model_filename", f"model_{symbol.replace('/', '_')}.joblib")
        else:
            self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Unknown model_type '{model_type}'. Cannot generate signal.", "ERROR")
            return None

        if not active_model_filename_to_use or not os.path.exists(active_model_filename_to_use):
            self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Model '{active_model_filename_to_use}' for {model_type} is missing. Triggering training check.", "ERROR")
            # Avoid recursive calls if training check itself is failing or MT5 is down
            # The training check is already called after successful MT5 connection.
            # If model is still missing, it means training failed or data was unavailable.
            # QtCore.QMetaObject.invokeMethod(self, "test_fetch_historical_data_indicators_and_train_model_threaded_slot", Qt.ConnectionType.QueuedConnection)
            return None

        self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Using model: {active_model_filename_to_use} for signal generation...", "INFO")

        live_data_df = self.mt5_manager.get_historical_data(symbol, timeframe_str, count=candles_to_fetch)
        if live_data_df.empty:
            self.log_to_ui_and_logger_wrapper(f"{log_prefix}: No live data fetched for prediction ({symbol}).", "WARNING")
            return None

        live_data_with_indicators = add_technical_indicators(live_data_df.copy(), log_callback=self.log_to_ui_and_logger_wrapper)
        if live_data_with_indicators.empty:
            self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Failed to add indicators to live data for prediction ({symbol}).", "WARNING")
            return None

        # Use the latest row for prediction
        features_for_prediction_df = live_data_with_indicators.iloc[[-1]] # Get last row as DataFrame
        if features_for_prediction_df.empty:
            self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Latest features row is empty after indicator addition for prediction ({symbol}).", "WARNING")
            return None

        # Get current spread (in points)
        current_spread_points = self.mt5_manager.get_current_spread(symbol)
        if current_spread_points is None: # If MT5Manager returns None (e.g. error)
            current_spread_points = 0 # Default to 0 if undetermined, or handle as error
            self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Could not get current spread for {symbol} via MT5Manager. Using 0.", "WARNING")

        # Predict
        prediction_array, probability_buy_array = load_model_and_predict( # From utils.py
            features_for_prediction_df,
            model_filename=active_model_filename_to_use,
            log_callback=self.log_to_ui_and_logger_wrapper
        )

        if prediction_array is not None and probability_buy_array is not None and len(prediction_array) > 0:
            latest_prediction_val = prediction_array[-1] # Prediction: 1 for buy, 0 for sell
            latest_prob_buy_val = probability_buy_array[-1] # Probability of buy class
            signal_text_val = "buy" if latest_prediction_val == 1 else "sell"
            confidence_percent = (latest_prob_buy_val * 100) if latest_prediction_val == 1 else ((1 - latest_prob_buy_val) * 100)

            latest_full_row_for_price = features_for_prediction_df.iloc[0] # Get the Series for the last row
            timestamp_val = latest_full_row_for_price.get('Timestamp', pd.Timestamp.now(tz='UTC')) # Timestamp of the candle
            # Ensure timestamp is timezone-aware (UTC)
            if not isinstance(timestamp_val, datetime): # Convert if not already datetime (e.g. from string)
                timestamp_val = pd.to_datetime(timestamp_val, errors='coerce')
                if pd.isna(timestamp_val): timestamp_val = pd.Timestamp.now(tz='UTC') # Fallback
            if timestamp_val.tzinfo is None:
                timestamp_val = timestamp_val.tz_localize('UTC')


            close_price_val = latest_full_row_for_price.get('Close', 0.0) # Close price of the candle

            # Get SL/TP pips from settings, specific to symbol if available
            sl_pips_setting = self.data_manager.get_setting("default_sl_pips", 50)
            tp_pips_setting = self.data_manager.get_setting("default_tp_pips", 100)
            gold_cfg_symbol = self.data_manager.get_setting("gold_symbol", "XAUUSD")
            btc_cfg_symbol = self.data_manager.get_setting("bitcoin_symbol", "BTCUSD")

            if symbol == gold_cfg_symbol:
                sl_pips_setting = self.data_manager.get_setting("gold_sl_pips", sl_pips_setting)
                tp_pips_setting = self.data_manager.get_setting("gold_tp_pips", tp_pips_setting)
            elif symbol == btc_cfg_symbol:
                sl_pips_setting = self.data_manager.get_setting("btc_sl_pips", sl_pips_setting)
                tp_pips_setting = self.data_manager.get_setting("btc_tp_pips", tp_pips_setting)
            # Add more symbol-specific settings if needed

            # Calculate SL/TP prices
            sl_price_calculated, tp_price_calculated = 0.0, 0.0
            symbol_trade_info = self.mt5_manager.get_symbol_info(symbol)
            digits_s = getattr(symbol_trade_info, 'digits', 5) if symbol_trade_info else 5 # Price decimal places
            if symbol_trade_info and hasattr(symbol_trade_info, 'point') and symbol_trade_info.point > 0 and close_price_val > 0:
                point_s = symbol_trade_info.point # Size of one point
                if signal_text_val == "buy":
                    if sl_pips_setting > 0: sl_price_calculated = round(close_price_val - (sl_pips_setting * point_s), digits_s)
                    if tp_pips_setting > 0: tp_price_calculated = round(close_price_val + (tp_pips_setting * point_s), digits_s)
                elif signal_text_val == "sell":
                    if sl_pips_setting > 0: sl_price_calculated = round(close_price_val + (sl_pips_setting * point_s), digits_s)
                    if tp_pips_setting > 0: tp_price_calculated = round(close_price_val - (tp_pips_setting * point_s), digits_s)
            else:
                self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Could not calculate SL/TP prices for {symbol} (missing symbol_info, point, or valid close_price). SL/TP will be 0.", "WARNING")

            self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Predicted: {signal_text_val.upper()} | Conf: {confidence_percent:.2f}% at {timestamp_val.strftime('%Y-%m-%d %H:%M:%S %Z')} for {symbol}. SLPips:{sl_pips_setting}, TPPips:{tp_pips_setting}. Spread:{current_spread_points}pts", "INFO")

            return {"time": timestamp_val,
                    "Symbol": symbol, "signal": signal_text_val,
                    "confidence_%": round(confidence_percent, 2), "close": round(close_price_val, digits_s),
                    "spread_pips": current_spread_points, # Store spread in points
                    "take_profit_price": tp_price_calculated, "stop_loss_price": sl_price_calculated,
                    "take_profit_pips": tp_pips_setting if tp_price_calculated > 0 else 0, # Store pips used for calc
                    "stop_loss_pips": sl_pips_setting if sl_price_calculated > 0 else 0,   # Store pips used for calc
                    "executed": False, # Default not executed
                    "notes": f"Model: {os.path.basename(active_model_filename_to_use)}"}
        else:
            self.log_to_ui_and_logger_wrapper(f"{log_prefix}: Prediction failed for {symbol} (model did not return valid prediction/probability).", "WARNING")
            return None


    @QtCore.pyqtSlot()
    def test_fetch_historical_data_indicators_and_train_model_threaded_slot(self):
        # This slot is primarily for direct invocation if needed, e.g., from a button for testing.
        # Normal flow is self.test_fetch_historical_data_indicators_and_train_model_threaded()
        self.test_fetch_historical_data_indicators_and_train_model_threaded()

    @QtCore.pyqtSlot()
    def refresh_all_signals_display_slot(self): # Slot for QueuedConnection
        self.refresh_all_signals_display()

    def refresh_all_signals_display(self):
        self.log_to_ui_and_logger_wrapper("Refreshing all signals (from CSV & generating new model signals)...", "INFO")
        self.load_signals_async() # Start async load of CSV signals

    def load_signals_async(self):
        # This will load CSV signals and then trigger on_signals_loaded_processed in the main thread.
        thread = Thread(target=self._load_signals_job_for_async, name="LoadSignalsAsyncThread", daemon=True)
        thread.start()

    def _load_signals_job_for_async(self):
        self.log_to_ui_and_logger_wrapper("Loading signals from CSV (async thread)...", "DEBUG")
        try:
            df_from_csv = self.data_manager.load_signals() # This should handle if file doesn't exist
            # Ensure 'time' column is datetime if it exists
            if df_from_csv is not None and 'time' in df_from_csv.columns:
                 df_from_csv['time'] = pd.to_datetime(df_from_csv['time'], errors='coerce')
            # Emit the loaded DataFrame (or an empty one if load failed or no file)
            self.signals_loaded_signal.emit(df_from_csv if df_from_csv is not None else pd.DataFrame(columns=self.df_signals_columns))
        except Exception as e:
            self.log_to_ui_and_logger_wrapper(f"Error in _load_signals_job_for_async loading CSV: {e}\n{traceback.format_exc()}", "ERROR")
            self.signals_loaded_signal.emit(pd.DataFrame(columns=self.df_signals_columns)) # Emit empty on error

    @QtCore.pyqtSlot(pd.DataFrame)
    def on_signals_loaded_processed(self, df_from_csv: pd.DataFrame):
        self.log_to_ui_and_logger_wrapper("START: on_signals_loaded_processed (Main Thread) - Combining CSV and new model signals.", "DEBUG")

        # Ensure df_from_csv has correct columns and 'time' is datetime
        if df_from_csv.empty:
            current_csv_signals = pd.DataFrame(columns=self.df_signals_columns)
        else:
            current_csv_signals = df_from_csv.copy()
        # Ensure 'time' column exists and is datetime
        if 'time' not in current_csv_signals.columns: current_csv_signals['time'] = pd.NaT
        current_csv_signals['time'] = pd.to_datetime(current_csv_signals['time'], errors='coerce')
        # Ensure all expected columns exist, fill with defaults if not
        for col in self.df_signals_columns:
            if col not in current_csv_signals.columns:
                if col == "executed": current_csv_signals[col] = False
                elif col in ["notes", "signal", "Symbol"]: current_csv_signals[col] = ""
                elif col == "time": current_csv_signals[col] = pd.NaT # Should be handled already
                elif col == "spread_pips": current_csv_signals[col] = 0
                else: current_csv_signals[col] = 0.0 # Default for numeric like confidence, prices, pips
        current_csv_signals = current_csv_signals.reindex(columns=self.df_signals_columns) # Ensure column order


        self.log_to_ui_and_logger_wrapper(f"CSV signals (post-proc) - Shape: {current_csv_signals.shape}", "DEBUG")

        # Generate new model signals (only if MT5 is connected, generate_signal_from_model handles this)
        newly_generated_model_signals_list = []
        gold_symbol_name = self.data_manager.get_setting("gold_symbol", "XAUUSD")
        if gold_symbol_name: # Only generate if symbol is configured
            model_signal_gold = self.generate_signal_from_model(symbol=gold_symbol_name, model_type="GOLD")
            if model_signal_gold: newly_generated_model_signals_list.append(model_signal_gold)

        bitcoin_symbol_name = self.data_manager.get_setting("bitcoin_symbol", "BTCUSD")
        if bitcoin_symbol_name: # Only generate if symbol is configured
            model_signal_bitcoin = self.generate_signal_from_model(symbol=bitcoin_symbol_name, model_type="BITCOIN")
            if model_signal_bitcoin: newly_generated_model_signals_list.append(model_signal_bitcoin)
        # Add more model signal generations here if needed

        newly_generated_model_signals_df = pd.DataFrame()
        if newly_generated_model_signals_list:
            newly_generated_model_signals_df = pd.DataFrame(newly_generated_model_signals_list)
            # Ensure all columns exist in newly generated signals df
            for col in self.df_signals_columns:
                if col not in newly_generated_model_signals_df.columns:
                    if col == "executed": newly_generated_model_signals_df[col] = False
                    elif col in ["notes", "signal", "Symbol"]: newly_generated_model_signals_df[col] = ""
                    elif col == "time": newly_generated_model_signals_df[col] = pd.NaT
                    elif col == "spread_pips": newly_generated_model_signals_df[col] = 0
                    else: newly_generated_model_signals_df[col] = 0.0
            newly_generated_model_signals_df = newly_generated_model_signals_df.reindex(columns=self.df_signals_columns)


        self.log_to_ui_and_logger_wrapper(f"Newly generated model signals - Shape: {newly_generated_model_signals_df.shape}", "DEBUG")

        # Combine CSV signals with newly generated ones
        temp_combined_list = []
        if not current_csv_signals.empty: temp_combined_list.append(current_csv_signals)
        if not newly_generated_model_signals_df.empty: temp_combined_list.append(newly_generated_model_signals_df)

        if temp_combined_list:
            combined_df = pd.concat(temp_combined_list, ignore_index=True)
        else: # If both are empty
            combined_df = pd.DataFrame(columns=self.df_signals_columns)
        
        if 'time' not in combined_df.columns: combined_df['time'] = pd.NaT # Ensure time col for empty case
        combined_df['time'] = pd.to_datetime(combined_df['time'], errors='coerce') # Ensure datetime type

        self.log_to_ui_and_logger_wrapper(f"Combined signals (pre-dedupe) - Shape: {combined_df.shape}", "DEBUG")

        # Deduplicate: Keep latest signal for same time & symbol, prioritizing by confidence if times are identical (though unlikely for model signals)
        if not combined_df.empty:
            combined_df.dropna(subset=['time', 'Symbol'], inplace=True) # Must have time and symbol

            if not combined_df.empty: # Check again after dropna
                # Sort to ensure 'first' kept is the most confident for exact time duplicates, or latest if times differ slightly
                if 'confidence_%' not in combined_df.columns: combined_df['confidence_%'] = 0.0
                combined_df['confidence_%'] = pd.to_numeric(combined_df['confidence_%'], errors='coerce').fillna(0.0)
                combined_df.sort_values(by=['time', 'confidence_%'], ascending=[False, False], inplace=True, na_position='last')
                combined_df.drop_duplicates(subset=['time', 'Symbol'], keep='first', inplace=True)
                self.log_to_ui_and_logger_wrapper(f"Combined signals (post-dedupe) - Shape: {combined_df.shape}", "DEBUG")

        # Assign to self.df_signals and ensure dtypes
        self.df_signals = combined_df.reindex(columns=self.df_signals_columns).copy()
        if 'time' in self.df_signals.columns: self.df_signals['time'] = pd.to_datetime(self.df_signals['time'], errors='coerce')
        if 'confidence_%' in self.df_signals.columns: self.df_signals['confidence_%'] = pd.to_numeric(self.df_signals['confidence_%'], errors='coerce').fillna(0.0)
        if 'executed' in self.df_signals.columns: # Ensure boolean
            self.df_signals['executed'] = self.df_signals['executed'].apply(lambda x: x if isinstance(x, bool) else str(x).lower() in ['true', 'yes', '1', 'نعم', '1.0'])
        else: self.df_signals['executed'] = False
        if 'notes' not in self.df_signals.columns: self.df_signals['notes'] = ""
        if 'spread_pips' in self.df_signals.columns: self.df_signals['spread_pips'] = pd.to_numeric(self.df_signals['spread_pips'], errors='coerce').fillna(0).astype(int)


        self.log_to_ui_and_logger_wrapper(f"Final self.df_signals assigned - Shape: {self.df_signals.shape}", "INFO")

        # Auto-execute new model signals if conditions met
        if not newly_generated_model_signals_df.empty:
            self.log_to_ui_and_logger_wrapper(f"Processing {len(newly_generated_model_signals_df)} new model signals for auto-execution...", "DEBUG")
            for _, model_signal_series in newly_generated_model_signals_df.iterrows():
                self.auto_execute_model_signal_if_conditions_met(model_signal_series.copy()) # Pass a copy

        # Update UI table, save combined signals, refresh account summary
        self.filter_and_display_signals_in_table() # Update UI
        self.data_manager.save_signals(self.df_signals) # Save combined to CSV

        if self.mt5_manager and self.mt5_manager.is_connected(): self.refresh_account_summary()
        self.auto_analysis_summary_for_logs() # Log summary of executed signals
        self.log_to_ui_and_logger_wrapper("FINISHED: on_signals_loaded_processed", "DEBUG")

    def filter_and_display_signals_in_table(self):
        min_conf = self.confidence_slider.value()
        self.confidence_label.setText(f"{min_conf}%") # Update label next to slider

        if self.df_signals.empty:
            self.signals_table.setRowCount(0)
            return

        display_df = self.df_signals.copy()
        if 'confidence_%' in display_df.columns:
            display_df['confidence_%'] = pd.to_numeric(display_df['confidence_%'], errors='coerce').fillna(0)
            filtered_df = display_df[display_df["confidence_%"] >= min_conf].copy()
        else: # Should not happen if columns are ensured
            self.log_to_ui_and_logger_wrapper("Signals Table: 'confidence_%' column missing. Displaying all signals.", "WARNING")
            filtered_df = display_df.copy()

        # Ensure 'time' is datetime for sorting, if it exists
        if 'time' in filtered_df.columns:
            filtered_df['time'] = pd.to_datetime(filtered_df['time'], errors='coerce')
        # else: filtered_df['time'] = pd.NaT # Add if missing, for consistent sort

        self._populate_signals_table_with_data(filtered_df)

    def _populate_signals_table_with_data(self, df: pd.DataFrame):
        self.signals_table.setUpdatesEnabled(False) # Performance
        self.signals_table.setRowCount(0) # Clear table
        df_sorted_for_display = df.copy()

        # Sort for display (latest and highest confidence first)
        if not df_sorted_for_display.empty:
            # Ensure 'time' and 'confidence_%' exist for sorting
            if 'time' not in df_sorted_for_display.columns: df_sorted_for_display['time'] = pd.NaT
            else: df_sorted_for_display['time'] = pd.to_datetime(df_sorted_for_display['time'], errors='coerce')

            if 'confidence_%' not in df_sorted_for_display.columns: df_sorted_for_display['confidence_%'] = 0.0
            else: df_sorted_for_display['confidence_%'] = pd.to_numeric(df_sorted_for_display['confidence_%'], errors='coerce').fillna(0.0)

            try:
                df_sorted_for_display.sort_values(by=['time', 'confidence_%'], ascending=[False, False], na_position='last', inplace=True)
            except Exception as e_sort_signals_display: # Catch potential errors during sort
                self.log_to_ui_and_logger_wrapper(f"Warning: Could not sort signals for display: {e_sort_signals_display}.", "WARNING")

        # Limit number of signals shown in table
        df_to_display_limited = df_sorted_for_display.head(MAX_SIGNALS_TO_SHOW_IN_TABLE)

        col_map = { # Mapping for clarity
            "time": 0, "Symbol": 1, "signal": 2, "confidence_%": 3, "close": 4, "spread_pips": 5,
            "take_profit_price": 6, "stop_loss_price": 7, "take_profit_pips": 8,
            "stop_loss_pips": 9, "executed": 10, "notes": 11
        }

        for _, row_s in df_to_display_limited.iterrows(): # Iterate over the limited DataFrame
            row_idx = self.signals_table.rowCount()
            self.signals_table.insertRow(row_idx)

            # Time
            time_val = row_s.get("time")
            time_str = time_val.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(time_val) and isinstance(time_val, datetime) else str(time_val)
            self.signals_table.setItem(row_idx, col_map["time"], QtWidgets.QTableWidgetItem(time_str))

            # Symbol
            symbol_str = str(row_s.get("Symbol", ""))
            self.signals_table.setItem(row_idx, col_map["Symbol"], QtWidgets.QTableWidgetItem(symbol_str))

            # Signal type (Buy/Sell)
            signal_text = self.data_manager.signal_to_text(row_s.get("signal")) # Convert 'buy'/'sell' to Arabic
            item_signal = QtWidgets.QTableWidgetItem(signal_text)
            if signal_text == "شراء": item_signal.setForeground(QtGui.QColor("#2ecc71")) # Green
            elif signal_text == "بيع": item_signal.setForeground(QtGui.QColor("#e74c3c")) # Red
            self.signals_table.setItem(row_idx, col_map["signal"], item_signal)

            # Confidence
            self.signals_table.setItem(row_idx, col_map["confidence_%"], QtWidgets.QTableWidgetItem(f"{pd.to_numeric(row_s.get('confidence_%', 0.0), errors='coerce'):.2f}"))

            # Price formatting based on symbol digits
            price_digits = 5 # Default
            if self.mt5_manager and self.mt5_manager.is_connected() and symbol_str:
                symbol_info_for_digits = self.mt5_manager.get_symbol_info(symbol_str)
                if symbol_info_for_digits:
                    price_digits = getattr(symbol_info_for_digits, 'digits', 5)

            self.signals_table.setItem(row_idx, col_map["close"], QtWidgets.QTableWidgetItem(format_price_display(pd.to_numeric(row_s.get('close', 0.0), errors='coerce'), price_digits)))

            # Spread
            spread_val_points = row_s.get("spread_pips", 0)
            spread_display = f"{pd.to_numeric(spread_val_points, errors='coerce'):.0f}" if pd.notna(spread_val_points) else "N/A"
            self.signals_table.setItem(row_idx, col_map["spread_pips"], QtWidgets.QTableWidgetItem(spread_display))

            # TP/SL Prices and Pips
            self.signals_table.setItem(row_idx, col_map["take_profit_price"], QtWidgets.QTableWidgetItem(format_price_display(pd.to_numeric(row_s.get('take_profit_price', 0.0), errors='coerce'), price_digits)))
            self.signals_table.setItem(row_idx, col_map["stop_loss_price"], QtWidgets.QTableWidgetItem(format_price_display(pd.to_numeric(row_s.get('stop_loss_price', 0.0), errors='coerce'), price_digits)))
            self.signals_table.setItem(row_idx, col_map["take_profit_pips"], QtWidgets.QTableWidgetItem(str(row_s.get("take_profit_pips", ""))))
            self.signals_table.setItem(row_idx, col_map["stop_loss_pips"], QtWidgets.QTableWidgetItem(str(row_s.get("stop_loss_pips", ""))))

            # Executed status
            executed_val = row_s.get("executed", False)
            is_executed_bool = executed_val if isinstance(executed_val, bool) else str(executed_val).lower() in ['true', 'yes', '1', 'نعم', '1.0']
            executed_text = "نعم" if is_executed_bool else "لا"
            item_executed = QtWidgets.QTableWidgetItem(executed_text)
            item_executed.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            if is_executed_bool: item_executed.setForeground(QtGui.QColor("#3498db")) # Blue for Yes
            else: item_executed.setForeground(QtGui.QColor("#c0392b")) # Darker Red for No
            self.signals_table.setItem(row_idx, col_map["executed"], item_executed)

            # Notes
            self.signals_table.setItem(row_idx, col_map["notes"], QtWidgets.QTableWidgetItem(str(row_s.get("notes", ""))))

        self.signals_table.setUpdatesEnabled(True) # Re-enable updates

    def on_confidence_slider_changed(self):
        new_val = self.confidence_slider.value()
        self.manual_filter_min_confidence = new_val # Update instance variable
        self.confidence_label.setText(f"{new_val}%") # Update UI label
        self.filter_and_display_signals_in_table() # Re-filter and display
        self.data_manager.update_setting("manual_filter_min_confidence", new_val) # Save to settings

    def is_trading_time_allowed(self):
        if not self.data_manager.get_setting("time_filter_enabled", False):
            return True # Time filter disabled, always allow
        try:
            now_utc = datetime.now(timezone.utc).time()
            start_time_str = self.data_manager.get_setting("trade_start_time", "00:00")
            end_time_str = self.data_manager.get_setting("trade_end_time", "23:59")
            start_trade_time = datetime.strptime(start_time_str, "%H:%M").time()
            end_trade_time = datetime.strptime(end_time_str, "%H:%M").time()

            if start_trade_time <= end_trade_time: # Normal case (e.g., 09:00 - 17:00)
                if start_trade_time <= now_utc <= end_trade_time: return True
            else: # Overnight case (e.g., 22:00 - 05:00)
                if now_utc >= start_trade_time or now_utc <= end_trade_time: return True

            self.log_to_ui_and_logger_wrapper(f"Trading blocked by time filter: Current UTC time {now_utc.strftime('%H:%M')} is outside allowed range {start_time_str}-{end_time_str}.", "INFO")
            return False
        except Exception as e:
            self.log_to_ui_and_logger_wrapper(f"Error in time filter logic: {e}. Allowing trade as a fallback.", "ERROR")
            self.logger.error(traceback.format_exc())
            return True # Fallback to allow if error

    def execute_trade_from_signal_data(self, signal_data: pd.Series = None, order_type_override: str = None, is_auto_trade: bool = False):
        source_log_prefix = "AutoTrade" if is_auto_trade else "ManualTrade"
        signal_time_for_update = None # To identify signal in df_signals for update
        signal_symbol_for_update = None
        current_note = "" # To store reason for not executing or result

        # 1. Check Time Filter
        if not self.is_trading_time_allowed():
             current_note = f"{source_log_prefix}: Blocked by time filter"
             if not is_auto_trade: QMessageBox.warning(self, "وقت التداول", "التداول غير مسموح به الآن حسب فلتر الوقت.")
             self.log_to_ui_and_logger_wrapper(current_note, "WARNING")
             if signal_data is not None and hasattr(signal_data, 'get'): self._update_signal_status_in_df(signal_data.get("time"), signal_data.get("Symbol"), False, current_note)
             return False, current_note

        # 2. Check News Filter
        if not self.trading_allowed_by_news and self.data_manager.get_setting("halt_trades_on_news", True):
            current_note = f"{source_log_prefix}: Blocked by news event"
            if not is_auto_trade: QMessageBox.warning(self, "تحذير الأخبار", "التداول معطل حالياً بسبب أخبار هامة.")
            self.log_to_ui_and_logger_wrapper(current_note, "WARNING")
            if signal_data is not None and hasattr(signal_data, 'get'): self._update_signal_status_in_df(signal_data.get("time"), signal_data.get("Symbol"), False, current_note)
            return False, current_note

        # 3. Check MT5 Connection
        if not self.mt5_manager or not self.mt5_manager.is_connected():
            current_note = f"{source_log_prefix}: MT5 not connected"
            if not is_auto_trade: QMessageBox.warning(self, "خطأ اتصال", "غير متصل بمنصة MT5.")
            self.log_to_ui_and_logger_wrapper(current_note, "ERROR")
            if signal_data is not None and hasattr(signal_data, 'get'): self._update_signal_status_in_df(signal_data.get("time"), signal_data.get("Symbol"), False, current_note)
            return False, current_note

        # 4. Get Signal Data if not provided (for manual trades from table)
        if signal_data is None: # Manual trade from table selection
            selected_rows = self.signals_table.selectionModel().selectedRows()
            if not selected_rows:
                if not is_auto_trade: QMessageBox.warning(self, "لم يتم الاختيار", "الرجاء اختيار إشارة من الجدول أولاً.")
                return False, "No signal selected from table"
            selected_table_row_index = selected_rows[0].row() # Get the actual displayed row index

            # Reconstruct the DataFrame as currently displayed to get the correct signal
            min_conf_filter_for_selection = self.confidence_slider.value()
            temp_df_for_selection = self.df_signals.copy() # Start with all signals
            if 'confidence_%' in temp_df_for_selection.columns:
                temp_df_for_selection['confidence_%'] = pd.to_numeric(temp_df_for_selection['confidence_%'], errors='coerce').fillna(0)
                temp_df_for_selection = temp_df_for_selection[temp_df_for_selection["confidence_%"] >= min_conf_filter_for_selection].copy()
            if 'time' in temp_df_for_selection.columns and not temp_df_for_selection.empty: # Sort as displayed
                temp_df_for_selection['time'] = pd.to_datetime(temp_df_for_selection['time'], errors='coerce')
                if 'confidence_%' not in temp_df_for_selection.columns: temp_df_for_selection['confidence_%'] = 0.0
                temp_df_for_selection.sort_values(by=['time', 'confidence_%'], ascending=[False, False], na_position='last', inplace=True)
            
            temp_df_for_selection_limited = temp_df_for_selection.head(MAX_SIGNALS_TO_SHOW_IN_TABLE) # Apply limit

            if selected_table_row_index >= len(temp_df_for_selection_limited):
                if not is_auto_trade: QMessageBox.critical(self, "خطأ فهرس", "فهرس الصف المختار خارج النطاق. الرجاء تحديث الإشارات والمحاولة مرة أخرى.")
                self.log_to_ui_and_logger_wrapper(f"{source_log_prefix}: Selected row index {selected_table_row_index} out of bounds for displayed signals ({len(temp_df_for_selection_limited)}).", "ERROR")
                return False, "Selected row index out of bounds for displayed signals"
            signal_data = temp_df_for_selection_limited.iloc[selected_table_row_index].copy() # Get the correct signal Series

        # Ensure signal_data is a Series and has expected fields
        if not isinstance(signal_data, pd.Series) or not hasattr(signal_data, 'get'):
            current_note = f"{source_log_prefix}: Invalid signal_data format."
            self.log_to_ui_and_logger_wrapper(current_note, "ERROR")
            return False, current_note
            
        signal_time_for_update = signal_data.get("time")
        signal_symbol_for_update = signal_data.get("Symbol")

        # 5. Validate Signal Type and Symbol
        symbol = signal_data.get("Symbol")
        final_order_type_str = order_type_override.lower() if order_type_override else str(signal_data.get("signal","")).lower()

        if final_order_type_str not in ['buy', 'sell']:
            current_note = f"{source_log_prefix}: Invalid signal type '{final_order_type_str}' for {symbol}"
            if not is_auto_trade: QMessageBox.warning(self, "نوع إشارة خاطئ", f"نوع الإشارة '{final_order_type_str}' غير صالح.")
            self.log_to_ui_and_logger_wrapper(current_note, "ERROR")
            if signal_time_for_update and signal_symbol_for_update: self._update_signal_status_in_df(signal_time_for_update, signal_symbol_for_update, False, current_note)
            return False, current_note

        if not symbol or str(symbol).strip() == "" or str(symbol).lower() == "n/a":
            current_note = f"{source_log_prefix}: Missing symbol in signal data"
            if not is_auto_trade: QMessageBox.warning(self, "بيانات ناقصة", "الرمز مفقود في بيانات الإشارة.")
            self.log_to_ui_and_logger_wrapper(current_note, "ERROR")
            if signal_time_for_update and signal_symbol_for_update: self._update_signal_status_in_df(signal_time_for_update, signal_symbol_for_update, False, current_note) # Update even if symbol is missing if time is there
            return False, current_note

        # 6. Check Minimum Trade Interval
        min_interval_key = f"min_trade_interval_minutes_{symbol.lower().replace('/', '_')}" # Symbol-specific setting key
        default_min_interval_global = self.data_manager.get_setting("default_min_trade_interval_minutes", 15)
        min_trade_interval_minutes = self.data_manager.get_setting(min_interval_key, default_min_interval_global)

        if symbol in self.last_trade_time and min_trade_interval_minutes > 0:
            time_since_last = datetime.now(timezone.utc) - self.last_trade_time[symbol]
            if time_since_last < timedelta(minutes=min_trade_interval_minutes):
                wait_time = timedelta(minutes=min_trade_interval_minutes) - time_since_last
                wait_sec = int(wait_time.total_seconds())
                current_note = f"{source_log_prefix}: Min interval for {symbol} ({min_trade_interval_minutes}m) not met. Wait {wait_sec//60}m {wait_sec%60}s."
                if not is_auto_trade: QMessageBox.information(self, "فاصل زمني للتداول", current_note)
                self.log_to_ui_and_logger_wrapper(current_note, "INFO")
                if signal_time_for_update and signal_symbol_for_update: self._update_signal_status_in_df(signal_time_for_update, signal_symbol_for_update, False, current_note)
                return False, current_note

        # 7. Get Symbol Info from MT5
        symbol_info = self.mt5_manager.get_symbol_info(symbol)
        if not symbol_info:
            current_note = f"{source_log_prefix}: No symbol info for {symbol} from MT5"
            if not is_auto_trade: QMessageBox.critical(self, "خطأ معلومات الرمز", f"لا يمكن جلب معلومات الرمز لـ {symbol}.")
            self.log_to_ui_and_logger_wrapper(current_note, "ERROR")
            if signal_time_for_update and signal_symbol_for_update: self._update_signal_status_in_df(signal_time_for_update, signal_symbol_for_update, False, current_note)
            return False, current_note

        # 8. Check Spread
        current_market_spread_points = self.mt5_manager.get_current_spread(symbol) # In points
        if current_market_spread_points is None: # Error getting spread
            current_market_spread_points = pd.to_numeric(signal_data.get("spread_pips"), errors='coerce') # Fallback to signal's spread
            if pd.isna(current_market_spread_points):
                current_note = f"{source_log_prefix}: Cannot determine current market spread for {symbol} (MT5 or signal). Blocking trade."
                self.log_to_ui_and_logger_wrapper(current_note, "ERROR")
                if not is_auto_trade: QMessageBox.warning(self, "خطأ سبريد", f"لا يمكن تحديد السبريد الحالي أو من الإشارة لـ {symbol}.")
                if signal_time_for_update and signal_symbol_for_update: self._update_signal_status_in_df(signal_time_for_update, signal_symbol_for_update, False, current_note)
                return False, current_note
            else:
                self.log_to_ui_and_logger_wrapper(f"{source_log_prefix}: Using spread from signal ({current_market_spread_points} pts) as live tick failed for {symbol}.", "WARNING")

        max_allowed_spread = 0 # Default no limit
        gold_cfg_symbol = self.data_manager.get_setting("gold_symbol", "XAUUSD")
        btc_cfg_symbol = self.data_manager.get_setting("bitcoin_symbol", "BTCUSD")
        if symbol == gold_cfg_symbol: max_allowed_spread = self.data_manager.get_setting("max_allowed_spread_points_gold", 30)
        elif symbol == btc_cfg_symbol: max_allowed_spread = self.data_manager.get_setting("max_allowed_spread_points_bitcoin", 1000) # Higher for BTC
        else: max_allowed_spread = self.data_manager.get_setting("max_allowed_spread_points_other", 0) # 0 means no check for others unless set

        if max_allowed_spread > 0 and current_market_spread_points > max_allowed_spread:
            current_note = f"{source_log_prefix}: Spread for {symbol} ({current_market_spread_points} pts) > Max allowed ({max_allowed_spread} pts)."
            if not is_auto_trade: QMessageBox.warning(self, "سبريد مرتفع", current_note)
            self.log_to_ui_and_logger_wrapper(current_note, "WARNING")
            if signal_time_for_update and signal_symbol_for_update: self._update_signal_status_in_df(signal_time_for_update, signal_symbol_for_update, False, current_note)
            return False, current_note

        # 9. Calculate Lot Size
        risk_percent = self.data_manager.get_setting("risk_percent_per_trade", 1.0)
        sl_pips_val_for_lot_calc = pd.to_numeric(signal_data.get("stop_loss_pips"), errors='coerce')

        default_sl_pips_global = self.data_manager.get_setting("default_sl_pips", 50)
        effective_sl_pips_for_lot = default_sl_pips_global # Start with global default

        # Override with symbol-specific SL pips from settings if available
        if symbol == gold_cfg_symbol: effective_sl_pips_for_lot = self.data_manager.get_setting("gold_sl_pips", default_sl_pips_global)
        elif symbol == btc_cfg_symbol: effective_sl_pips_for_lot = self.data_manager.get_setting("btc_sl_pips", default_sl_pips_global)
        # Add more symbol-specific SL pips here if needed

        # If signal provides valid SL pips, it takes precedence over settings for lot calculation
        if pd.notna(sl_pips_val_for_lot_calc) and sl_pips_val_for_lot_calc > 0 :
            effective_sl_pips_for_lot = sl_pips_val_for_lot_calc
            self.log_to_ui_and_logger_wrapper(f"{source_log_prefix}: Using SL pips from signal ({effective_sl_pips_for_lot}) for lot calculation for {symbol}.", "DEBUG")
        else:
            self.log_to_ui_and_logger_wrapper(f"{source_log_prefix}: Using SL pips from settings ({effective_sl_pips_for_lot}) for lot calculation for {symbol}.", "DEBUG")


        lot = self.calculate_lot_size_advanced(symbol, risk_percent, effective_sl_pips_for_lot)
        volume_digits_precision = getattr(symbol_info, 'volume_digits', 2) # From symbol_info
        min_vol_trade = getattr(symbol_info, 'volume_min', 0.01)
        if min_vol_trade <=0: min_vol_trade = 0.01 # Ensure positive min_vol
        if lot <= 0 or lot < min_vol_trade: # Ensure lot is at least min_vol
            self.log_to_ui_and_logger_wrapper(f"{source_log_prefix}: Calculated lot ({lot:.{volume_digits_precision}f}) for {symbol} is too small or zero. Using min_vol: {min_vol_trade:.{volume_digits_precision}f}", "WARNING")
            lot = min_vol_trade
        lot = round(lot, volume_digits_precision) # Round to symbol's lot precision

        # 10. Determine SL/TP Prices
        tick_for_order = self.mt5_manager.get_tick(symbol)
        if not tick_for_order:
            current_note = f"{source_log_prefix}: No market tick for {symbol} available before sending order."
            if not is_auto_trade: QMessageBox.critical(self, "خطأ سعر السوق", f"لا يمكن جلب السعر الحالي للسوق لـ {symbol}.")
            self.log_to_ui_and_logger_wrapper(current_note, "ERROR")
            if signal_time_for_update and signal_symbol_for_update: self._update_signal_status_in_df(signal_time_for_update, signal_symbol_for_update, False, current_note)
            return False, current_note

        price_for_sl_tp_calc_from_signal = pd.to_numeric(signal_data.get('close'), errors='coerce')
        # Use current market price if signal price is invalid
        market_price_for_sl_tp_calc = tick_for_order.ask if final_order_type_str == "buy" else tick_for_order.bid
        if not pd.notna(price_for_sl_tp_calc_from_signal) or price_for_sl_tp_calc_from_signal <= 0:
            self.log_to_ui_and_logger_wrapper(f"{source_log_prefix}: Invalid 'close' price in signal ({signal_data.get('close')}). Using current market price {market_price_for_sl_tp_calc} for SL/TP calc for {symbol}.", "WARNING")
            price_for_sl_tp_calc_final = market_price_for_sl_tp_calc
        else:
            price_for_sl_tp_calc_final = price_for_sl_tp_calc_from_signal

        if price_for_sl_tp_calc_final <=0: # Should not happen if market_price was used as fallback
             current_note = f"{source_log_prefix}: Market price for {symbol} is invalid ({price_for_sl_tp_calc_final}). Cannot set SL/TP."
             self.log_to_ui_and_logger_wrapper(current_note, "ERROR")
             if signal_time_for_update and signal_symbol_for_update: self._update_signal_status_in_df(signal_time_for_update, signal_symbol_for_update, False, current_note)
             return False, current_note

        point_size = symbol_info.point; digits = symbol_info.digits; final_tp_price = 0.0; final_sl_price = 0.0

        # Get TP/SL pips from signal OR settings
        tp_pips_from_signal = pd.to_numeric(signal_data.get("take_profit_pips"), errors='coerce')
        sl_pips_from_signal_for_order = pd.to_numeric(signal_data.get("stop_loss_pips"), errors='coerce') # This is for order, not lot calc

        default_tp_pips_global = self.data_manager.get_setting("default_tp_pips", 100)
        effective_tp_pips_for_order = default_tp_pips_global
        effective_sl_pips_for_order = effective_sl_pips_for_lot # Start with SL pips used for lot calc

        if symbol == gold_cfg_symbol:
            effective_tp_pips_for_order = self.data_manager.get_setting("gold_tp_pips", default_tp_pips_global)
            if not (pd.notna(sl_pips_from_signal_for_order) and sl_pips_from_signal_for_order > 0): # If signal SL pips not set, use gold settings
                 effective_sl_pips_for_order = self.data_manager.get_setting("gold_sl_pips", effective_sl_pips_for_lot)
        elif symbol == btc_cfg_symbol:
            effective_tp_pips_for_order = self.data_manager.get_setting("btc_tp_pips", default_tp_pips_global)
            if not (pd.notna(sl_pips_from_signal_for_order) and sl_pips_from_signal_for_order > 0):
                 effective_sl_pips_for_order = self.data_manager.get_setting("btc_sl_pips", effective_sl_pips_for_lot)

        # Signal's TP/SL pips override settings if valid
        if pd.notna(tp_pips_from_signal) and tp_pips_from_signal > 0: effective_tp_pips_for_order = tp_pips_from_signal
        if pd.notna(sl_pips_from_signal_for_order) and sl_pips_from_signal_for_order > 0: effective_sl_pips_for_order = sl_pips_from_signal_for_order

        # Calculate final SL/TP prices based on determined pips and reference price
        if final_order_type_str == "buy":
            if effective_sl_pips_for_order > 0: final_sl_price = round(price_for_sl_tp_calc_final - (effective_sl_pips_for_order * point_size), digits)
            if effective_tp_pips_for_order > 0: final_tp_price = round(price_for_sl_tp_calc_final + (effective_tp_pips_for_order * point_size), digits)
        elif final_order_type_str == "sell":
            if effective_sl_pips_for_order > 0: final_sl_price = round(price_for_sl_tp_calc_final + (effective_sl_pips_for_order * point_size), digits)
            if effective_tp_pips_for_order > 0: final_tp_price = round(price_for_sl_tp_calc_final - (effective_tp_pips_for_order * point_size), digits)


        # 11. Prepare and Confirm Trade (if manual)
        time_str_for_comment = str(signal_data.get('time', datetime.now().strftime("%H%M%S"))).replace("-","").replace(":","").replace(" ","_")[:15]
        comment_prefix = "Auto" if is_auto_trade else "Man"
        base_comment = f"{comment_prefix}{final_order_type_str[:1].upper()}{time_str_for_comment}"
        model_note_raw = str(signal_data.get('notes', ''))
        trade_comment = base_comment
        if model_note_raw and "Model" in model_note_raw : # Try to append short model ID
            try:
                model_id_part = model_note_raw.split(":")[-1].strip().split(".")[0] # e.g. "Model: model_XAUUSD_xyz.joblib" -> "model_XAUUSD_xyz"
                model_id_short = "".join(filter(str.isalnum, model_id_part))[:7] # "modelXA"
            except: model_id_short = ""
            if model_id_short: trade_comment = f"{base_comment}_{model_id_short}"
        trade_comment = trade_comment.replace(":", "").replace(" ", "_")[:31] # Sanitize and shorten


        self.log_to_ui_and_logger_wrapper(f"{source_log_prefix}: Attempting {final_order_type_str.upper()} {symbol} | Lot:{lot:.{volume_digits_precision}f} | TP:{format_price_display(final_tp_price, digits)} ({effective_tp_pips_for_order} pips) | SL:{format_price_display(final_sl_price, digits)} ({effective_sl_pips_for_order} pips) | Comment:'{trade_comment}' | RefPriceForSLTP: {format_price_display(price_for_sl_tp_calc_final, digits)}", "INFO")

        if not is_auto_trade: # Manual trade confirmation dialog
            sl_display = format_price_display(final_sl_price, digits) if final_sl_price > 0 else 'لا يوجد'
            tp_display = format_price_display(final_tp_price, digits) if final_tp_price > 0 else 'لا يوجد'
            market_price_for_confirm = tick_for_order.ask if final_order_type_str == "buy" else tick_for_order.bid
            confirm_msg = (f"تنفيذ {final_order_type_str.upper()} على {symbol}؟\n"
                           f"الحجم: {lot:.{volume_digits_precision}f}\n"
                           f"السبريد الحالي (نقاط): {current_market_spread_points} (الحد المسموح: {max_allowed_spread if max_allowed_spread > 0 else 'N/A'})\n"
                           f"السعر الحالي للسوق: {format_price_display(market_price_for_confirm, digits)}\n"
                           f"وقف الخسارة المحسوب: {sl_display} ({effective_sl_pips_for_order} نقاط)\n"
                           f"جني الأرباح المحسوب: {tp_display} ({effective_tp_pips_for_order} نقاط)\n"
                           f"(SL/TP تم حسابهما من سعر مرجعي: {format_price_display(price_for_sl_tp_calc_final, digits)})")
            reply = QMessageBox.question(self, "تأكيد التنفيذ اليدوي", confirm_msg, QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)
            if reply != QMessageBox.StandardButton.Yes:
                current_note = f"{source_log_prefix}: User cancelled manual trade for {symbol}."
                self.log_to_ui_and_logger_wrapper(current_note, "INFO")
                if signal_time_for_update and signal_symbol_for_update: self._update_signal_status_in_df(signal_time_for_update, signal_symbol_for_update, False, current_note)
                return False, current_note

        # 12. Send Order
        success, result_obj_or_msg = self.mt5_manager.send_order(
            symbol=symbol, order_type=final_order_type_str, volume=lot,
            price=None, # Market order, price determined by MT5Manager
            sl=final_sl_price if final_sl_price > 0 else 0.0, # Pass 0.0 if no SL
            tp=final_tp_price if final_tp_price > 0 else 0.0, # Pass 0.0 if no TP
            comment=trade_comment
        )

        # 13. Process Result
        if success and hasattr(result_obj_or_msg, 'order') and result_obj_or_msg.order > 0:
            order_id = result_obj_or_msg.order
            current_note = f"{source_log_prefix}: Executed. Order ID: {order_id}"
            if not is_auto_trade:
                QMessageBox.information(self, f"نجاح ({'تلقائي' if is_auto_trade else 'يدوي'})", f"أمر {final_order_type_str.upper()} لـ {symbol} تم إرساله بنجاح. رقم الأمر: {order_id}")
            self.log_to_ui_and_logger_wrapper(current_note, "INFO")
            self.last_trade_time[symbol] = datetime.now(timezone.utc) # Update last trade time

            if signal_time_for_update and signal_symbol_for_update: # Mark signal as executed
                self._update_signal_status_in_df(signal_time_for_update, signal_symbol_for_update, True, current_note)

            self.filter_and_display_signals_in_table() # Refresh signals table UI
            self.data_manager.save_signals(self.df_signals) # Save updated signals to CSV
            self.refresh_account_summary() # Refresh account summary
            if self.data_manager.get_setting("log_trade_requests_enabled", True): # Log request details
                self.log_deal_request_details_after_execution(result_obj_or_msg, symbol, final_order_type_str, lot)
            return True, result_obj_or_msg
        else: # Order failed
            error_message = result_obj_or_msg if isinstance(result_obj_or_msg, str) else "Unknown order send error"
            if hasattr(result_obj_or_msg, 'comment') and result_obj_or_msg.comment: # Broker comment on failure
                error_message = f"{error_message} (Broker: {result_obj_or_msg.comment})"

            current_note = f"{source_log_prefix}: Failed - {error_message}"
            if not is_auto_trade:
                QMessageBox.critical(self, "خطأ في تنفيذ الأمر", f"فشل إرسال الأمر لـ {symbol}: {error_message}")
            self.log_to_ui_and_logger_wrapper(current_note, "ERROR")
            if signal_time_for_update and signal_symbol_for_update: # Mark as not executed with error
                 self._update_signal_status_in_df(signal_time_for_update, signal_symbol_for_update, False, current_note)

            self.filter_and_display_signals_in_table() # Refresh UI
            return False, error_message


    def auto_execute_model_signal_if_conditions_met(self, model_signal_series: pd.Series):
        signal_time = model_signal_series.get("time")
        signal_symbol = model_signal_series.get("Symbol")
        signal_type = model_signal_series.get("signal")
        signal_confidence = pd.to_numeric(model_signal_series.get("confidence_%", 0), errors='coerce')
        # current_note_for_signal = str(model_signal_series.get("notes", "")) # Not used here directly

        if not self.data_manager.get_setting("auto_trade_enabled", False):
            note_to_set = "AutoTrade: Disabled in settings."
            # Don't log here, execute_trade_from_signal_data will if it's called.
            # Just update the signal note if it hasn't been executed for this reason.
            self._update_signal_status_in_df(signal_time, signal_symbol, False, note_to_set)
            return

        if not self.mt5_manager or not self.mt5_manager.is_connected():
            note_to_set = f"AutoTrade: MT5 not connected for {signal_symbol} at signal time."
            self.log_to_ui_and_logger_wrapper(note_to_set, "WARNING")
            self._update_signal_status_in_df(signal_time, signal_symbol, False, note_to_set)
            return

        min_confidence_for_auto = self.data_manager.get_setting("auto_trade_min_confidence", 70)

        if signal_confidence >= min_confidence_for_auto:
            self.log_to_ui_and_logger_wrapper(f"AutoTrade: Signal for {signal_symbol} ({str(signal_type).upper()}) with confidence {signal_confidence:.2f}% meets threshold {min_confidence_for_auto}%. Attempting execution...", "INFO")

            if AUTO_TRADE_LOCK.locked(): # Prevent re-entrant auto-trades
                note_to_set = "AutoTrade: Lock active (another auto-trade in progress). Skipping current signal to prevent overlap."
                self.log_to_ui_and_logger_wrapper(note_to_set, "WARNING")
                self._update_signal_status_in_df(signal_time, signal_symbol, False, note_to_set)
                return

            with AUTO_TRADE_LOCK: # Acquire lock for this auto-trade attempt
                time_str_log = signal_time.strftime('%H:%M:%S %Z') if isinstance(signal_time, datetime) else str(signal_time)
                self.log_to_ui_and_logger_wrapper(f"AutoTrade: Lock acquired for {signal_symbol} at {time_str_log}.", "DEBUG")

                # Call the main execution logic
                self.execute_trade_from_signal_data(
                    signal_data=model_signal_series, # Pass the full signal Series
                    is_auto_trade=True
                )
                # The execute_trade_from_signal_data will handle logging success/failure and updating df_signals
            self.log_to_ui_and_logger_wrapper(f"AutoTrade: Lock released for {signal_symbol} at {time_str_log}.", "DEBUG")
        else:
            note_to_set = f"AutoTrade: Confidence {signal_confidence:.2f}% for {signal_symbol} < threshold {min_confidence_for_auto}%. Not executed."
            self.log_to_ui_and_logger_wrapper(note_to_set, "INFO")
            self._update_signal_status_in_df(signal_time, signal_symbol, False, note_to_set)


    @QtCore.pyqtSlot()
    def filter_and_display_signals_in_table_slot(self): # Slot for queued connection if needed
        self.filter_and_display_signals_in_table()

    def log_deal_request_details_after_execution(self, order_result_obj, symbol: str, order_type: str, volume: float):
        deal_log_path = self.data_manager.trade_requests_file # Get path from DataManager
        sl_in_result = 0.0; tp_in_result = 0.0; comment_from_request = ""
        if hasattr(order_result_obj, 'request') and order_result_obj.request: # Ensure request object exists
            sl_in_result = order_result_obj.request.sl
            tp_in_result = order_result_obj.request.tp
            comment_from_request = order_result_obj.request.comment

        retcode_val = getattr(order_result_obj, 'retcode', -1) # Get retcode, default -1 if not found

        deal_info = {
            "request_timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], # Millisecond precision
            "order_id_from_broker": getattr(order_result_obj, 'order', 'N/A'),
            "deal_id_from_broker": getattr(order_result_obj, 'deal', 'N/A'),
            "symbol_requested": symbol,
            "type_requested": order_type,
            "volume_requested": volume,
            "price_at_request_or_fill": getattr(order_result_obj, 'price', 0.0), # Fill price from result
            "sl_in_request": sl_in_result,
            "tp_in_request": tp_in_result,
            "comment_sent": comment_from_request,
            "broker_retcode": retcode_val,
            "broker_retcode_message": self.mt5_manager._get_retcode_description(retcode_val) if self.mt5_manager else "N/A",
            "broker_comment_on_result": getattr(order_result_obj, 'comment', '') # Broker's comment on the result
        }
        try:
            df_new_deal_request = pd.DataFrame([deal_info])
            file_exists = os.path.exists(deal_log_path)
            df_new_deal_request.to_csv(deal_log_path, mode='a', header=not file_exists, index=False, encoding='utf-8-sig')
            self.log_to_ui_and_logger_wrapper(f"Trade request for order {deal_info['order_id_from_broker']} logged to {deal_log_path}", "INFO")
        except Exception as e:
            self.log_to_ui_and_logger_wrapper(f"Error logging trade request to '{deal_log_path}': {e}", "ERROR")
            self.logger.error(traceback.format_exc())


    def confirm_close_all_positions(self):
        if not self.mt5_manager or not self.mt5_manager.is_connected():
            QMessageBox.information(self, "غير متصل", "يرجى الاتصال بـ MT5 أولاً.")
            return

        current_magic = self.data_manager.get_setting("mt5_magic_number", 234000)
        num_positions_to_close = self.mt5_manager.get_open_positions_count(magic=current_magic)

        if num_positions_to_close == 0 :
            QMessageBox.information(self, "لا صفقات", f"لا توجد صفقات مفتوحة (magic: {current_magic}).")
            self.log_to_ui_and_logger_wrapper(f"Close all: No open positions (magic {current_magic}).", "INFO")
            return

        reply = QMessageBox.question(self, "تأكيد الإغلاق",
                                     f"هل تريد إغلاق جميع الصفقات ({num_positions_to_close}، magic: {current_magic})؟",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.No) # Default to No
        if reply != QMessageBox.StandardButton.Yes:
            self.log_to_ui_and_logger_wrapper("User cancelled closing all positions.", "INFO")
            return

        close_comment = f"ManCloseAll M{current_magic}"[:31] # Max 31 chars for comment

        self.log_to_ui_and_logger_wrapper(f"User confirmed. Closing {num_positions_to_close} positions (magic: {current_magic}) with comment: '{close_comment}'.", "INFO")
        success, msg = self.mt5_manager.close_all_trades(magic=current_magic, comment=close_comment)
        if success:
            QMessageBox.information(self, "نجاح الإغلاق", msg)
            self.log_to_ui_and_logger_wrapper(f"Close All Succeeded: {msg}", "INFO")
        else:
            QMessageBox.critical(self, "خطأ إغلاق", f"فشل إغلاق الصفقات: {msg}")
            self.log_to_ui_and_logger_wrapper(f"Close All Failed: {msg}", "ERROR")

        # Refresh UI after closing
        self.refresh_account_summary()
        self.refresh_performance_stats() # Update deals history and performance
        self.filter_and_display_signals_in_table() # Update signals table (though not directly affected)


    def monitor_positions_for_auto_close(self):
        if not self.mt5_manager or not self.mt5_manager.is_connected(): return
        if not self.data_manager.get_setting("auto_close_by_points_enabled", False): return
        target_total_profit_points = self.data_manager.get_setting("auto_close_target_points", 1000)
        if target_total_profit_points <= 0: return # Must be a positive target

        current_magic = self.data_manager.get_setting("mt5_magic_number", 234000)
        positions = self.mt5_manager.get_open_positions(magic=current_magic)
        if not positions: return # No positions for this magic number

        current_total_profit_in_points = 0.0; has_any_open_position = False
        for pos in positions:
            has_any_open_position = True
            info = self.mt5_manager.get_symbol_info(pos.symbol)
            tick = self.mt5_manager.get_tick(pos.symbol)
            if not info or not tick or not hasattr(info, 'point') or info.point == 0:
                self.log_to_ui_and_logger_wrapper(f"AutoCloseMonitor: Skipping position {pos.ticket} for symbol {pos.symbol} due to missing info/tick or zero point value.", "WARNING")
                continue # Skip this position if data is incomplete

            point_value_for_calc = info.point # e.g., 0.00001 for EURUSD (5-digit)
            profit_in_points_for_pos = 0.0
            if pos.type == self.mt5_manager._ORDER_TYPE_BUY: # BUY position
                profit_in_points_for_pos = (tick.bid - pos.price_open) / point_value_for_calc
            elif pos.type == self.mt5_manager._ORDER_TYPE_SELL: # SELL position
                profit_in_points_for_pos = (pos.price_open - tick.ask) / point_value_for_calc
            current_total_profit_in_points += profit_in_points_for_pos

        if has_any_open_position: # Log current status only if there are positions
            self.log_to_ui_and_logger_wrapper(f"AutoCloseMonitor: Aggregate profit for magic {current_magic}: {current_total_profit_in_points:.2f} points. Target: {target_total_profit_points}.", "DEBUG")

        if has_any_open_position and current_total_profit_in_points >= target_total_profit_points:
            close_comment = f"AutoClose M{current_magic} PtsTgt"[:31]

            self.log_to_ui_and_logger_wrapper(f"AutoClose: Profit target {target_total_profit_points} reached (current: {current_total_profit_in_points:.2f}). Closing all positions for magic {current_magic}...", "INFO")
            success, msg = self.mt5_manager.close_all_trades(magic=current_magic, comment=close_comment)
            if success: self.log_to_ui_and_logger_wrapper(f"AutoClose: All positions (magic {current_magic}) closed successfully. {msg}", "INFO")
            else: self.log_to_ui_and_logger_wrapper(f"AutoClose: Failed to close positions (magic {current_magic}). {msg}", "ERROR")

            # Refresh UI after auto-closing
            self.refresh_account_summary()
            self.refresh_performance_stats()


    @QtCore.pyqtSlot(list) # news_list is a list of (title_str, datetime_utc_obj) tuples
    def on_news_updated_ui(self, news_list: list):
        halt_on_news_setting = self.data_manager.get_setting("halt_trades_on_news", True)
        self.trading_allowed_by_news = not (bool(news_list) and halt_on_news_setting) # Update app state

        if not bool(news_list): # No relevant news
            if self.news_alert_label.isVisible(): self.news_alert_label.hide()
            self.log_to_ui_and_logger_wrapper("News status: Trading allowed (No relevant high-impact news currently detected).", "INFO")
        else: # Relevant news detected
            titles_with_time = []
            for item_title, item_datetime_utc in news_list[:3]: # Display up to 3 news items
                try: # Convert UTC to local time for display
                    item_datetime_local = item_datetime_utc.astimezone(datetime.now().astimezone().tzinfo)
                    time_str_display = item_datetime_local.strftime('%H:%M')
                except Exception: # Fallback to UTC if conversion fails
                    time_str_display = item_datetime_utc.strftime('%H:%M UTC')
                titles_with_time.append(f"- {item_title} ({time_str_display})")
            news_display_text = "\n".join(titles_with_time)

            alert_message = f"⚠️ تحذير: أخبار هامة ({len(news_list)}):\n{news_display_text}"
            if halt_on_news_setting:
                 alert_message += "\n⛔ التداول (التلقائي واليدوي) معطل مؤقتاً بسبب الأخبار (حسب الإعدادات)."
                 style_sheet = "color: #FFFFFF; font-weight: bold; padding: 8px; border: 2px solid #A52A2A; background-color: #CD5C5C; border-radius: 5px; qproperty-alignment: AlignCenter;" # Red style
            else:
                 alert_message += "\n❗ التداول مسموح به ولكن يرجى الحذر، توجد أخبار هامة قد تؤثر على السوق."
                 style_sheet = "color: #000000; font-weight: bold; padding: 8px; border: 2px solid #D4AC0D; background-color: #F9E79F; border-radius: 5px; qproperty-alignment: AlignCenter;" # Yellow style

            self.news_alert_label.setText(alert_message)
            self.news_alert_label.setStyleSheet(style_sheet)
            self.news_alert_label.show()
            self.log_to_ui_and_logger_wrapper(f"News status: High-impact news detected. Trading allowed by news: {self.trading_allowed_by_news}. Halt on news setting: {halt_on_news_setting}", "WARNING")


    def get_current_trading_session(self): # Approximates major sessions based on UTC
        now_utc = datetime.now(timezone.utc)
        hour_utc = now_utc.hour
        weekday_utc = now_utc.weekday() # Monday is 0 and Sunday is 6

        sessions = []
        # Sydney (approx 21:00 UTC Sun - 06:00 UTC Fri) - Simplified
        if (weekday_utc == 6 and hour_utc >= 21) or \
           (0 <= weekday_utc <= 3 and (hour_utc >= 21 or hour_utc < 7)) or \
           (weekday_utc == 4 and hour_utc < 7): # Ends before London opens fully
             if not any(s.startswith("سيدني") for s in sessions): sessions.append("سيدني")

        # Tokyo (approx 00:00 UTC Mon - 09:00 UTC Fri)
        if (0 <= hour_utc < 9) and (0 <= weekday_utc <= 4): # Mon-Fri
            if not any(s.startswith("طوكيو") for s in sessions): sessions.append("طوكيو")

        # London (approx 07:00 UTC Mon - 16:00 UTC Fri)
        if (7 <= hour_utc < 16) and (0 <= weekday_utc <= 4): # Mon-Fri
             if not any(s.startswith("لندن") for s in sessions): sessions.append("لندن")

        # New York (approx 12:00 UTC Mon - 21:00 UTC Fri)
        if (12 <= hour_utc < 21) and (0 <= weekday_utc <= 4): # Mon-Fri
            if not any(s.startswith("نيويورك") for s in sessions): sessions.append("نيويورك")

        # Market closed (Friday night to Sunday night UTC)
        if (weekday_utc == 4 and hour_utc >= 21) or \
           (weekday_utc == 5) or \
           (weekday_utc == 6 and hour_utc < 21):
            return "مغلقة (عطلة نهاية الأسبوع)"

        if not sessions: # If no major session identified during Mon-Fri
            if 0 <= weekday_utc <= 4 : # It's a weekday
                 return "مغلقة (بين الجلسات الرئيسية)"
            return "مغلقة (غير محدد)" # Should be covered by weekend check
        elif len(sessions) > 1:
            return "تداخل: " + " و ".join(sessions)
        else:
            return sessions[0]


    def update_trading_session_display(self):
        session_text = self.get_current_trading_session()
        self.trading_session_label.setText(f"الجلسة الحالية (UTC تقريبي): {session_text}")


    def closeEvent(self, event: QtGui.QCloseEvent):
        self.log_to_ui_and_logger_wrapper("Application is closing. Disconnecting and stopping timers...", "INFO")
        if self.mt5_manager and self.mt5_manager.is_connected(): self.mt5_manager.disconnect()

        # Stop all QTimers
        timers_to_stop = [self.refresh_signals_timer, self.position_monitor_timer,
                          self.account_summary_timer, self.session_update_timer]
        if self.news_manager and hasattr(self.news_manager, 'timer') and isinstance(self.news_manager.timer, QtCore.QTimer):
            timers_to_stop.append(self.news_manager.timer)

        for timer_instance in timers_to_stop:
            if timer_instance and timer_instance.isActive():
                timer_instance.stop()
                timer_name = timer_instance.objectName() if timer_instance.objectName() else 'Unnamed QTimer'
                self.log_to_ui_and_logger_wrapper(f"Timer '{timer_name}' stopped.", "DEBUG")

        # Stop NewsManager's polling thread if it exists and has the method
        if self.news_manager and hasattr(self.news_manager, 'stop_polling_thread'):
            self.news_manager.stop_polling_thread()
            self.log_to_ui_and_logger_wrapper("NewsManager polling thread signaled to stop.", "DEBUG")

        self.log_to_ui_and_logger_wrapper("All application timers stopped. Exiting now.", "INFO")
        super().closeEvent(event) # Proceed with closing


def main():
    # Setup logging
    log_file_name = f"trading_app_activity_{datetime.now().strftime('%Y%m%d')}.log"
    log_formatter = logging.Formatter(
        fmt='%(asctime)s.%(msecs)03d - %(name)s - [%(levelname)s] - (%(threadName)-10s) - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logs_dir = "logs" # Store logs in a 'logs' subdirectory
    if not os.path.exists(logs_dir):
        try: os.makedirs(logs_dir)
        except OSError as e: # Handle potential error during directory creation
            print(f"Error creating logs directory {logs_dir}: {e}", file=sys.stderr)
            # Continue without file logging if directory creation fails, console logging will still work.

    log_file_path = os.path.join(logs_dir, log_file_name)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG) # Capture all levels to root

    if root_logger.hasHandlers(): # Clear any existing handlers (e.g., from previous runs in interactive session)
        root_logger.handlers.clear()

    # File Handler
    try:
        file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
        file_handler.setFormatter(log_formatter); file_handler.setLevel(logging.DEBUG) # Log DEBUG and above to file
        root_logger.addHandler(file_handler)
    except Exception as e:
        print(f"Error setting up file logger to {log_file_path}: {e}", file=sys.stderr)

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout) # Log to standard output
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.INFO) # Log INFO and above to console
    root_logger.addHandler(console_handler)

    # Initial log messages
    main_logger = logging.getLogger(__name__) # Get logger for this main module
    user_login_name_for_app = "Halim1980-ai" # This can be made dynamic if needed
    initial_log_utc_time_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    main_logger.info(f"--- Trading Application starting. User: {user_login_name_for_app}. App UTC Start: {initial_log_utc_time_str} ---")
    main_logger.info(f"Log file: {os.path.abspath(log_file_path)}")


    # Create and run the PyQt application
    app = QtWidgets.QApplication(sys.argv)

    # Apply a modern style if available
    available_styles = QStyleFactory.keys()
    if "Fusion" in available_styles: app.setStyle(QStyleFactory.create("Fusion")); main_logger.info("Applied 'Fusion' style.")
    elif "WindowsVista" in available_styles and sys.platform == "win32": app.setStyle(QStyleFactory.create("WindowsVista")); main_logger.info("Applied 'WindowsVista' style.")
    else: main_logger.info(f"Default style '{app.style().objectName()}' used. Available: {available_styles}")


    try:
        main_window = TradingApp()
        main_window.user_login_display = user_login_name_for_app # Pass username to app instance
        main_window.setWindowTitle(f"تطبيق التداول الآلي MT5 - {main_window.get_user_login_display()} - UTC Start: {main_window.utc_start_time_for_title}") # Update title
        main_window.show()
        main_logger.info("TradingApp UI shown. Entering Qt event loop...")
        exit_code = app.exec()
        main_logger.info(f"--- Trading Application exited with code {exit_code}. UTC: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} ---")
        sys.exit(exit_code)
    except Exception as e_critical_gui: # Catch any unhandled exceptions during app setup or run
        main_logger.critical(f"Unhandled CRITICAL exception during TradingApp or Qt event loop: {e_critical_gui}", exc_info=True)
        # Try to show a Qt message box for critical UI errors
        try:
            error_box = QMessageBox()
            error_box.setIcon(QMessageBox.Icon.Critical)
            error_box.setWindowTitle("خطأ فادح في واجهة المستخدم")
            error_box.setText(f"حدث خطأ فادح في واجهة المستخدم:\n{e_critical_gui}\n\nيرجى مراجعة ملف السجل: {log_file_path}")
            error_box.setDetailedText(traceback.format_exc()) # Add traceback for more details
            error_box.exec()
        except Exception as mb_ex_gui: # If even the message box fails
            main_logger.error(f"Could not display critical GUI error message box: {mb_ex_gui}")
        sys.exit(1) # Exit with error code

if __name__ == "__main__":
    try:
        main()
    except Exception as e_critical_main: # Catch errors even before full logging is set up
        # Fallback logging for very early critical errors
        print(f"CRITICAL MAIN EXECUTION ERROR (before full logging): {e_critical_main}\n{traceback.format_exc()}", file=sys.stderr)
        logs_dir_crit = "logs" # Attempt to use the same logs directory
        if not os.path.exists(logs_dir_crit):
            try: os.makedirs(logs_dir_crit)
            except: pass # Ignore if creating fallback log dir fails
        crit_log_fallback = os.path.join(logs_dir_crit, f"trading_app_CRITICAL_MAIN_ERROR_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        try: # Try to write to a fallback critical error log file
            with open(crit_log_fallback, "a", encoding="utf-8") as f_crit:
                f_crit.write(f"{datetime.now(timezone.utc).isoformat()} - CRITICAL MAIN EXECUTION ERROR: {e_critical_main}\n")
                f_crit.write(traceback.format_exc() + "\n")
        except Exception:
            pass # Ignore if writing to fallback log fails
        sys.exit(1) # Exit with error code