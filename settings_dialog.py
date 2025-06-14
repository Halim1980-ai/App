from PyQt6 import QtWidgets, QtCore
from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QFormLayout, QLineEdit, QPushButton,
    QCheckBox, QSpinBox, QDoubleSpinBox, QGroupBox, QDialogButtonBox,
    QMessageBox, QScrollArea, QWidget, QTabWidget # Added QTabWidget
)

class SettingsDialog(QDialog):
    def __init__(self, data_manager_instance, parent=None):
        super().__init__(parent)
        self.data_manager = data_manager_instance
        self.setWindowTitle("إعدادات التطبيق ومنصة MT5")
        self.setMinimumWidth(600) # Might need a bit more width for tabs
        self.setMinimumHeight(500) # Adjust as needed

        self._setup_ui()
        self.load_settings_to_ui()

    def _create_mt5_tab(self):
        tab_widget = QWidget()
        layout = QVBoxLayout(tab_widget)
        mt5_group = QGroupBox("إعدادات اتصال MT5")
        mt5_form_layout = QFormLayout(mt5_group)
        
        self.mt5_login_edit = QLineEdit()
        mt5_form_layout.addRow("رقم حساب MT5:", self.mt5_login_edit)
        self.mt5_password_edit = QLineEdit()
        self.mt5_password_edit.setEchoMode(QLineEdit.EchoMode.Password)
        mt5_form_layout.addRow("كلمة مرور MT5:", self.mt5_password_edit)
        self.mt5_server_edit = QLineEdit()
        mt5_form_layout.addRow("خادم MT5:", self.mt5_server_edit)
        self.mt5_path_edit = QLineEdit()
        mt5_form_layout.addRow("مسار منصة MT5:", self.mt5_path_edit)
        self.mt5_magic_edit = QLineEdit()
        mt5_form_layout.addRow("الرقم السحري الافتراضي:", self.mt5_magic_edit)
        self.mt5_retries_spin = QSpinBox(); self.mt5_retries_spin.setRange(0, 10)
        mt5_form_layout.addRow("عدد محاولات اتصال MT5:", self.mt5_retries_spin)
        self.mt5_retry_delay_spin = QDoubleSpinBox(); self.mt5_retry_delay_spin.setRange(0.1, 60.0); self.mt5_retry_delay_spin.setDecimals(1); self.mt5_retry_delay_spin.setSuffix(" ثانية")
        mt5_form_layout.addRow("تأخير بين محاولات MT5:", self.mt5_retry_delay_spin)
        self.mt5_timeout_ms_spin = QSpinBox(); self.mt5_timeout_ms_spin.setRange(1000, 120000); self.mt5_timeout_ms_spin.setSuffix(" مللي ثانية")
        mt5_form_layout.addRow("مهلة اتصال MT5:", self.mt5_timeout_ms_spin)
        
        layout.addWidget(mt5_group)
        return tab_widget

    def _create_trading_tab(self):
        tab_widget = QWidget()
        layout = QVBoxLayout(tab_widget)
        trading_group = QGroupBox("إعدادات التداول العامة")
        trading_form_layout = QFormLayout(trading_group)
        self.manual_filter_min_confidence_spin = QSpinBox(); self.manual_filter_min_confidence_spin.setRange(0, 100); self.manual_filter_min_confidence_spin.setSuffix("%")
        trading_form_layout.addRow("حد الثقة الأدنى للفلتر اليدوي (UI):", self.manual_filter_min_confidence_spin)
        self.auto_trade_enabled_checkbox = QCheckBox("تفعيل التداول التلقائي")
        trading_form_layout.addRow(self.auto_trade_enabled_checkbox)
        self.auto_trade_min_confidence_spin = QSpinBox(); self.auto_trade_min_confidence_spin.setRange(0, 100); self.auto_trade_min_confidence_spin.setSuffix("%")
        trading_form_layout.addRow("الحد الأدنى للثقة للتداول التلقائي:", self.auto_trade_min_confidence_spin)
        self.risk_percent_spin = QDoubleSpinBox(); self.risk_percent_spin.setRange(0.01, 100.0); self.risk_percent_spin.setDecimals(2); self.risk_percent_spin.setSuffix("%")
        trading_form_layout.addRow("نسبة المخاطرة لكل صفقة:", self.risk_percent_spin)
        self.default_sl_pips_spin = QSpinBox(); self.default_sl_pips_spin.setRange(0, 10000); self.default_sl_pips_spin.setSuffix(" نقطة")
        trading_form_layout.addRow("وقف الخسارة الافتراضي العام (نقاط):", self.default_sl_pips_spin)
        self.default_tp_pips_spin = QSpinBox(); self.default_tp_pips_spin.setRange(0, 20000); self.default_tp_pips_spin.setSuffix(" نقطة")
        trading_form_layout.addRow("جني الأرباح الافتراضي العام (نقاط):", self.default_tp_pips_spin)
        self.min_trade_interval_default_spin = QSpinBox(); self.min_trade_interval_default_spin.setRange(0, 1440); self.min_trade_interval_default_spin.setSuffix(" دقيقة")
        trading_form_layout.addRow("الفاصل الزمني الأدنى بين الصفقات (افتراضي عام):", self.min_trade_interval_default_spin)
        layout.addWidget(trading_group)
        # You can add more QGroupBoxes to this tab's layout if needed
        return tab_widget

    def _create_symbols_tab(self):
        tab_widget = QWidget()
        layout = QVBoxLayout(tab_widget) # Use QVBoxLayout for multiple groups if needed
        
        symbol_specific_group = QGroupBox("إعدادات خاصة بالرموز")
        symbol_specific_layout = QFormLayout(symbol_specific_group)
        self.gold_symbol_edit = QLineEdit()
        symbol_specific_layout.addRow("رمز الذهب (مثال: XAUUSD):", self.gold_symbol_edit)
        self.gold_sl_pips_spin = QSpinBox(); self.gold_sl_pips_spin.setRange(0, 20000); self.gold_sl_pips_spin.setSuffix(" نقطة")
        symbol_specific_layout.addRow("وقف الخسارة للذهب (نقاط):", self.gold_sl_pips_spin)
        self.gold_tp_pips_spin = QSpinBox(); self.gold_tp_pips_spin.setRange(0, 40000); self.gold_tp_pips_spin.setSuffix(" نقطة")
        symbol_specific_layout.addRow("جني الأرباح للذهب (نقاط):", self.gold_tp_pips_spin)
        self.max_spread_gold_spin = QSpinBox(); self.max_spread_gold_spin.setRange(0, 1000); self.max_spread_gold_spin.setSuffix(" نقطة")
        symbol_specific_layout.addRow("أقصى سبريد مسموح للذهب (نقاط):", self.max_spread_gold_spin)
        self.min_interval_gold_spin = QSpinBox(); self.min_interval_gold_spin.setRange(0, 1440); self.min_interval_gold_spin.setSuffix(" دقيقة")
        symbol_specific_layout.addRow("الفاصل الأدنى بين صفقات الذهب (دقائق):", self.min_interval_gold_spin)
        
        symbol_specific_layout.addRow(QtWidgets.QLabel("---")) # Separator

        self.bitcoin_symbol_edit = QLineEdit()
        symbol_specific_layout.addRow("رمز البيتكوين (مثال: BTCUSD):", self.bitcoin_symbol_edit)
        self.btc_sl_pips_spin = QSpinBox(); self.btc_sl_pips_spin.setRange(0, 200000); self.btc_sl_pips_spin.setSuffix(" نقطة")
        symbol_specific_layout.addRow("وقف الخسارة للبيتكوين (نقاط):", self.btc_sl_pips_spin)
        self.btc_tp_pips_spin = QSpinBox(); self.btc_tp_pips_spin.setRange(0, 400000); self.btc_tp_pips_spin.setSuffix(" نقطة")
        symbol_specific_layout.addRow("جني الأرباح للبيتكوين (نقاط):", self.btc_tp_pips_spin)
        self.max_spread_btc_spin = QSpinBox(); self.max_spread_btc_spin.setRange(0, 20000); self.max_spread_btc_spin.setSuffix(" نقطة")
        symbol_specific_layout.addRow("أقصى سبريد مسموح للبيتكوين (نقاط):", self.max_spread_btc_spin)
        self.min_interval_btc_spin = QSpinBox(); self.min_interval_btc_spin.setRange(0, 1440); self.min_interval_btc_spin.setSuffix(" دقيقة")
        symbol_specific_layout.addRow("الفاصل الأدنى بين صفقات البيتكوين (دقائق):", self.min_interval_btc_spin)

        symbol_specific_layout.addRow(QtWidgets.QLabel("---")) # Separator
        self.max_spread_other_spin = QSpinBox(); self.max_spread_other_spin.setRange(0, 1000); self.max_spread_other_spin.setSuffix(" نقطة")
        symbol_specific_layout.addRow("أقصى سبريد مسموح للرموز الأخرى (نقاط):", self.max_spread_other_spin)
        layout.addWidget(symbol_specific_group)
        return tab_widget

    def _create_timers_models_tab(self):
        tab_widget = QWidget()
        layout = QVBoxLayout(tab_widget)

        timers_group = QGroupBox("إعدادات المؤقتات")
        timers_form_layout = QFormLayout(timers_group)
        self.signals_refresh_interval_spin = QSpinBox(); self.signals_refresh_interval_spin.setRange(1, 1440); self.signals_refresh_interval_spin.setSuffix(" دقيقة")
        timers_form_layout.addRow("فاصل تحديث الإشارات:", self.signals_refresh_interval_spin)
        self.position_monitor_interval_spin = QSpinBox(); self.position_monitor_interval_spin.setRange(1, 300); self.position_monitor_interval_spin.setSuffix(" ثانية")
        timers_form_layout.addRow("فاصل مراقبة الصفقات:", self.position_monitor_interval_spin)
        layout.addWidget(timers_group)

        model_files_group = QGroupBox("أسماء ملفات النماذج")
        model_files_form_layout = QFormLayout(model_files_group)
        self.model_filename_gold_edit = QLineEdit()
        model_files_form_layout.addRow("ملف نموذج الذهب:", self.model_filename_gold_edit)
        self.model_filename_btc_edit = QLineEdit()
        model_files_form_layout.addRow("ملف نموذج البيتكوين:", self.model_filename_btc_edit)
        layout.addWidget(model_files_group)
        
        return tab_widget

    def _create_performance_news_tab(self):
        tab_widget = QWidget()
        layout = QVBoxLayout(tab_widget)

        perf_log_group = QGroupBox("إعدادات الأداء والتسجيل")
        perf_log_form_layout = QFormLayout(perf_log_group)
        self.log_closed_deals_checkbox = QCheckBox("تفعيل جلب/تسجيل الصفقات المغلقة من MT5")
        perf_log_form_layout.addRow(self.log_closed_deals_checkbox)
        self.initial_balance_analysis_spin = QDoubleSpinBox(); self.initial_balance_analysis_spin.setRange(1.0, 100000000.0); self.initial_balance_analysis_spin.setDecimals(2)
        perf_log_form_layout.addRow("الرصيد الأولي لتحليل الأداء:", self.initial_balance_analysis_spin)
        self.sharpe_periods_spin = QSpinBox(); self.sharpe_periods_spin.setRange(1, 1000)
        perf_log_form_layout.addRow("فترات شارب بالسنة:", self.sharpe_periods_spin)
        self.log_trade_requests_checkbox = QCheckBox("تسجيل طلبات الصفقات")
        perf_log_form_layout.addRow(self.log_trade_requests_checkbox)
        layout.addWidget(perf_log_group)

        news_filter_group = QGroupBox("إعدادات فلتر الأخبار")
        news_filter_form_layout = QFormLayout(news_filter_group)
        self.news_check_enabled_checkbox = QCheckBox("تفعيل التحقق من الأخبار")
        news_filter_form_layout.addRow(self.news_check_enabled_checkbox)
        self.news_check_interval_spin = QSpinBox(); self.news_check_interval_spin.setRange(1, 1440); self.news_check_interval_spin.setSuffix(" دقيقة") # Moved here
        news_filter_form_layout.addRow("فاصل التحقق من الأخبار:", self.news_check_interval_spin)
        self.news_impact_filter_edit = QLineEdit()
        news_filter_form_layout.addRow("فلتر أهمية الأخبار:", self.news_impact_filter_edit)
        self.halt_trades_on_news_checkbox = QCheckBox("إيقاف التداول عند الأخبار")
        news_filter_form_layout.addRow(self.halt_trades_on_news_checkbox)
        self.news_halt_before_spin = QSpinBox(); self.news_halt_before_spin.setRange(0, 120); self.news_halt_before_spin.setSuffix(" دقيقة")
        news_filter_form_layout.addRow("إيقاف قبل الخبر بـ:", self.news_halt_before_spin)
        self.news_halt_after_spin = QSpinBox(); self.news_halt_after_spin.setRange(0, 120); self.news_halt_after_spin.setSuffix(" دقيقة")
        news_filter_form_layout.addRow("إيقاف بعد الخبر بـ:", self.news_halt_after_spin)
        self.news_api_url_edit = QLineEdit()
        news_filter_form_layout.addRow("رابط API للأخبار:", self.news_api_url_edit)
        layout.addWidget(news_filter_group)
        return tab_widget

    def _create_filters_auto_close_tab(self):
        tab_widget = QWidget()
        layout = QVBoxLayout(tab_widget)

        time_filter_group = QGroupBox("إعدادات فلتر الوقت للتداول (UTC)")
        time_filter_form_layout = QFormLayout(time_filter_group)
        self.time_filter_enabled_checkbox = QCheckBox("تفعيل فلتر الوقت")
        time_filter_form_layout.addRow(self.time_filter_enabled_checkbox)
        self.trade_start_time_edit = QLineEdit()
        time_filter_form_layout.addRow("وقت بدء التداول (UTC):", self.trade_start_time_edit)
        self.trade_end_time_edit = QLineEdit()
        time_filter_form_layout.addRow("وقت انتهاء التداول (UTC):", self.trade_end_time_edit)
        layout.addWidget(time_filter_group)

        auto_close_group = QGroupBox("إعدادات الإغلاق التلقائي الكلي بالنقاط")
        auto_close_form_layout = QFormLayout(auto_close_group)
        self.auto_close_enabled_checkbox = QCheckBox("تفعيل الإغلاق عند ربح كلي")
        auto_close_form_layout.addRow(self.auto_close_enabled_checkbox)
        self.auto_close_target_points_spin = QSpinBox(); self.auto_close_target_points_spin.setRange(0, 1000000); self.auto_close_target_points_spin.setSuffix(" نقطة")
        auto_close_form_layout.addRow("هدف الربح الكلي:", self.auto_close_target_points_spin)
        layout.addWidget(auto_close_group)
        return tab_widget


    def _setup_ui(self):
        dialog_layout = QVBoxLayout(self)

        tab_widget = QTabWidget()
        
        # Create and add tabs
        tab_widget.addTab(self._create_mt5_tab(), "اتصال MT5")
        tab_widget.addTab(self._create_trading_tab(), "تداول عام")
        tab_widget.addTab(self._create_symbols_tab(), "رموز محددة")
        # Combine some smaller groups into logical tabs
        tab_widget.addTab(self._create_timers_models_tab(), "مؤقتات ونماذج")
        tab_widget.addTab(self._create_performance_news_tab(), "أداء وأخبار")
        tab_widget.addTab(self._create_filters_auto_close_tab(), "فلاتر وإغلاق تلقائي")
        
        dialog_layout.addWidget(tab_widget)

        # --- Dialog Buttons ---
        self.button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.button_box.accepted.connect(self.save_settings_from_ui)
        self.button_box.rejected.connect(self.reject)
        dialog_layout.addWidget(self.button_box)

    def load_settings_to_ui(self):
        # ... (نفس الكود السابق لـ load_settings_to_ui بدون تغيير) ...
        settings = self.data_manager.settings
        
        # MT5 Connection
        self.mt5_login_edit.setText(str(settings.get("mt5_login", "")))
        self.mt5_password_edit.setText(settings.get("mt5_password", "")) 
        self.mt5_server_edit.setText(settings.get("mt5_server", ""))
        self.mt5_path_edit.setText(settings.get("mt5_path", ""))
        self.mt5_magic_edit.setText(str(settings.get("mt5_magic_number", "234000")))
        self.mt5_retries_spin.setValue(settings.get("mt5_retries", 3))
        self.mt5_retry_delay_spin.setValue(settings.get("mt5_retry_delay", 2.0))
        self.mt5_timeout_ms_spin.setValue(settings.get("mt5_timeout_ms", 20000))

        # General Trading
        self.manual_filter_min_confidence_spin.setValue(settings.get("manual_filter_min_confidence", 70))
        self.auto_trade_enabled_checkbox.setChecked(settings.get("auto_trade_enabled", False))
        self.auto_trade_min_confidence_spin.setValue(settings.get("auto_trade_min_confidence", 75))
        self.risk_percent_spin.setValue(settings.get("risk_percent_per_trade", 1.0))
        self.default_sl_pips_spin.setValue(settings.get("default_sl_pips", 50))
        self.default_tp_pips_spin.setValue(settings.get("default_tp_pips", 100))
        self.min_trade_interval_default_spin.setValue(settings.get("min_trade_interval_minutes_default", 15))


        # Symbol Specific
        self.gold_symbol_edit.setText(settings.get("gold_symbol", "XAUUSD"))
        self.gold_sl_pips_spin.setValue(settings.get("gold_sl_pips", 300))
        self.gold_tp_pips_spin.setValue(settings.get("gold_tp_pips", 600))
        self.max_spread_gold_spin.setValue(settings.get("max_allowed_spread_points_gold", 30))
        self.min_interval_gold_spin.setValue(settings.get("min_trade_interval_minutes_xauusd", 10))


        self.bitcoin_symbol_edit.setText(settings.get("bitcoin_symbol", "BTCUSD"))
        self.btc_sl_pips_spin.setValue(settings.get("btc_sl_pips", 10000))
        self.btc_tp_pips_spin.setValue(settings.get("btc_tp_pips", 20000))
        self.max_spread_btc_spin.setValue(settings.get("max_allowed_spread_points_bitcoin", 1000))
        self.min_interval_btc_spin.setValue(settings.get("min_trade_interval_minutes_btcusd", 30))

        self.max_spread_other_spin.setValue(settings.get("max_allowed_spread_points_other", 50))

        # Timers
        self.signals_refresh_interval_spin.setValue(settings.get("signals_refresh_interval_minutes", 15))
        self.position_monitor_interval_spin.setValue(settings.get("position_monitor_interval_seconds", 15))
        self.news_check_interval_spin.setValue(settings.get("news_check_interval_minutes", 30))
        
        # Performance & Logging
        self.log_closed_deals_checkbox.setChecked(settings.get("log_closed_deals_enabled", True))
        self.initial_balance_analysis_spin.setValue(settings.get("default_initial_balance_for_analysis", 10000.0))
        self.sharpe_periods_spin.setValue(settings.get("sharpe_periods_per_year", 252))
        self.log_trade_requests_checkbox.setChecked(settings.get("log_trade_requests_enabled", True))

        # News Filter
        self.news_check_enabled_checkbox.setChecked(settings.get("news_check_enabled", True))
        news_impact_list = settings.get("news_impact_filter", ["High"]) 
        self.news_impact_filter_edit.setText(",".join(news_impact_list) if isinstance(news_impact_list, list) else str(news_impact_list))
        self.halt_trades_on_news_checkbox.setChecked(settings.get("halt_trades_on_news", True))
        self.news_halt_before_spin.setValue(settings.get("news_halt_minutes_before", 15))
        self.news_halt_after_spin.setValue(settings.get("news_halt_minutes_after", 15))
        self.news_api_url_edit.setText(settings.get("news_api_url_forex_factory", ""))

        # Time Filter
        self.time_filter_enabled_checkbox.setChecked(settings.get("time_filter_enabled", False))
        self.trade_start_time_edit.setText(settings.get("trade_start_time", "00:00"))
        self.trade_end_time_edit.setText(settings.get("trade_end_time", "23:59"))

        # Auto Close
        self.auto_close_enabled_checkbox.setChecked(settings.get("auto_close_by_points_enabled", False))
        self.auto_close_target_points_spin.setValue(settings.get("auto_close_target_points", 1000))

        # Model Filenames
        self.model_filename_gold_edit.setText(settings.get("current_model_filename", "model_XAUUSD.joblib"))
        self.model_filename_btc_edit.setText(settings.get("current_btc_model_filename", "model_BTCUSD.joblib"))

    def save_settings_from_ui(self):
        # ... (نفس الكود السابق لـ save_settings_from_ui بدون تغيير) ...
        self.data_manager.update_setting("mt5_login", self.mt5_login_edit.text())
        self.data_manager.update_setting("mt5_password", self.mt5_password_edit.text()) 
        self.data_manager.update_setting("mt5_server", self.mt5_server_edit.text())
        self.data_manager.update_setting("mt5_path", self.mt5_path_edit.text())
        try:
            self.data_manager.update_setting("mt5_magic_number", int(self.mt5_magic_edit.text()))
        except ValueError:
            QMessageBox.warning(self, "خطأ في الإدخال", "الرقم السحري لـ MT5 يجب أن يكون رقمًا صحيحًا. سيتم استخدام القيمة الافتراضية.")
            self.data_manager.update_setting("mt5_magic_number", self.data_manager.default_settings.get("mt5_magic_number")) 
        self.data_manager.update_setting("mt5_retries", self.mt5_retries_spin.value())
        self.data_manager.update_setting("mt5_retry_delay", self.mt5_retry_delay_spin.value())
        self.data_manager.update_setting("mt5_timeout_ms", self.mt5_timeout_ms_spin.value())

        # General Trading
        self.data_manager.update_setting("manual_filter_min_confidence", self.manual_filter_min_confidence_spin.value())
        self.data_manager.update_setting("auto_trade_enabled", self.auto_trade_enabled_checkbox.isChecked())
        self.data_manager.update_setting("auto_trade_min_confidence", self.auto_trade_min_confidence_spin.value())
        self.data_manager.update_setting("risk_percent_per_trade", self.risk_percent_spin.value())
        self.data_manager.update_setting("default_sl_pips", self.default_sl_pips_spin.value())
        self.data_manager.update_setting("default_tp_pips", self.default_tp_pips_spin.value())
        self.data_manager.update_setting("min_trade_interval_minutes_default", self.min_trade_interval_default_spin.value())

        # Symbol Specific
        self.data_manager.update_setting("gold_symbol", self.gold_symbol_edit.text())
        self.data_manager.update_setting("gold_sl_pips", self.gold_sl_pips_spin.value())
        self.data_manager.update_setting("gold_tp_pips", self.gold_tp_pips_spin.value())
        self.data_manager.update_setting("max_allowed_spread_points_gold", self.max_spread_gold_spin.value())
        self.data_manager.update_setting("min_trade_interval_minutes_xauusd", self.min_interval_gold_spin.value())


        self.data_manager.update_setting("bitcoin_symbol", self.bitcoin_symbol_edit.text())
        self.data_manager.update_setting("btc_sl_pips", self.btc_sl_pips_spin.value())
        self.data_manager.update_setting("btc_tp_pips", self.btc_tp_pips_spin.value())
        self.data_manager.update_setting("max_allowed_spread_points_bitcoin", self.max_spread_btc_spin.value())
        self.data_manager.update_setting("min_trade_interval_minutes_btcusd", self.min_interval_btc_spin.value())
        
        self.data_manager.update_setting("max_allowed_spread_points_other", self.max_spread_other_spin.value())

        # Timers
        self.data_manager.update_setting("signals_refresh_interval_minutes", self.signals_refresh_interval_spin.value())
        self.data_manager.update_setting("position_monitor_interval_seconds", self.position_monitor_interval_spin.value())
        self.data_manager.update_setting("news_check_interval_minutes", self.news_check_interval_spin.value())

        # Performance & Logging
        self.data_manager.update_setting("log_closed_deals_enabled", self.log_closed_deals_checkbox.isChecked())
        self.data_manager.update_setting("default_initial_balance_for_analysis", self.initial_balance_analysis_spin.value())
        self.data_manager.update_setting("sharpe_periods_per_year", self.sharpe_periods_spin.value())
        self.data_manager.update_setting("log_trade_requests_enabled", self.log_trade_requests_checkbox.isChecked())
        
        # News Filter
        self.data_manager.update_setting("news_check_enabled", self.news_check_enabled_checkbox.isChecked())
        news_impact_items = [item.strip() for item in self.news_impact_filter_edit.text().split(',') if item.strip()]
        self.data_manager.update_setting("news_impact_filter", news_impact_items)
        self.data_manager.update_setting("halt_trades_on_news", self.halt_trades_on_news_checkbox.isChecked())
        self.data_manager.update_setting("news_halt_minutes_before", self.news_halt_before_spin.value())
        self.data_manager.update_setting("news_halt_minutes_after", self.news_halt_after_spin.value())
        self.data_manager.update_setting("news_api_url_forex_factory", self.news_api_url_edit.text())

        # Time Filter
        self.data_manager.update_setting("time_filter_enabled", self.time_filter_enabled_checkbox.isChecked())
        self.data_manager.update_setting("trade_start_time", self.trade_start_time_edit.text()) 
        self.data_manager.update_setting("trade_end_time", self.trade_end_time_edit.text())   

        # Auto Close
        self.data_manager.update_setting("auto_close_by_points_enabled", self.auto_close_enabled_checkbox.isChecked())
        self.data_manager.update_setting("auto_close_target_points", self.auto_close_target_points_spin.value())

        # Model Filenames
        self.data_manager.update_setting("current_model_filename", self.model_filename_gold_edit.text())
        self.data_manager.update_setting("current_btc_model_filename", self.model_filename_btc_edit.text())

        self.data_manager.save_settings() 
        self.accept()

# ... (نفس الكود السابق لقسم الاختبار if __name__ == '__main__': بدون تغيير) ...
if __name__ == '__main__':
    import sys
    # Mock DataManager for testing
    class MockDataManager:
        def __init__(self):
            self.settings = { 
                "mt5_login": "12345", "mt5_password": "pass", "mt5_server": "TestServer", 
                "mt5_path": "C:/path", "mt5_magic_number": 999, "mt5_retries": 2, 
                "mt5_retry_delay": 5.0, "mt5_timeout_ms": 10000,
                "manual_filter_min_confidence": 60, "auto_trade_enabled": True,
                "auto_trade_min_confidence": 80, "risk_percent_per_trade": 0.5,
                "default_sl_pips": 40, "default_tp_pips": 80,
                "min_trade_interval_minutes_default": 20,
                "gold_symbol": "GOLD", "gold_sl_pips": 200, "gold_tp_pips": 400,
                "max_allowed_spread_points_gold": 20, "min_trade_interval_minutes_xauusd": 5,
                "bitcoin_symbol": "BTC", "btc_sl_pips": 5000, "btc_tp_pips": 15000,
                "max_allowed_spread_points_bitcoin": 500, "min_trade_interval_minutes_btcusd": 25,
                "max_allowed_spread_points_other": 40,
                "signals_refresh_interval_minutes": 10, "position_monitor_interval_seconds": 10,
                "news_check_interval_minutes": 20,
                "log_closed_deals_enabled": False, "default_initial_balance_for_analysis": 5000.0,
                "sharpe_periods_per_year": 200, "log_trade_requests_enabled": False,
                "news_check_enabled": False, "news_impact_filter": ["Medium"], 
                "halt_trades_on_news": False, "news_halt_minutes_before": 10,
                "news_halt_minutes_after": 10, "news_api_url_forex_factory": "http://example.com",
                "time_filter_enabled": True, "trade_start_time": "01:00", "trade_end_time": "22:00",
                "auto_close_by_points_enabled": True, "auto_close_target_points": 500,
                "current_model_filename": "model_GOLD.joblib", "current_btc_model_filename": "model_BTC.joblib"
            }
            self.default_settings = self.settings.copy() 
        def get_setting(self, key, default_override=None):
            if default_override is not None: return self.settings.get(key, default_override)
            return self.settings.get(key, self.default_settings.get(key))
        def update_setting(self, key, value): self.settings[key] = value; print(f"MockDM: Updated {key} to {value}")
        def save_settings(self): print("MockDM: save_settings called")

    app = QtWidgets.QApplication(sys.argv)
    mock_dm = MockDataManager()
    dialog = SettingsDialog(mock_dm)
    if dialog.exec() == QDialog.DialogCode.Accepted:
        print("Settings Dialog Accepted. Updated settings in MockDataManager:")
        for k, v in mock_dm.settings.items():
            print(f"  {k}: {v}")
    else:
        print("Settings Dialog Cancelled.")
    sys.exit(app.exec())