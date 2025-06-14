from PyQt6 import QtCore
import logging
from datetime import datetime, timezone, timedelta

# Assuming DataManager is in a file named data_manager.py in the same directory
# from data_manager import DataManager # This line would be used in the main app file

class NewsManager(QtCore.QObject):
    news_updated = QtCore.pyqtSignal(list) # list of tuples: (title, datetime_utc)

    def __init__(self, log_callback=None, data_manager=None):
        super().__init__()
        self.logger = logging.getLogger(__name__ + ".NewsManager")
        self.log_callback = log_callback if log_callback else lambda msg, lvl="INFO": self.logger.info(f"[{lvl}] {msg}")
        self.data_manager = data_manager # Expecting a DataManager instance
        self.timer = QtCore.QTimer(self)
        self.timer.setObjectName("NewsCheckTimer") # For easier identification in logs
        self.timer.timeout.connect(self.check_news_and_emit)
        
        if self.data_manager:
            # Initial check for enabled status before starting timer
            if not self.data_manager.get_setting("news_check_enabled", True):
                self.log_callback("NewsManager initialized, but news check is disabled in settings. Timer not started.", "INFO")
            else:
                self._update_timer_interval()
                # Initial check soon after startup
                QtCore.QTimer.singleShot(1000, self.check_news_and_emit) 
                self.log_callback("NewsManager initialized and timer started for news checks.", "DEBUG")
        else:
            self.log_callback("NewsManager initialized WITHOUT DataManager. Timer not started. News checks will be disabled.", "WARNING")


    def _update_timer_interval(self):
        if not self.data_manager:
            self.log_callback("Cannot update news timer: DataManager not available.", "ERROR")
            if self.timer.isActive():
                self.timer.stop()
            return

        # --- Start: Added news_check_enabled check ---
        if not self.data_manager.get_setting("news_check_enabled", True):
            if self.timer.isActive():
                self.timer.stop()
            self.log_callback("News check is disabled in settings. Timer stopped/not updating interval.", "INFO")
            # Optionally emit an empty list if you want UI to clear news when disabled
            # if hasattr(self, 'news_updated'):
            # self.news_updated.emit([])
            return
        # --- End: Added news_check_enabled check ---

        interval_minutes = self.data_manager.get_setting("news_check_interval_minutes", 30)
        if self.timer.isActive():
            self.timer.stop()
        
        if interval_minutes > 0:
            self.timer.start(interval_minutes * 60 * 1000)
            self.log_callback(f"News check timer interval set to {interval_minutes} minutes.", "INFO")
        else:
            self.log_callback(f"News check timer interval is {interval_minutes} minutes. Timer will not start.", "WARNING")


    def check_news_and_emit(self):
        # --- Start: Added news_check_enabled check ---
        if not self.data_manager or not self.data_manager.get_setting("news_check_enabled", True):
            # self.log_callback("News check is disabled in settings. Skipping check_news_and_emit.", "DEBUG") # Potentially too verbose
            # Ensure timer is stopped if it somehow became active while disabled
            if self.timer.isActive() and self.data_manager.get_setting("news_check_interval_minutes", 30) > 0 : # Check interval too, to avoid stopping a stopped timer
                 # Check interval condition to prevent stopping if interval is 0 (already stopped)
                is_actually_enabled_now = self.data_manager.get_setting("news_check_enabled", True)
                if not is_actually_enabled_now: # Double check, setting might have changed
                    self.timer.stop()
                    self.log_callback("News check became disabled. Timer stopped during check_news_and_emit.", "INFO")

            if hasattr(self, 'news_updated'): # Emit empty list if disabled
                self.news_updated.emit([])
            return
        # --- End: Added news_check_enabled check ---

        self.log_callback("Checking for news (Placeholder)...", "DEBUG")
        # This is a placeholder. In a real scenario, you'd fetch news from an API.
        # Example placeholder news items (title, datetime_utc_naive)
        # In a real app, dt_utc_naive should be actual UTC datetimes from the news source.
        news_items_placeholder = [
            # ("HIGH IMPACT: US Non-Farm Payrolls", datetime.now(timezone.utc) - timedelta(minutes=5)),
            # ("MEDIUM IMPACT: ECB Press Conference", datetime.now(timezone.utc) + timedelta(minutes=30)),
            # ("LOW IMPACT: German ZEW Economic Sentiment", datetime.now(timezone.utc) - timedelta(hours=1)),
        ]
        
        # These should ideally come from DataManager or be more configurable
        high_impact_keywords = ["HIGH IMPACT", "ECB", "FOMC", "NFP", "CPI", "INTEREST RATE", "RATE DECISION", "GDP", "UNEMPLOYMENT"]
        # Example: Get impact filter from DataManager
        # impact_filter_settings = self.data_manager.get_setting("news_impact_filter", ["High"]) # e.g. ["High", "Medium"]
        # This would require news source to provide impact level string.

        now_utc = datetime.now(timezone.utc)
        relevant_news = []

        # --- Actual News Fetching and Parsing Logic Would Go Here ---
        # For example, if fetching from Forex Factory XML:
        # news_url = self.data_manager.get_setting("news_api_url_forex_factory")
        # if news_url:
        #     try:
        #         # response = requests.get(news_url)
        #         # response.raise_for_status()
        #         # root = ET.fromstring(response.content)
        #         # Iterate through news items in XML (e.g., <event> tags)
        #         # Extract title, date, time, impact, currency
        #         # Convert date/time to UTC datetime objects
        #         # Populate news_items_placeholder or directly process here
        #         pass # Placeholder for actual implementation
        #     except Exception as e:
        #         self.log_callback(f"Error fetching or parsing news: {e}", "ERROR")
        #         # Potentially emit empty list or cached data on error
        #         self.news_updated.emit([])
        #         return
        # else:
        #     self.log_callback("News API URL not configured.", "WARNING")
        #     self.news_updated.emit([])
        #     return
        # --- End Actual News Fetching ---


        for title, dt_utc_item in news_items_placeholder: # This loop will be empty with current placeholder
            # Ensure dt_utc_item is timezone-aware (UTC)
            if dt_utc_item.tzinfo is None:
                dt_utc_aware = dt_utc_item.replace(tzinfo=timezone.utc)
            else:
                dt_utc_aware = dt_utc_item.astimezone(timezone.utc)

            # Determine impact based on title or source data
            # For placeholder, we use keywords. For real data, source might give impact directly.
            is_high_impact_by_keyword = any(keyword in title.upper() for keyword in high_impact_keywords)
            
            # Example: Check against impact_filter_settings
            # actual_impact_from_source = "High" # This would come from the news item
            # if actual_impact_from_source not in impact_filter_settings:
            #     continue # Skip if not matching desired impact levels

            time_difference = dt_utc_aware - now_utc
            
            # Use configurable halt margins for relevance window (example)
            # These settings are for the TradingApp to halt, but can guide NewsManager's relevance window
            halt_before_min = self.data_manager.get_setting("news_halt_minutes_before", 15)
            halt_after_min = self.data_manager.get_setting("news_halt_minutes_after", 15)
            
            # Define a broader window for "relevant" news to display, e.g., -60 mins to +120 mins
            # The actual trading halt is determined by TradingApp using news_halt_minutes_before/after
            relevant_window_past = timedelta(minutes=-60) # Show news from last 60 mins
            relevant_window_future = timedelta(minutes=120) # Show news up to 120 mins in future

            is_relevant_time = (relevant_window_past < time_difference < relevant_window_future)

            if is_high_impact_by_keyword and is_relevant_time: # Adapt this condition based on actual impact data
                 relevant_news.append((title, dt_utc_aware))
        
        if relevant_news:
            self.log_callback(f"Relevant news found: {len(relevant_news)} items (based on placeholder/keywords).", "WARNING")
            for title, dt_utc_event in relevant_news:
                 self.log_callback(f" - News: '{title}' at {dt_utc_event.strftime('%Y-%m-%d %H:%M UTC')}", "DEBUG")
        else:
            self.log_callback("No relevant news items found in the current window (placeholder data is empty).", "DEBUG")
            
        self.news_updated.emit(relevant_news)

# Example usage (for testing this module standalone)
# if __name__ == '__main__':
#     import sys
#     app = QtCore.QCoreApplication(sys.argv) # QCoreApplication for non-GUI test
#     
#     class DummyDataManager:
#         _settings = {"news_check_enabled": True, "news_check_interval_minutes": 0.1} # Check every 6 seconds for test
#
#         def get_setting(self, key, default=None):
#             return self._settings.get(key, default)
#
#     def test_log_callback(message, level):
#         print(f"[{datetime.now().isoformat(sep=' ', timespec='milliseconds')}] [{level}] {message}")
#
#     dummy_dm = DummyDataManager()
#     news_manager_instance = NewsManager(log_callback=test_log_callback, data_manager=dummy_dm)
#
#     def on_news(news_list_data):
#         print(f"--- News Updated Signal Received ({len(news_list_data)} items) ---")
#         for title, dt in news_list_data:
#             print(f"  '{title}' at {dt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
#         if not news_list_data:
#             print("  (No relevant news items)")
#
#     news_manager_instance.news_updated.connect(on_news)
#     
#     print("NewsManager test started. Will check for news periodically...")
#     # To test disabling:
#     # QtCore.QTimer.singleShot(10000, lambda: dummy_dm._settings.update({"news_check_enabled": False}))
#     # print("News check will be disabled in 10 seconds.")
#
#     sys.exit(app.exec())