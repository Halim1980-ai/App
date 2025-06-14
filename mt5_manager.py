import MetaTrader5 as mt5
from datetime import datetime, timezone, timedelta
import pandas as pd
import time # Ensure time is imported for time.sleep
import traceback # For more detailed exception logging

class MT5Manager:
    _ORDER_TYPE_BUY = mt5.ORDER_TYPE_BUY
    _ORDER_TYPE_SELL = mt5.ORDER_TYPE_SELL
    _DEAL_ENTRY_IN = mt5.DEAL_ENTRY_IN
    _DEAL_ENTRY_OUT = mt5.DEAL_ENTRY_OUT
    _DEAL_ENTRY_INOUT = mt5.DEAL_ENTRY_INOUT
    _DEAL_ENTRY_OUT_BY = 3 # mt5.DEAL_ENTRY_OUT_BY (assuming it's 3, verify if different)


    def __init__(self, log_callback=None, data_manager=None):
        self.log_callback = log_callback if log_callback else print
        self.data_manager = data_manager
        self.connected = False
        self.last_raw_error_message = "" # MODIFIED: Initialize last raw error message
        self._load_config()

    def _log(self, message, level="INFO"):
        if self.log_callback:
            # Allow log_callback to handle the "MT5Manager:" prefix if it wants
            self.log_callback(message, level)
        else:
            print(f"MT5Manager: [{level}] {message}")

    def _load_config(self):
        if self.data_manager:
            self._log("MT5Manager: Attempting to load config from DataManager...", "DEBUG")
            self.login = self.data_manager.get_setting("mt5_login")
            self.password = self.data_manager.get_setting("mt5_password")
            self.server = self.data_manager.get_setting("mt5_server")
            self.path = self.data_manager.get_setting("mt5_path")
            self.magic_number = self.data_manager.get_setting("mt5_magic_number", 234000)
            self.retries = self.data_manager.get_setting("mt5_retries", 3) # Renamed from self.retry to self.retries for clarity
            self.retry_delay = self.data_manager.get_setting("mt5_retry_delay", 2.0)
            self.timeout_ms = self.data_manager.get_setting("mt5_timeout_ms", 20000)
            self._log(f"MT5Manager: Loaded credentials from DataManager - Login: '{self.login}', Server: '{self.server}', Path: '{self.path}'", "DEBUG")
            self._log(f"MT5Manager: Config reloaded: Magic={self.magic_number}, Retries={self.retries}, Delay={self.retry_delay}s, Timeout={self.timeout_ms}ms", "DEBUG")
        else:
            self._log("MT5Manager: DataManager not provided. Cannot load MT5 config dynamically.", "WARNING")
            self.login = None; self.password = None; self.server = None; self.path = None;
            self.magic_number = 234000; self.retries = 3; self.retry_delay = 2.0; self.timeout_ms = 20000;

    def _load_config_from_data_manager(self):
        self._load_config()

    def connect(self):
        self._log("MT5Manager: Connect method called.", "DEBUG")
        if self.connected:
            self._log("MT5Manager: Already connected.", "INFO")
            return True

        self._load_config() # Ensure latest config is used
        self.last_raw_error_message = "" # Reset last error message on new connect attempt

        if not self.login or not self.server or not self.path:
            self.last_raw_error_message = "MT5 connection details (login, server, path) are not fully configured."
            self._log(f"MT5Manager: {self.last_raw_error_message}", "ERROR")
            return False

        self._log(f"MT5Manager: Attempting connection with - Login: {self.login}, Server: {self.server}, Path: '{self.path}'", "INFO")
        for attempt in range(self.retries):
            self._log(f"MT5Manager: MT5 Connection attempt {attempt + 1}/{self.retries}...", "INFO")
            try:
                if not mt5.initialize(login=int(self.login), password=self.password, server=self.server, path=self.path, timeout=self.timeout_ms):
                    error_code, error_description = mt5.last_error()
                    self.last_raw_error_message = f"initialize() failed. Code: {error_code}, Description: {error_description}"
                    self._log(f"MT5Manager: {self.last_raw_error_message}", "ERROR")
                    if attempt < self.retries - 1:
                        self._log(f"MT5Manager: Retrying in {self.retry_delay} seconds...", "DEBUG")
                        time.sleep(self.retry_delay)
                    continue

                account_info = mt5.account_info()
                if account_info:
                    self.connected = True
                    self.last_raw_error_message = "" # Clear error on success
                    self._log(f"MT5Manager: MT5 connected: Acc: {account_info.login}, Name: {account_info.name}, Broker: {account_info.company}, Server: {account_info.server}, Curr: {account_info.currency}", "INFO")
                    return True
                else:
                    # This case might be rare if initialize() succeeded but account_info() fails immediately.
                    error_code, error_description = mt5.last_error()
                    self.last_raw_error_message = f"account_info() failed after successful initialize. Code: {error_code}, Desc: {error_description}"
                    self._log(f"MT5Manager: {self.last_raw_error_message}", "ERROR")
                    mt5.shutdown()
                    if attempt < self.retries - 1:
                        self._log(f"MT5Manager: Retrying in {self.retry_delay} seconds...", "DEBUG")
                        time.sleep(self.retry_delay)
            except ValueError as ve: # Specifically for int(self.login) if login is not a number
                self.last_raw_error_message = f"Configuration error: MT5 Login ('{self.login}') must be a number. Details: {ve}"
                self._log(f"MT5Manager: {self.last_raw_error_message}", "ERROR")
                # No retries for config errors usually
                return False # Exit connect attempt if login is invalid format
            except Exception as e:
                self.last_raw_error_message = f"Exception during MT5 connection attempt {attempt + 1}: {str(e)}"
                self._log(f"MT5Manager: {self.last_raw_error_message}\n{traceback.format_exc()}", "ERROR")
                if mt5.terminal_info(): # Check if terminal was initialized before exception
                    mt5.shutdown()
                if attempt < self.retries - 1:
                    self._log(f"MT5Manager: Retrying in {self.retry_delay} seconds...", "DEBUG")
                    time.sleep(self.retry_delay)

        self.connected = False
        if not self.last_raw_error_message: # If loop finishes without setting a specific error
            self.last_raw_error_message = "Failed to connect to MT5 after all retries. Unknown reason if no specific error was logged."
        self._log(f"MT5Manager: Failed to connect to MT5. Last error: {self.last_raw_error_message}", "ERROR")
        return False

    def disconnect(self):
        if self.connected:
            mt5.shutdown()
            self.connected = False
            self._log("MT5Manager: MT5 connection terminated.", "INFO")
        else:
            self._log("MT5Manager: Not connected, no need to terminate.", "DEBUG")

    def is_connected(self):
        # Simple check for now. More robust checks can be added if needed.
        # e.g., mt5.terminal_info() is not None
        return self.connected

    def get_account_info(self):
        if not self.is_connected():
            self._log("MT5Manager: get_account_info - Not connected.", "WARNING")
            return None
        return mt5.account_info()

    def get_symbol_info(self, symbol: str):
        if not self.is_connected():
            self._log(f"MT5Manager: get_symbol_info for {symbol} - Not connected.", "WARNING")
            return None
        return mt5.symbol_info(symbol)

    def get_tick(self, symbol: str):
        if not self.is_connected():
            self._log(f"MT5Manager: get_tick for {symbol} - Not connected.", "WARNING")
            return None
        return mt5.symbol_info_tick(symbol)

    def get_current_spread(self, symbol: str) -> int | None:
        if not self.is_connected():
            self._log(f"MT5Manager: get_current_spread for {symbol}: Not connected to MT5.", "WARNING")
            return None

        tick = self.get_tick(symbol)
        if tick and hasattr(tick, 'spread') and isinstance(tick.spread, int) and tick.spread >= 0 : # MT5 spread is in points (integer)
            # self._log(f"MT5Manager: Spread for {symbol} from tick_info: {tick.spread} points.", "DEBUG")
            return tick.spread
        else:
            # Fallback: try to get from symbol_info if tick.spread is not available/valid
            # symbol_info.spread is also in points.
            symbol_info = self.get_symbol_info(symbol)
            if symbol_info and hasattr(symbol_info, 'spread') and isinstance(symbol_info.spread, int) and symbol_info.spread >=0:
                # self._log(f"MT5Manager: Spread for {symbol} from symbol_info: {symbol_info.spread} points (fallback).", "DEBUG")
                return symbol_info.spread
            else:
                err_msg = f"MT5Manager: Could not determine spread for {symbol}. "
                err_msg += f"Tick: {tick._asdict() if tick else 'None'}. "
                err_msg += f"SymbolInfo: Spread={getattr(symbol_info, 'spread', 'N/A')} if SymbolInfo exists."
                self._log(err_msg, "WARNING")
                return None

    def get_historical_data(self, symbol: str, timeframe_str: str, count: int):
        if not self.is_connected():
            self._log(f"MT5Manager: get_historical_data for {symbol} - Not connected.", "WARNING")
            return pd.DataFrame()

        tf_map = {
            "M1": mt5.TIMEFRAME_M1, "M5": mt5.TIMEFRAME_M5, "M15": mt5.TIMEFRAME_M15,
            "M30": mt5.TIMEFRAME_M30, "H1": mt5.TIMEFRAME_H1, "H4": mt5.TIMEFRAME_H4,
            "D1": mt5.TIMEFRAME_D1, "W1": mt5.TIMEFRAME_W1, "MN1": mt5.TIMEFRAME_MN1
        }
        timeframe = tf_map.get(timeframe_str.upper())
        if not timeframe:
            self.last_raw_error_message = f"Invalid timeframe string: {timeframe_str}"
            self._log(f"MT5Manager: {self.last_raw_error_message}", "ERROR")
            return pd.DataFrame()

        try:
            rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, count)
            if rates is None or len(rates) == 0:
                error_code, error_description = mt5.last_error()
                self.last_raw_error_message = f"No historical data for {symbol}, {timeframe_str}. Code: {error_code}, Desc: {error_description}"
                self._log(f"MT5Manager: {self.last_raw_error_message}", "WARNING")
                return pd.DataFrame()

            df = pd.DataFrame(rates)
            df['Timestamp'] = pd.to_datetime(df['time'], unit='s', utc=True)
            df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'tick_volume': 'Volume'}, inplace=True)
            # self._log(f"MT5Manager: Successfully fetched {len(df)} candles for {symbol} {timeframe_str}.", "DEBUG")
            return df[['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']]
        except Exception as e:
            self.last_raw_error_message = f"Exception in get_historical_data for {symbol}, {timeframe_str}: {str(e)}"
            self._log(f"MT5Manager: {self.last_raw_error_message}\n{traceback.format_exc()}", "ERROR")
            return pd.DataFrame()

    def get_deals_history(self, from_date: datetime, to_date: datetime, magic: int = None):
        if not self.is_connected():
            self._log("MT5Manager: get_deals_history - Not connected.", "WARNING")
            return None

        self._log(f"MT5Manager: Fetching deals from MT5: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}, Magic: {magic if magic is not None else 'Any'}", "DEBUG")
        try:
            if from_date.tzinfo is None: from_date = from_date.replace(tzinfo=timezone.utc)
            if to_date.tzinfo is None: to_date = to_date.replace(tzinfo=timezone.utc)

            deals = mt5.history_deals_get(from_date, to_date)

            if deals is None or len(deals) == 0:
                error_code, error_description = mt5.last_error()
                self._log(f"MT5Manager: No deals found in MT5 for the period/magic. Code: {error_code}, Desc: {error_description}", "INFO")
                return pd.DataFrame()

            df_deals = pd.DataFrame(list(deals), columns=deals[0]._asdict().keys())
            self._log(f"MT5Manager: Fetched {len(df_deals)} deals from MT5 before filtering by magic.", "DEBUG")

            if 'time_msc' in df_deals.columns:
                df_deals['open_time'] = pd.NaT
                df_deals['close_time'] = pd.NaT
                in_mask = df_deals['entry'] == mt5.DEAL_ENTRY_IN
                out_mask = df_deals['entry'] == mt5.DEAL_ENTRY_OUT
                df_deals.loc[in_mask, 'open_time'] = pd.to_datetime(df_deals.loc[in_mask, 'time_msc'], unit='ms', utc=True)
                df_deals.loc[out_mask, 'close_time'] = pd.to_datetime(df_deals.loc[out_mask, 'time_msc'], unit='ms', utc=True)
            else:
                self._log("MT5Manager: 'time_msc' column not found in MT5 deals.", "WARNING")
            
            if 'time' in df_deals.columns: # Original order time (seconds)
                 df_deals['order_creation_time_utc'] = pd.to_datetime(df_deals['time'], unit='s', utc=True)


            if magic is not None:
                if 'magic' in df_deals.columns:
                    df_deals = df_deals[df_deals['magic'] == magic].copy()
                    self._log(f"MT5Manager: Filtered to {len(df_deals)} deals by magic number {magic}.", "DEBUG")
                else:
                    self._log(f"MT5Manager: 'magic' column not found for MT5 deals filtering, cannot filter by magic.", "WARNING")

            self._log(f"MT5Manager: Returning {len(df_deals)} deals from MT5 after processing.", "INFO")
            return df_deals

        except Exception as e:
            self.last_raw_error_message = f"Error getting deals history from MT5: {str(e)}"
            self._log(f"MT5Manager: {self.last_raw_error_message}\n{traceback.format_exc()}", "ERROR")
            return None

    def send_order(self, symbol, order_type, volume, price=None, sl=0.0, tp=0.0, comment=""):
        if not self.is_connected():
            self.last_raw_error_message = "Not connected to MT5"
            return False, self.last_raw_error_message

        order_type_mt5 = None
        current_tick = mt5.symbol_info_tick(symbol)
        if not current_tick:
            self.last_raw_error_message = f"Failed to get tick for {symbol} to determine price."
            self._log(f"MT5Manager: {self.last_raw_error_message}", "ERROR")
            return False, self.last_raw_error_message

        if order_type.lower() == 'buy':
            order_type_mt5 = mt5.ORDER_TYPE_BUY
            if price is None: price = current_tick.ask
        elif order_type.lower() == 'sell':
            order_type_mt5 = mt5.ORDER_TYPE_SELL
            if price is None: price = current_tick.bid
        else:
            self.last_raw_error_message = "Invalid order type specified"
            return False, self.last_raw_error_message

        if price is None or price <= 0:
            self.last_raw_error_message = f"Order price for {symbol} is invalid ({price}). Tick Ask: {current_tick.ask}, Bid: {current_tick.bid}"
            self._log(f"MT5Manager: {self.last_raw_error_message}", "ERROR")
            return False, self.last_raw_error_message

        request = {
            "action": mt5.TRADE_ACTION_DEAL, "symbol": symbol, "volume": float(volume),
            "type": order_type_mt5, "price": float(price), "sl": float(sl), "tp": float(tp),
            "deviation": 20, # Increased default deviation slightly
            "magic": int(self.magic_number), "comment": comment[:31], # Ensure comment length
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC, # Changed from FOK to IOC as it's generally more accepted by brokers
        }
        self._log(f"MT5Manager: Sending order request: {request}", "DEBUG")

        check_result = mt5.order_check(request)
        if check_result is None:
            error_code, error_description = mt5.last_error()
            self.last_raw_error_message = f"order_check failed, returned None. Code: {error_code}, Desc: {error_description}"
            self._log(f"MT5Manager: {self.last_raw_error_message}", "ERROR")
            return False, f"Order check failed: {self._get_retcode_description(error_code)} ({error_description})"
        if check_result.retcode != mt5.TRADE_RETCODE_DONE:
            self.last_raw_error_message = f"Order check failed. Retcode: {check_result.retcode} - {self._get_retcode_description(check_result.retcode)}. Comment: {check_result.comment}"
            self._log(f"MT5Manager: {self.last_raw_error_message}", "ERROR")
            return False, f"Order check failed: {check_result.comment} ({self._get_retcode_description(check_result.retcode)})"
        self._log(f"MT5Manager: Order check successful for {symbol}. Proceeding to send.", "DEBUG")

        result = mt5.order_send(request)
        if result is None:
            error_code, error_description = mt5.last_error()
            self.last_raw_error_message = f"order_send failed, returned None. Code: {error_code}, Desc: {error_description}"
            self._log(f"MT5Manager: {self.last_raw_error_message}", "ERROR")
            return False, f"Order send failed: {self._get_retcode_description(error_code)} ({error_description})"

        self._log(f"MT5Manager: Order send result: Code={result.retcode}, Deal={result.deal}, Order={result.order}, Comment='{result.comment}'", "INFO")

        if result.retcode == mt5.TRADE_RETCODE_DONE or result.retcode == mt5.TRADE_RETCODE_PLACED:
            self.last_raw_error_message = "" # Clear error on success
            return True, result
        else:
            self.last_raw_error_message = f"Order send failed: {result.comment} (Code: {result.retcode} - {self._get_retcode_description(result.retcode)})"
            return False, self.last_raw_error_message # Return the detailed message

    def get_open_positions(self, symbol: str = None, magic: int = None):
        if not self.is_connected():
            self._log("MT5Manager: get_open_positions - Not connected.", "WARNING")
            return []
        try:
            if symbol:
                positions = mt5.positions_get(symbol=symbol)
            else:
                positions = mt5.positions_get()

            if positions is None:
                error_code, error_description = mt5.last_error()
                self._log(f"MT5Manager: Failed to get positions. Code: {error_code}, Desc: {error_description}", "WARNING")
                return []

            if magic is not None:
                return [p for p in positions if p.magic == magic]
            return list(positions)
        except Exception as e:
            self._log(f"MT5Manager: Error getting open positions: {e}\n{traceback.format_exc()}", "ERROR")
            return []

    def get_open_positions_count(self, symbol: str = None, magic: int = None) -> int:
        return len(self.get_open_positions(symbol=symbol, magic=magic))

    def close_all_trades(self, magic: int = None, comment: str = "CloseAll"):
        if not self.is_connected():
            self.last_raw_error_message = "Not connected to MT5"
            return False, self.last_raw_error_message

        closed_count = 0
        failed_count = 0
        summary_messages = []
        final_status_message = ""

        positions_to_close = self.get_open_positions(magic=magic)
        if not positions_to_close:
            msg = f"No open positions found to close (Magic: {magic if magic is not None else 'Any'})."
            self._log(f"MT5Manager: {msg}", "INFO")
            return True, msg

        self._log(f"MT5Manager: Attempting to close {len(positions_to_close)} positions (Magic: {magic if magic is not None else 'Any'}).", "INFO")

        for position in positions_to_close:
            symbol = position.symbol
            volume = position.volume
            ticket = position.ticket
            order_type_to_close = mt5.ORDER_TYPE_SELL if position.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY

            current_tick_close = mt5.symbol_info_tick(symbol)
            if not current_tick_close:
                self._log(f"MT5Manager: Failed to get tick for closing {ticket} on {symbol}. Skipping.", "ERROR")
                failed_count += 1
                summary_messages.append(f"Pos {ticket}({symbol}): Price error.")
                continue

            price_to_close = 0.0
            if order_type_to_close == mt5.ORDER_TYPE_SELL: # Closing a BUY position
                price_to_close = current_tick_close.bid
            else: # Closing a SELL position
                price_to_close = current_tick_close.ask

            if price_to_close <= 0.0:
                self._log(f"MT5Manager: Invalid price ({price_to_close}) for closing position {ticket} on {symbol}. Skipping.", "ERROR")
                failed_count += 1
                summary_messages.append(f"Pos {ticket}({symbol}): Invalid close price.")
                continue

            request = {
                "action": mt5.TRADE_ACTION_DEAL, "symbol": symbol, "volume": volume,
                "type": order_type_to_close, "position": ticket, "price": price_to_close,
                "deviation": 20, "magic": position.magic, # Use original magic
                "comment": comment[:31], "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            self._log(f"MT5Manager: Closing position {ticket} ({symbol}) with request: {request}", "DEBUG")
            result = mt5.order_send(request)

            if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                self._log(f"MT5Manager: Successfully closed position {ticket} ({symbol}). Deal: {result.deal}", "INFO")
                closed_count += 1
                summary_messages.append(f"Pos {ticket}({symbol}): Closed.")
            else:
                err_desc = ""
                if result:
                    err_desc = f"Code: {result.retcode} - {self._get_retcode_description(result.retcode)}, Comment: {result.comment}"
                else:
                    l_err_c, l_err_d = mt5.last_error()
                    err_desc = f"MT5 Error Code: {l_err_c} - {l_err_d}"
                self._log(f"MT5Manager: Failed to close position {ticket} ({symbol}). {err_desc}", "ERROR")
                failed_count += 1
                summary_messages.append(f"Pos {ticket}({symbol}): Fail - {result.comment if result else err_desc}")

        final_status_message = f"Close All Summary (Magic: {magic if magic is not None else 'Any'}): Closed {closed_count}, Failed {failed_count}."
        if summary_messages:
            final_status_message += " Details: " + '; '.join(summary_messages)
        
        self._log(f"MT5Manager: {final_status_message}", "INFO")
        self.last_raw_error_message = final_status_message if failed_count > 0 else "" # Store if there were failures

        return failed_count == 0, final_status_message

    def _get_retcode_description(self, retcode):
        codes = {
            10004: "Requote", 10006: "Request rejected", 10007: "Request canceled by trader",
            10008: "Order placed", 10009: "Request completed (TRADE_RETCODE_DONE)", 10010: "Request partially completed",
            10011: "Request processing error", 10012: "Request timed out", 10013: "Invalid request",
            10014: "Invalid volume", 10015: "Invalid price", 10016: "Invalid stops",
            10017: "Trade is disabled", 10018: "Market is closed", 10019: "Not enough money",
            10020: "Price changed", 10021: "No quotes", 10022: "Invalid expiration",
            10023: "Order state changed", 10024: "Too frequent requests", 10025: "No changes",
            10026: "Autotrading disabled by server", 10027: "Autotrading disabled by client",
            10028: "Request locked for processing", 10030: "No connection", 10031: "Operation canceled",
            10032: "SL is too close to market", 10033: "TP is too close to market",
            10034: "Order is closed", 10035: "Position is closed", 10036: "Too many requests",
            10038: "Position not found", 10039: "Volume is too large", 10040: "Volume is too small",
            10041: "Invalid SL", 10042: "Invalid TP", 10043: "History request failed",
            10044: "Trading disabled for symbol", 10045: "Closing order only allowed",
            10046: "Order is being processed",
            mt5.TRADE_RETCODE_PLACED: "Order placed (pending or part of market execution)"
        }
        # Try to get description from MetaTrader5.last_error() if available and code matches
        # This is just an idea, last_error() might not always correspond to the retcode directly in all contexts.
        # last_err_code, last_err_desc = mt5.last_error()
        # if last_err_code == retcode and last_err_desc:
        #    return f"{codes.get(retcode, f'Unknown retcode: {retcode}')} (MT5: {last_err_desc})"
        return codes.get(retcode, f"Unknown retcode: {retcode}")
