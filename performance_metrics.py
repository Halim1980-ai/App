import pandas as pd
import numpy as np
from datetime import datetime, timezone # Ensure datetime and timezone are imported
from matplotlib.figure import Figure # For type hinting if needed
import matplotlib.pyplot as plt # Crucial import for plt.setp

# Function to safely log messages, defaulting to print if no callback is provided
def _safe_log(log_callback, message, level="INFO"):
    if log_callback:
        log_callback(message, level)
    else:
        print(f"[{level}] {message}")


def calculate_equity_curve(deals_df_for_equity: pd.DataFrame,
                           initial_balance: float,
                           timestamp_col: str,
                           profit_col: str = 'profit',
                           log_callback=None):
    logger = lambda msg, lvl="INFO": _safe_log(log_callback, f"EquityCurve: {msg}", lvl)

    if deals_df_for_equity.empty or timestamp_col not in deals_df_for_equity.columns or profit_col not in deals_df_for_equity.columns:
        logger(f"Empty DataFrame or missing required columns ('{timestamp_col}', '{profit_col}'). Returning initial balance point.", "WARNING")
        start_time_for_no_trades = pd.Timestamp('1970-01-01 00:00:00', tz='UTC') 
        return pd.Series([initial_balance], index=[start_time_for_no_trades], name="equity")

    df_copy = deals_df_for_equity.copy() 
    if not pd.api.types.is_datetime64_any_dtype(df_copy[timestamp_col]):
        df_copy[timestamp_col] = pd.to_datetime(df_copy[timestamp_col], errors='coerce', utc=True)
    else: # Ensure it's UTC if already datetime
        if df_copy[timestamp_col].dt.tz is None:
            df_copy[timestamp_col] = df_copy[timestamp_col].dt.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
        else:
            df_copy[timestamp_col] = df_copy[timestamp_col].dt.tz_convert('UTC')

    df_copy[profit_col] = pd.to_numeric(df_copy[profit_col], errors='coerce')
    df_copy.dropna(subset=[timestamp_col, profit_col], inplace=True) 

    if df_copy.empty:
        logger("DataFrame empty after dropping NaNs from timestamp/profit. Returning initial balance point.", "WARNING")
        start_time_for_no_trades = pd.Timestamp('1970-01-01 00:00:00', tz='UTC')
        return pd.Series([initial_balance], index=[start_time_for_no_trades], name="equity")

    df_copy.sort_values(by=timestamp_col, inplace=True)

    if df_copy[timestamp_col].duplicated().any():
        logger(f"Duplicate timestamps found in '{timestamp_col}'. Grouping by timestamp and summing '{profit_col}'.", "INFO")
        profit_per_timestamp = df_copy.groupby(timestamp_col)[profit_col].sum()
    else:
        profit_per_timestamp = df_copy.set_index(timestamp_col)[profit_col]

    if profit_per_timestamp.empty:
        logger("No profit data after grouping by timestamp. Returning initial balance point.", "WARNING")
        start_time_for_no_trades = pd.Timestamp('1970-01-01 00:00:00', tz='UTC')
        return pd.Series([initial_balance], index=[start_time_for_no_trades], name="equity")

    cumulative_profit = profit_per_timestamp.cumsum()
    equity_series_from_trades = initial_balance + cumulative_profit
    
    first_trade_timestamp = equity_series_from_trades.index.min()

    if pd.isna(first_trade_timestamp): 
        logger("Could not determine first trade timestamp. Using a default early timestamp for initial balance.", "ERROR")
        initial_point_timestamp = pd.Timestamp('1970-01-01 00:00:00', tz='UTC')
    else:
        initial_point_timestamp = first_trade_timestamp - pd.Timedelta(nanoseconds=1) 
    
    initial_equity_point = pd.Series([initial_balance], index=[initial_point_timestamp], name="equity")
    
    full_equity_curve = pd.concat([initial_equity_point, equity_series_from_trades])
    full_equity_curve.sort_index(inplace=True)
    full_equity_curve = full_equity_curve[~full_equity_curve.index.duplicated(keep='first')]

    logger(f"Equity curve constructed. Length: {len(full_equity_curve)}. Index type: {type(full_equity_curve.index)}. Starts at: {full_equity_curve.index.min()}, Ends at: {full_equity_curve.index.max()}", "DEBUG")
    return full_equity_curve


def calculate_max_drawdown(equity_curve: pd.Series, log_callback=None):
    logger = lambda msg, lvl="INFO": _safe_log(log_callback, f"MaxDrawdown: {msg}", lvl)
    
    if equity_curve is None or equity_curve.empty or len(equity_curve) < 2:
        logger("Equity curve is empty or too short. Returning 0 drawdown and empty series.", "WARNING")
        idx = equity_curve.index if equity_curve is not None and not equity_curve.empty else pd.DatetimeIndex([])
        return 0.0, pd.Series(dtype=float, index=idx, name="drawdown_percentage")

    peak = equity_curve.expanding(min_periods=1).max()
    drawdown_values = equity_curve - peak  
    
    drawdown_percentage = pd.Series(index=equity_curve.index, dtype=float, name="drawdown_percentage")
    for i in drawdown_percentage.index:
        if peak[i] != 0:
            drawdown_percentage[i] = (drawdown_values[i] / peak[i]) * 100
        else: 
            drawdown_percentage[i] = 0 if equity_curve[i] == 0 else -100 

    drawdown_percentage[drawdown_percentage > 0] = 0 
    drawdown_percentage.fillna(0, inplace=True) 

    max_dd_pct = drawdown_percentage.min() 
    return max_dd_pct, drawdown_percentage


def calculate_streaks(profit_series: pd.Series):
    if profit_series.empty:
        return 0, 0
    
    longest_win_streak = 0
    current_win_streak = 0
    longest_lose_streak = 0
    current_lose_streak = 0

    for profit in profit_series:
        if profit > 0:
            current_win_streak += 1
            current_lose_streak = 0 
        elif profit < 0:
            current_lose_streak += 1
            current_win_streak = 0 
        else: 
            current_win_streak = 0 
            current_lose_streak = 0

        if current_win_streak > longest_win_streak:
            longest_win_streak = current_win_streak
        if current_lose_streak > longest_lose_streak:
            longest_lose_streak = current_lose_streak
            
    return longest_win_streak, longest_lose_streak

def get_performance_summary(deals_history_df: pd.DataFrame,
                            initial_balance: float = 10000.0,
                            primary_timestamp_col: str = 'close_time',
                            fallback_timestamp_col: str = 'open_time',
                            profit_col: str = 'profit',
                            periods_per_year_for_sharpe: int = 252, 
                            log_callback=None):
    logger = lambda msg, lvl="INFO": _safe_log(log_callback, f"PerformanceSummary: {msg}", lvl)
    
    summary = {
        "total_trades": 0, "net_profit_total": 0.0, "net_profit_pct": 0.0,
        "gross_profit": 0.0, "gross_loss": 0.0, "profit_factor": 0.0,
        "winning_trades": 0, "losing_trades": 0, "win_rate_pct": 0.0,
        "average_profit_per_trade": 0.0,
        "average_profit_per_winning_trade": 0.0,
        "average_loss_per_losing_trade": 0.0,
        "max_drawdown_pct": 0.0, "sharpe_ratio": np.nan, 
        "longest_winning_streak": 0, "longest_losing_streak": 0,
        "equity_curve_series": None, "drawdown_percentage_series": None,
        "final_equity": initial_balance,
        "actual_timestamp_col_used": "N/A"
    }

    placeholder_time = pd.Timestamp('1970-01-01 00:00:00', tz='UTC')
    summary["equity_curve_series"] = pd.Series([initial_balance], index=[placeholder_time], name="equity")
    summary["drawdown_percentage_series"] = pd.Series([0.0], index=[placeholder_time], name="drawdown_percentage")

    if deals_history_df is None or deals_history_df.empty or profit_col not in deals_history_df.columns:
        logger("No deals or profit column missing. Returning defaults with initial balance equity.", "INFO")
        summary["actual_timestamp_col_used"] = "N/A (No Trades)"
        return summary

    df_for_calc = deals_history_df.copy() 

    actual_ts_col = primary_timestamp_col
    # Ensure primary_timestamp_col is datetime and UTC for validity check
    temp_primary_ts = pd.to_datetime(df_for_calc.get(primary_timestamp_col), errors='coerce', utc=True)

    if primary_timestamp_col not in df_for_calc.columns or temp_primary_ts.isnull().sum() > len(df_for_calc) * 0.75: 
        logger(f"Primary timestamp col '{primary_timestamp_col}' mostly invalid. Trying fallback '{fallback_timestamp_col}'.", "WARNING")
        temp_fallback_ts = pd.to_datetime(df_for_calc.get(fallback_timestamp_col), errors='coerce', utc=True)
        if fallback_timestamp_col in df_for_calc.columns and not temp_fallback_ts.isnull().all():
            actual_ts_col = fallback_timestamp_col
            df_for_calc[actual_ts_col] = temp_fallback_ts # Use the converted series
            logger(f"Using fallback timestamp column '{actual_ts_col}'.", "INFO")
        else:
            logger(f"Fallback timestamp '{fallback_timestamp_col}' also invalid or missing. Cannot reliably calculate time-based metrics.", "ERROR")
            summary["actual_timestamp_col_used"] = "Error - No Valid Timestamp Column"
            summary["total_trades"] = len(df_for_calc)
            if profit_col in df_for_calc.columns:
                 df_for_calc[profit_col] = pd.to_numeric(df_for_calc[profit_col], errors='coerce')
                 df_for_calc.dropna(subset=[profit_col], inplace=True) 
                 summary["net_profit_total"] = df_for_calc[profit_col].sum()
                 if initial_balance != 0 : summary["net_profit_pct"] = (summary["net_profit_total"] / initial_balance) * 100
            return summary 
    else:
        df_for_calc[actual_ts_col] = temp_primary_ts # Use the converted series
        logger(f"Using primary timestamp column '{actual_ts_col}'.", "INFO")
    
    summary["actual_timestamp_col_used"] = actual_ts_col

    df_for_calc[profit_col] = pd.to_numeric(df_for_calc[profit_col], errors='coerce')
    df_for_calc.dropna(subset=[actual_ts_col, profit_col], inplace=True)

    if df_for_calc.empty:
        logger("DataFrame empty after dropping NaNs from chosen timestamp/profit. Returning defaults.", "INFO")
        summary["actual_timestamp_col_used"] = f"{actual_ts_col} (No Valid Data After Cleaning)"
        return summary

    summary["total_trades"] = len(df_for_calc)
    summary["net_profit_total"] = round(df_for_calc[profit_col].sum(), 2)
    if initial_balance != 0: 
        summary["net_profit_pct"] = round((summary["net_profit_total"] / initial_balance) * 100, 2)

    profits_series = df_for_calc[df_for_calc[profit_col] > 0][profit_col]
    losses_series = df_for_calc[df_for_calc[profit_col] < 0][profit_col]
    summary["gross_profit"] = round(profits_series.sum(), 2)
    summary["gross_loss"] = round(losses_series.sum(), 2) 

    if summary["gross_loss"] != 0: 
        summary["profit_factor"] = round(abs(summary["gross_profit"] / summary["gross_loss"]), 2)
    else: 
        summary["profit_factor"] = float('inf') if summary["gross_profit"] > 0 else 0.0

    summary["winning_trades"] = len(profits_series)
    summary["losing_trades"] = len(losses_series)

    if summary["total_trades"] > 0:
        summary["win_rate_pct"] = round((summary["winning_trades"] / summary["total_trades"]) * 100, 2)
        summary["average_profit_per_trade"] = round(summary["net_profit_total"] / summary["total_trades"], 2)
    
    if summary["winning_trades"] > 0:
        summary["average_profit_per_winning_trade"] = round(summary["gross_profit"] / summary["winning_trades"], 2)
    if summary["losing_trades"] > 0:
        summary["average_loss_per_losing_trade"] = round(summary["gross_loss"] / summary["losing_trades"], 2) 

    equity_curve = calculate_equity_curve(df_for_calc, initial_balance, actual_ts_col, profit_col, log_callback)
    summary["equity_curve_series"] = equity_curve 

    if equity_curve is not None and not equity_curve.empty:
        summary["final_equity"] = round(equity_curve.iloc[-1], 2)
        max_dd, dd_series = calculate_max_drawdown(equity_curve, log_callback)
        summary["max_drawdown_pct"] = round(max_dd, 2)
        summary["drawdown_percentage_series"] = dd_series 
        logger(f"Equity curve calculated. Initial: {initial_balance:.2f}, Final: {summary['final_equity']:.2f}. Max DD: {summary['max_drawdown_pct']:.2f}%", "INFO")
    else:
        logger("Equity curve calculation resulted in None or empty. Using initial balance for final equity.", "WARNING")
        summary["final_equity"] = initial_balance 
        dd_idx = df_for_calc[actual_ts_col].min() if not df_for_calc.empty and actual_ts_col in df_for_calc else placeholder_time
        summary["drawdown_percentage_series"] = pd.Series([0.0], index=[dd_idx], name="drawdown_percentage")

    if equity_curve is not None and len(equity_curve) >= 2: 
        equity_returns = equity_curve.pct_change().dropna() 
        if not equity_returns.empty and equity_returns.std() != 0 and not np.isinf(equity_returns.std()) and not np.isnan(equity_returns.std()):
            mean_return = equity_returns.mean()
            std_return = equity_returns.std()
            sharpe = (mean_return / std_return) * np.sqrt(periods_per_year_for_sharpe)
            summary["sharpe_ratio"] = round(sharpe, 2)
            logger(f"Sharpe Ratio calculated: {summary['sharpe_ratio']:.2f} (annualized using {periods_per_year_for_sharpe} periods). Mean return: {mean_return:.4f}, Std return: {std_return:.4f}", "INFO")
        else:
            logger("Not enough data or zero/invalid volatility for Sharpe Ratio calculation (equity returns).", "INFO")
            summary["sharpe_ratio"] = np.nan 
    else:
        logger("Equity curve too short or invalid for Sharpe Ratio calculation.", "INFO")
        summary["sharpe_ratio"] = np.nan 

    win_streak, lose_streak = calculate_streaks(df_for_calc[profit_col])
    summary["longest_winning_streak"] = win_streak
    summary["longest_losing_streak"] = lose_streak
    
    logger(f"Full summary calculated. Final Equity: {summary['final_equity']:.2f}", "DEBUG")
    return summary


def plot_performance_curves(equity_curve: pd.Series, 
                            drawdown_pct_series: pd.Series, 
                            figure_to_plot_on: Figure, 
                            title_suffix: str = ""):
    if figure_to_plot_on is None:
        print("[ERROR] PlotPerformance: Figure object is None. Cannot plot.") 
        return

    figure_to_plot_on.clear() 

    if equity_curve is None or equity_curve.empty or not isinstance(equity_curve.index, pd.DatetimeIndex) or len(equity_curve) < 1: 
        ax = figure_to_plot_on.add_subplot(111)
        ax.text(0.5, 0.5, f"لا توجد بيانات كافية لمنحنى حقوق الملكية\n{title_suffix}", 
                ha='center', va='center', fontsize=10, color='grey', transform=ax.transAxes)
        ax.set_xticks([]); ax.set_yticks([])
        try: figure_to_plot_on.tight_layout(pad=1.0)
        except Exception: pass 
        return

    gs = figure_to_plot_on.add_gridspec(2, 1, height_ratios=[2, 1]) 
    ax_equity = figure_to_plot_on.add_subplot(gs[0])
    ax_drawdown = figure_to_plot_on.add_subplot(gs[1], sharex=ax_equity) 

    ax_equity.plot(equity_curve.index, equity_curve.values, label='حقوق الملكية', color='dodgerblue', linewidth=1.8)
    ax_equity.set_ylabel('حقوق الملكية', color='dodgerblue', fontsize=10)
    ax_equity.tick_params(axis='y', labelcolor='dodgerblue', labelsize=9)
    ax_equity.set_title(f'منحنى حقوق الملكية والتراجع {title_suffix}', fontsize=12, pad=10)
    ax_equity.grid(True, linestyle=':', alpha=0.6, linewidth=0.7)
    ax_equity.legend(loc='upper left', fontsize=9)
    ax_equity.tick_params(axis='x', labelsize=9) 

    if drawdown_pct_series is not None and not drawdown_pct_series.empty and isinstance(drawdown_pct_series.index, pd.DatetimeIndex) and len(drawdown_pct_series) > 0:
        drawdown_to_plot = drawdown_pct_series.copy()
        
        ax_drawdown.fill_between(drawdown_to_plot.index, drawdown_to_plot.values, 0,
                                 where=drawdown_to_plot.values <= 0, 
                                 color='salmon', alpha=0.4, label='التراجع')
        ax_drawdown.plot(drawdown_to_plot.index, drawdown_to_plot.values, color='red', linewidth=1.2) 
        ax_drawdown.set_ylabel('التراجع (%)', color='red', fontsize=10)
        ax_drawdown.tick_params(axis='y', labelcolor='red', labelsize=9)
        ax_drawdown.grid(True, linestyle=':', alpha=0.6, linewidth=0.7)
        ax_drawdown.legend(loc='lower left', fontsize=9)
    else:
        ax_drawdown.text(0.5, 0.5, "لا توجد بيانات تراجع للعرض", 
                        ha='center', va='center', transform=ax_drawdown.transAxes, fontsize=9, color='grey')
    
    ax_drawdown.tick_params(axis='x', labelsize=9) 
    figure_to_plot_on.autofmt_xdate(rotation=15, ha='right') 
    plt.setp(ax_equity.get_xticklabels(), visible=False) 

    try:
        figure_to_plot_on.tight_layout(pad=1.5, h_pad=1.0, w_pad=1.0, rect=[0.03, 0.03, 0.97, 0.95])
    except Exception as e_layout:
        print(f"[WARNING] PlotPerformance: tight_layout failed: {e_layout}")