import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report 
import joblib
import os
import traceback 

def add_technical_indicators(df: pd.DataFrame, log_callback=None) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty:
        if log_callback: log_callback("add_technical_indicators: Input DataFrame is empty. Cannot add indicators.", "WARNING")
        return pd.DataFrame() 

    df_out = df.copy()

    if 'Close' not in df_out.columns:
        if log_callback: log_callback("add_technical_indicators: 'Close' column missing. Cannot calculate price-based indicators.", "ERROR")
        return df_out 
    try:
        df_out['Close'] = pd.to_numeric(df_out['Close'], errors='raise')
    except ValueError:
        if log_callback: log_callback("add_technical_indicators: 'Close' column contains non-numeric values. Cannot calculate price-based indicators.", "ERROR")
        return df_out

    sma_window = 50
    if len(df_out) >= sma_window:
        try:
            df_out['SMA_50'] = df_out['Close'].rolling(window=sma_window, min_periods=1).mean() 
            df_out['SMA_50'].fillna(method='ffill', inplace=True)
            df_out['SMA_50'].fillna(method='bfill', inplace=True)
        except Exception as e_sma:
            if log_callback: log_callback(f"add_technical_indicators: Error calculating SMA_50: {e_sma}", "ERROR")
            df_out['SMA_50'] = np.nan 
    else:
        if log_callback: log_callback(f"add_technical_indicators: Not enough data for SMA_50 (need {sma_window}, got {len(df_out)}). Setting SMA_50 to NaN.", "WARNING")
        df_out['SMA_50'] = np.nan

    rsi_window = 14
    if len(df_out) >= rsi_window + 1: 
        try:
            delta = df_out['Close'].diff(1)
            gain = delta.where(delta > 0, 0).fillna(0) 
            loss = -delta.where(delta < 0, 0).fillna(0)

            avg_gain = gain.ewm(com=rsi_window - 1, min_periods=rsi_window).mean()
            avg_loss = loss.ewm(com=rsi_window - 1, min_periods=rsi_window).mean()
            
            rs = avg_gain / avg_loss.replace(0, np.nan) 
            df_out['RSI_14'] = 100 - (100 / (1 + rs))
            
            df_out['RSI_14'].fillna(method='ffill', inplace=True)
            df_out['RSI_14'].fillna(method='bfill', inplace=True)
            df_out['RSI_14'].fillna(50, inplace=True) 
        except Exception as e_rsi:
            if log_callback: log_callback(f"add_technical_indicators: Error calculating RSI_14: {e_rsi}", "ERROR")
            df_out['RSI_14'] = 50.0 
    else:
        if log_callback: log_callback(f"add_technical_indicators: Not enough data for RSI_14 (need {rsi_window + 1}, got {len(df_out)}). Setting RSI_14 to 50.", "WARNING")
        df_out['RSI_14'] = 50.0

    macd_fast_period = 12
    macd_slow_period = 26
    macd_signal_period = 9
    
    if len(df_out) >= macd_slow_period: 
        try:
            exp12 = df_out['Close'].ewm(span=macd_fast_period, adjust=False, min_periods=1).mean()
            exp26 = df_out['Close'].ewm(span=macd_slow_period, adjust=False, min_periods=1).mean()
            df_out['MACD'] = exp12 - exp26
            df_out['MACD_Signal'] = df_out['MACD'].ewm(span=macd_signal_period, adjust=False, min_periods=1).mean()
            df_out['MACD_Hist'] = df_out['MACD'] - df_out['MACD_Signal']

            for col_macd in ['MACD', 'MACD_Signal', 'MACD_Hist']:
                df_out[col_macd].fillna(method='ffill', inplace=True)
                df_out[col_macd].fillna(method='bfill', inplace=True)
                df_out[col_macd].fillna(0, inplace=True) 

            if log_callback: log_callback(f"add_technical_indicators: MACD calculated. MACD NaNs after fill: {df_out['MACD'].isnull().sum()}", "DEBUG")
        except Exception as e_macd:
            if log_callback: log_callback(f"add_technical_indicators: Error calculating MACD: {e_macd}", "ERROR")
            df_out['MACD'] = 0.0 
            df_out['MACD_Signal'] = 0.0
            df_out['MACD_Hist'] = 0.0
    else:
        if log_callback: log_callback(f"add_technical_indicators: Not enough data for MACD (need {macd_slow_period}, got {len(df_out)}). Setting MACD components to 0.", "WARNING")
        df_out['MACD'] = 0.0
        df_out['MACD_Signal'] = 0.0
        df_out['MACD_Hist'] = 0.0
        
    if 'Volume' in df_out.columns:
        df_out['Volume'] = pd.to_numeric(df_out['Volume'], errors='coerce').fillna(0)
    else:
        if log_callback: log_callback("add_technical_indicators: 'Volume' column missing. Adding it with zeros.", "WARNING")
        df_out['Volume'] = 0.0 

    if log_callback: log_callback(f"add_technical_indicators: Indicators added. Final check - SMA_50 NaNs: {df_out['SMA_50'].isnull().sum()}, RSI_14 NaNs: {df_out['RSI_14'].isnull().sum()}", "DEBUG")
    return df_out

def create_target_variable(df: pd.DataFrame, periods_ahead: int = 1, log_callback=None) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty or 'Close' not in df.columns:
        if log_callback: log_callback("create_target_variable: Input DataFrame is empty or 'Close' column missing. Cannot create target.", "WARNING")
        return df 

    df_out = df.copy()
    
    try:
        df_out['Close'] = pd.to_numeric(df_out['Close'], errors='raise')
    except ValueError:
        if log_callback: log_callback("create_target_variable: 'Close' column contains non-numeric values. Cannot create target.", "ERROR")
        return df 

    df_out['Future_Close'] = df_out['Close'].shift(-periods_ahead)
    
    df_out['Target'] = np.nan 
    df_out.loc[df_out['Future_Close'].notna() & (df_out['Future_Close'] > df_out['Close']), 'Target'] = 1
    df_out.loc[df_out['Future_Close'].notna() & (df_out['Future_Close'] <= df_out['Close']), 'Target'] = 0
    
    if log_callback: 
        nan_target_count = df_out['Target'].isnull().sum()
        log_callback(f"create_target_variable: Target created. {nan_target_count} row(s) at the end will have NaN Target.", "DEBUG")
    return df_out

def train_and_save_model(df_with_target_and_features: pd.DataFrame, 
                         model_filename: str = "model.joblib", 
                         log_callback=None) -> str | None:
    if not isinstance(df_with_target_and_features, pd.DataFrame) or df_with_target_and_features.empty:
        if log_callback: log_callback("train_and_save_model: DataFrame is empty. Cannot train model.", "ERROR")
        return None

    features_to_use = ['Close', 'Volume', 'SMA_50', 'RSI_14', 'MACD', 'MACD_Signal', 'MACD_Hist']
    
    missing_features = [f for f in features_to_use if f not in df_with_target_and_features.columns]
    if missing_features:
        if log_callback: log_callback(f"train_and_save_model: Missing features for training: {missing_features}. Cannot train model.", "ERROR")
        return None
        
    if 'Target' not in df_with_target_and_features.columns:
        if log_callback: log_callback("train_and_save_model: 'Target' column missing. Cannot train model.", "ERROR")
        return None

    df_for_training = df_with_target_and_features[features_to_use + ['Target']].copy()
    original_rows = len(df_for_training)
    
    df_for_training.dropna(subset=features_to_use + ['Target'], inplace=True)
    dropped_rows = original_rows - len(df_for_training)

    if log_callback: log_callback(f"train_and_save_model: Dropped {dropped_rows} rows with NaNs from features/target. Rows remaining for training: {len(df_for_training)}", "DEBUG")

    min_rows_for_training = 50 
    if len(df_for_training) < min_rows_for_training:
        if log_callback: log_callback(f"train_and_save_model: Not enough data ({len(df_for_training)} rows) after NaN drop (minimum required: {min_rows_for_training}). Aborting training.", "WARNING")
        return None

    X = df_for_training[features_to_use]
    y = df_for_training['Target'].astype(int) 

    if len(y.unique()) < 2:
        if log_callback: log_callback(f"train_and_save_model: Only one class present in target variable 'y' after cleaning (Values: {y.unique()}). Cannot train classifier.", "ERROR")
        return None

    X_train, X_test, y_train, y_test = pd.DataFrame(columns=X.columns), pd.Series(dtype='int'), pd.DataFrame(columns=X.columns), pd.Series(dtype='int')
    try:
        min_samples_per_class_for_stratify = 2 
        y_value_counts = y.value_counts()
        can_stratify = all(count >= min_samples_per_class_for_stratify for count in y_value_counts.values) and len(y_value_counts) >= 2
        
        test_size = 0.2
        
        if can_stratify:
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42, stratify=y)
        else:
            if log_callback: log_callback("train_and_save_model: Cannot stratify target variable. Splitting without stratification.", "WARNING")
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42)

    except ValueError as e_split: 
         if log_callback: log_callback(f"train_and_save_model: Error during train_test_split: {e_split}. Training with all available data for X_train/y_train.", "WARNING")
         X_train, y_train = X.copy(), y.copy() 

    model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
    
    try: 
        if log_callback: log_callback(f"train_and_save_model: Starting model training with {len(X_train)} samples...", "INFO")
        model.fit(X_train, y_train)
        if log_callback: log_callback("train_and_save_model: Model training completed.", "INFO")

        if not X_test.empty and not y_test.empty and len(X_test) > 0:
            try:
                y_pred_test = model.predict(X_test)
                accuracy = accuracy_score(y_test, y_pred_test)
                if log_callback: log_callback(f"train_and_save_model: Model Accuracy on Test Set: {accuracy:.4f}", "INFO")
            except Exception as e_eval:
                 if log_callback: log_callback(f"train_and_save_model: Error during test set evaluation: {e_eval}", "WARNING")
        else:
             if log_callback: log_callback("train_and_save_model: Test set was empty or too small, skipping test set evaluation.", "DEBUG")

        model_dir = os.path.dirname(model_filename)
        if model_dir and not os.path.exists(model_dir): 
            os.makedirs(model_dir, exist_ok=True)
            if log_callback: log_callback(f"train_and_save_model: Created directory for model: {model_dir}", "DEBUG")
        
        joblib.dump(model, model_filename)
        if log_callback: log_callback(f"train_and_save_model: Model '{model_filename}' saved successfully. Features used: {features_to_use}. Training rows: {len(X_train)}.", "INFO")
        return model_filename
    except Exception as e: 
        if log_callback: log_callback(f"train_and_save_model: Error during model training or saving: {e}\n{traceback.format_exc()}", "ERROR")
        return None

def load_model_and_predict(df_features_live: pd.DataFrame, 
                           model_filename: str = "model.joblib", 
                           log_callback=None) -> tuple[np.ndarray | None, np.ndarray | None]:
    if not os.path.exists(model_filename):
        if log_callback: log_callback(f"load_model_and_predict: Model file '{model_filename}' not found.", "ERROR")
        return None, None
    
    if not isinstance(df_features_live, pd.DataFrame) or df_features_live.empty:
        if log_callback: log_callback("load_model_and_predict: Input DataFrame for prediction is empty.", "WARNING")
        return None, None

    features_expected = ['Close', 'Volume', 'SMA_50', 'RSI_14', 'MACD', 'MACD_Signal', 'MACD_Hist']
    
    missing_features = [f for f in features_expected if f not in df_features_live.columns]
    if missing_features:
        if log_callback: log_callback(f"load_model_and_predict: Missing features in live data for prediction: {missing_features}. Cannot predict.", "ERROR")
        return None, None
        
    X_live = df_features_live[features_expected].copy()

    if X_live.isnull().values.any():
        nan_cols = X_live.columns[X_live.isnull().any()].tolist()
        if log_callback: log_callback(f"load_model_and_predict: NaNs found in live data features for prediction in columns: {nan_cols}. Filling with 0 for prediction.", "WARNING")
        X_live.fillna(0, inplace=True) 

    try: 
        model = joblib.load(model_filename)
        if log_callback: log_callback(f"load_model_and_predict: Model '{model_filename}' loaded successfully. Type: {type(model)}", "DEBUG")

        if not (hasattr(model, 'predict') and callable(model.predict) and \
                hasattr(model, 'predict_proba') and callable(model.predict_proba)):
            if log_callback: log_callback(f"load_model_and_predict: Loaded object from '{model_filename}' is not a valid scikit-learn model. Type is {type(model)}.", "ERROR")
            return None, None
            
        predictions = model.predict(X_live)
        probabilities = model.predict_proba(X_live) 
        
        prob_buy = None
        if hasattr(model, 'classes_'):
            try:
                class_1_index_arr = np.where(model.classes_ == 1)[0]
                if len(class_1_index_arr) > 0: 
                    class_1_index = class_1_index_arr[0]
                    prob_buy = probabilities[:, class_1_index]
                else: 
                    if log_callback: log_callback(f"load_model_and_predict: Class '1' (BUY) not found in model.classes_ ({model.classes_}).", "ERROR")
                    if probabilities.shape[1] == 2: prob_buy = probabilities[:, 1] 
                    else: prob_buy = np.full(len(predictions), 0.5)
            except IndexError: 
                if log_callback: log_callback(f"load_model_and_predict: IndexError accessing model.classes_ for class '1'. model.classes_ are {model.classes_}", "ERROR")
                if probabilities.shape[1] == 2: prob_buy = probabilities[:, 1] 
                else: prob_buy = np.full(len(predictions), 0.5)
        else: 
            if log_callback: log_callback("load_model_and_predict: Model has no 'classes_' attribute. Assuming prob of 2nd class for BUY if 2 classes.", "WARNING")
            if probabilities.shape[1] == 2: prob_buy = probabilities[:, 1]
            else: prob_buy = np.full(len(predictions), 0.5)

        if log_callback: log_callback(f"load_model_and_predict: Model '{model_filename}' predicted on {len(X_live)} row(s).", "DEBUG")
        return predictions, prob_buy
    except Exception as e: 
        if log_callback: log_callback(f"load_model_and_predict: Error loading model or predicting: {e}\n{traceback.format_exc()}", "ERROR")
        return None, None

def format_price_display(price_value, digits=5):
    if price_value is None or pd.isna(price_value):
        return "N/A"
    try:
        return f"{float(price_value):.{digits}f}"
    except (ValueError, TypeError):
        return str(price_value)

if __name__ == "__main__":
    def dummy_log(message, level="INFO"): 
        print(f"[{level}] {message}")

    num_rows = 150 
    rng = np.random.default_rng(seed=42) 
    sample_data = {
        'Timestamp': pd.date_range(start='2023-01-01 10:00', periods=num_rows, freq='15T', tz='UTC'),
        'Open': rng.random(num_rows) * 10 + 1900, 
        'Volume': rng.integers(100, 1000, num_rows)
    }
    sample_df = pd.DataFrame(sample_data)
    sample_df['Close'] = sample_df['Open'] + rng.normal(0, 0.5, num_rows).cumsum() 
    sample_df['High'] = sample_df[['Open', 'Close']].max(axis=1) + rng.random(num_rows) * 0.2
    sample_df['Low'] = sample_df[['Open', 'Close']].min(axis=1) - rng.random(num_rows) * 0.2
    
    dummy_log("--- Testing add_technical_indicators ---")
    df_with_inds = add_technical_indicators(sample_df.copy(), log_callback=dummy_log)
    print("\nTail of df_with_inds (last 5 rows):")
    print(df_with_inds.tail())
    print("\nNaN counts in df_with_inds:")
    print(df_with_inds.isnull().sum())

    dummy_log("\n--- Testing create_target_variable ---")
    df_with_target = create_target_variable(df_with_inds.copy(), periods_ahead=5, log_callback=dummy_log)
    print("\nTail of df_with_target (Close, Future_Close, Target - last 10 rows):")
    print(df_with_target[['Close', 'Future_Close', 'Target']].tail(10))
    print("\nTarget value counts:")
    print(df_with_target['Target'].value_counts(dropna=False))


    dummy_log("\n--- Testing train_and_save_model ---")
    test_model_path = "test_model_utils.joblib"
    
    if os.path.exists(test_model_path):
        try: os.remove(test_model_path)
        except Exception as e_del_old: dummy_log(f"Could not delete old test model {test_model_path}: {e_del_old}", "WARNING")

    saved_path = train_and_save_model(df_with_target.copy(), model_filename=test_model_path, log_callback=dummy_log)
    
    if saved_path:
        dummy_log(f"Model saved to: {saved_path}")

        dummy_log("\n--- Testing load_model_and_predict ---")
        predict_data_input = df_with_inds.copy() 
        features_for_pred_check = ['Close', 'Volume', 'SMA_50', 'RSI_14', 'MACD', 'MACD_Signal', 'MACD_Hist']
        predict_data_clean = predict_data_input.dropna(subset=features_for_pred_check)
        
        if not predict_data_clean.empty:
            predict_data_latest_rows = predict_data_clean.tail(3) 
            dummy_log(f"Predicting on latest clean row data ({len(predict_data_latest_rows)} samples):\n{predict_data_latest_rows[features_for_pred_check]}")
            
            predictions, probabilities_buy = load_model_and_predict(
                predict_data_latest_rows, 
                model_filename=test_model_path, 
                log_callback=dummy_log
            )
            if predictions is not None and probabilities_buy is not None:
                dummy_log(f"Predictions: {predictions}")
                dummy_log(f"Probabilities (Buy class): {probabilities_buy}")
                for i in range(len(predictions)):
                    pred_label = "BUY" if predictions[i] == 1 else "SELL"
                    if predictions[i] == 1: 
                        conf = probabilities_buy[i] * 100
                    else: 
                        conf = (1 - probabilities_buy[i]) * 100 
                    dummy_log(f"Sample {i+1}: Prediction = {pred_label}, Confidence = {conf:.2f}% (Raw Buy Prob: {probabilities_buy[i]:.4f})")
            else:
                dummy_log("Prediction failed or returned None.", "ERROR")
        else:
            dummy_log("Not enough clean data for prediction test after dropping NaNs for features.", "WARNING")
        
        if os.path.exists(test_model_path):
            try: os.remove(test_model_path)
            except Exception as e_del: dummy_log(f"Could not delete test model {test_model_path}: {e_del}", "WARNING")
    else:
        dummy_log("Model training failed or was aborted in test.", "ERROR")