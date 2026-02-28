"""
AutoML Engine for Sales Forecasting
Automatically detects data structure and applies best ML models
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Tuple, List
import warnings
warnings.filterwarnings('ignore')

from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, ExtraTreesRegressor
from sklearn.linear_model import Ridge, Lasso, ElasticNet
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import TimeSeriesSplit, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
import xgboost as xgb
import lightgbm as lgb
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.arima.model import ARIMA
import joblib
import json
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataProfiler:
    """Automatically profiles uploaded data and detects columns"""

    def profile(self, df: pd.DataFrame) -> Dict[str, Any]:
        profile = {
            "shape": df.shape,
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "missing": df.isnull().sum().to_dict(),
            "date_columns": [],
            "numeric_columns": [],
            "categorical_columns": [],
            "target_column": None,
            "date_column": None,
        }

        for col in df.columns:
            col_lower = col.lower().strip()

            # Skip columns where cells contain dicts/lists (scraped JSON)
            try:
                sample = df[col].dropna()
                if len(sample) > 0 and isinstance(sample.iloc[0], (dict, list, set, tuple)):
                    profile["categorical_columns"].append(col)
                    continue
            except Exception:
                profile["categorical_columns"].append(col)
                continue

            # Detect date columns by name
            if any(kw in col_lower for kw in ['date', 'time', 'month', 'year', 'week', 'day', 'period']):
                try:
                    pd.to_datetime(df[col], errors='coerce')
                    profile["date_columns"].append(col)
                except Exception:
                    pass

            # Try converting object columns to datetime
            if df[col].dtype == object:
                try:
                    parsed = pd.to_datetime(df[col], errors='coerce')
                    if parsed.notna().sum() > len(df) * 0.5:
                        if col not in profile["date_columns"]:
                            profile["date_columns"].append(col)
                    else:
                        profile["categorical_columns"].append(col)
                except Exception:
                    profile["categorical_columns"].append(col)
            elif pd.api.types.is_numeric_dtype(df[col]):
                profile["numeric_columns"].append(col)

        # Detect target column (sales, revenue, amount, etc.)
        target_keywords = ['sales', 'revenue', 'amount', 'total', 'price', 'income',
                           'profit', 'quantity', 'units', 'value', 'target', 'forecast']
        for col in profile["numeric_columns"]:
            col_lower = col.lower()
            if any(kw in col_lower for kw in target_keywords):
                profile["target_column"] = col
                break

        if not profile["target_column"] and profile["numeric_columns"]:
            profile["target_column"] = profile["numeric_columns"][-1]

        if profile["date_columns"]:
            profile["date_column"] = profile["date_columns"][0]

        return profile


class FeatureEngineer:
    """Creates time-series features from datetime columns"""

    def __init__(self):
        self.label_encoders = {}
        self.scaler = StandardScaler()
        self.feature_columns = []

    def create_time_features(self, df: pd.DataFrame, date_col: str) -> pd.DataFrame:
        df = df.copy()
        try:
            df[date_col] = pd.to_datetime(df[date_col])
            df['year'] = df[date_col].dt.year
            df['month'] = df[date_col].dt.month
            df['quarter'] = df[date_col].dt.quarter
            df['day_of_week'] = df[date_col].dt.dayofweek
            df['day_of_year'] = df[date_col].dt.dayofyear
            df['week_of_year'] = df[date_col].dt.isocalendar().week.astype(int)
            df['is_month_start'] = df[date_col].dt.is_month_start.astype(int)
            df['is_month_end'] = df[date_col].dt.is_month_end.astype(int)
            df['is_quarter_end'] = df[date_col].dt.is_quarter_end.astype(int)
            # Sine/cosine encoding for cyclical features
            df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
            df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
        except Exception as e:
            logger.warning(f"Could not create time features: {e}")
        return df

    def create_lag_features(self, df: pd.DataFrame, target_col: str,
                             lags: List[int] = [1, 2, 3]) -> pd.DataFrame:
        df = df.copy()
        for lag in lags:
            if len(df) > lag:
                df[f'{target_col}_lag_{lag}'] = df[target_col].shift(lag)
        return df

    def create_rolling_features(self, df: pd.DataFrame, target_col: str,
                                  windows: List[int] = [3, 6]) -> pd.DataFrame:
        df = df.copy()
        for w in windows:
            if len(df) > w:
                df[f'{target_col}_rolling_mean_{w}'] = df[target_col].shift(1).rolling(w).mean()
                df[f'{target_col}_rolling_std_{w}'] = df[target_col].shift(1).rolling(w).std()
                df[f'{target_col}_rolling_min_{w}'] = df[target_col].shift(1).rolling(w).min()
                df[f'{target_col}_rolling_max_{w}'] = df[target_col].shift(1).rolling(w).max()
        return df

    def encode_categoricals(self, df: pd.DataFrame, cat_cols: List[str]) -> pd.DataFrame:
        df = df.copy()
        for col in cat_cols:
            if col in df.columns:
                try:
                    # Flatten any dict/list/complex values to strings first
                    df[col] = df[col].apply(
                        lambda x: str(x) if isinstance(x, (dict, list, set, tuple)) else x
                    )
                    le = LabelEncoder()
                    df[col] = le.fit_transform(df[col].fillna('__missing__').astype(str))
                    self.label_encoders[col] = le
                except Exception as e:
                    # If encoding fails, drop the column rather than crash
                    df = df.drop(columns=[col])
        return df


class ModelSelector:
    """Trains multiple models and selects the best one"""

    MODELS = {
        'xgboost': xgb.XGBRegressor(
            n_estimators=100, learning_rate=0.1, max_depth=4,
            subsample=0.8, colsample_bytree=0.8, random_state=42,
            verbosity=0, tree_method='hist', n_jobs=-1
        ),
        'lightgbm': lgb.LGBMRegressor(
            n_estimators=100, learning_rate=0.1, max_depth=4,
            num_leaves=15, random_state=42, verbose=-1, n_jobs=-1
        ),
        'random_forest': RandomForestRegressor(
            n_estimators=100, max_depth=8, random_state=42, n_jobs=-1
        ),
        'gradient_boosting': GradientBoostingRegressor(
            n_estimators=100, learning_rate=0.1, max_depth=4,
            random_state=42, subsample=0.8
        ),
        'extra_trees': ExtraTreesRegressor(
            n_estimators=100, max_depth=8, random_state=42, n_jobs=-1
        ),
        'ridge': Ridge(alpha=1.0),
    }

    def __init__(self):
        self.results = {}
        self.best_model = None
        self.best_model_name = None
        self.best_score = float('inf')

    def train_and_evaluate(self, X_train: np.ndarray, y_train: np.ndarray,
                            X_test: np.ndarray, y_test: np.ndarray) -> Dict:
        results = {}
        for name, model in self.MODELS.items():
            try:
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)
                mae = mean_absolute_error(y_test, y_pred)
                rmse = np.sqrt(mean_squared_error(y_test, y_pred))
                r2 = r2_score(y_test, y_pred)
                mape = np.mean(np.abs((y_test - y_pred) / (y_test + 1e-10))) * 100

                results[name] = {
                    'model': model,
                    'mae': float(mae),
                    'rmse': float(rmse),
                    'r2': float(r2),
                    'mape': float(mape),
                    'accuracy': float(max(0, 100 - mape)),
                    'predictions': y_pred.tolist()
                }
                logger.info(f"{name}: MAE={mae:.2f}, RMSE={rmse:.2f}, R2={r2:.4f}, MAPE={mape:.2f}%")
            except Exception as e:
                logger.warning(f"Model {name} failed: {e}")

        self.results = results
        if results:
            # Select best by RMSE
            self.best_model_name = min(results, key=lambda k: results[k]['rmse'])
            self.best_model = results[self.best_model_name]['model']
        return results


class AutoMLForecaster:
    """Main AutoML pipeline for sales forecasting"""

    def __init__(self):
        self.profiler = DataProfiler()
        self.feature_engineer = FeatureEngineer()
        self.model_selector = ModelSelector()
        self.profile = None
        self.df_processed = None
        self.forecast_df = None
        self.metrics = {}
        self.model_results = {}
        self.feature_importance = {}

    def fit(self, df: pd.DataFrame) -> Dict[str, Any]:
        logger.info("Starting AutoML pipeline...")

        # Step 1: Profile data
        self.profile = self.profiler.profile(df)
        logger.info(f"Profile: {self.profile['shape']}, Target: {self.profile['target_column']}")

        target_col = self.profile['target_column']
        date_col = self.profile['date_column']

        # Fallback: if no target detected, try to find any numeric column
        if not target_col:
            numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
            if not numeric_cols:
                for c in df.columns:
                    converted = pd.to_numeric(df[c], errors='coerce')
                    if converted.notna().sum() > len(df) * 0.5:
                        numeric_cols.append(c)
            if numeric_cols:
                target_col = numeric_cols[-1]
                self.profile['target_column'] = target_col
                logger.info(f"No target column detected, using fallback: {target_col}")
            else:
                raise ValueError(
                    "Could not detect a numeric target column. "
                    "Please ensure your file has a numeric column named something like: "
                    "sales, revenue, amount, price, quantity, total, value."
                )

        cat_cols = [c for c in self.profile['categorical_columns']
                    if c != target_col and c != date_col]

        # Step 2: Clean data
        df_clean = df.copy()

        # ── Sanitize ALL columns: flatten any dict/list/complex objects to strings ──
        for col in df_clean.columns:
            try:
                # Check if any cell contains a non-scalar (dict, list, set, etc.)
                sample = df_clean[col].dropna()
                if len(sample) > 0 and isinstance(sample.iloc[0], (dict, list, set, tuple)):
                    df_clean[col] = df_clean[col].apply(
                        lambda x: str(x) if isinstance(x, (dict, list, set, tuple)) else x
                    )
            except Exception:
                # If anything fails, convert whole column to string
                try:
                    df_clean[col] = df_clean[col].astype(str)
                except Exception:
                    df_clean = df_clean.drop(columns=[col])

        # Convert target to numeric
        df_clean[target_col] = pd.to_numeric(df_clean[target_col], errors='coerce')
        df_clean = df_clean.dropna(subset=[target_col])
        df_clean[target_col] = pd.to_numeric(df_clean[target_col], errors='coerce')
        df_clean = df_clean.dropna(subset=[target_col])

        # Cap at 5000 rows for performance (sample if larger)
        if len(df_clean) > 5000:
            df_clean = df_clean.sample(5000, random_state=42).reset_index(drop=True)
            logger.info(f"Dataset sampled to 5000 rows for performance")

        # Step 3: Feature engineering
        if date_col:
            df_clean = self.feature_engineer.create_time_features(df_clean, date_col)
            df_clean = df_clean.sort_values(date_col)

        df_clean = self.feature_engineer.create_lag_features(df_clean, target_col)
        df_clean = self.feature_engineer.create_rolling_features(df_clean, target_col)

        if cat_cols:
            df_clean = self.feature_engineer.encode_categoricals(df_clean, cat_cols)

        # Step 4: Prepare features
        exclude_cols = [target_col]
        if date_col:
            exclude_cols.append(date_col)

        feature_cols = [c for c in df_clean.columns if c not in exclude_cols
                        and pd.api.types.is_numeric_dtype(df_clean[c])]

        df_model = df_clean[feature_cols + [target_col]].dropna()

        X = df_model[feature_cols].values
        y = df_model[target_col].values

        # Step 5: Train/test split (80/20 time-aware)
        split_idx = int(len(X) * 0.8)
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]

        # Ensure enough data
        if len(X_train) < 5:
            # Use all data for training if small dataset
            X_train, y_train = X, y
            X_test, y_test = X[-max(1, len(X)//5):], y[-max(1, len(y)//5):]

        # Step 6: Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)

        # Step 7: Train models
        self.model_results = self.model_selector.train_and_evaluate(
            X_train_scaled, y_train, X_test_scaled, y_test
        )

        best_name = self.model_selector.best_model_name
        best_result = self.model_results.get(best_name, {})

        self.metrics = {
            'best_model': best_name,
            'mae': best_result.get('mae', 0),
            'rmse': best_result.get('rmse', 0),
            'r2': best_result.get('r2', 0),
            'mape': best_result.get('mape', 0),
            'accuracy': best_result.get('accuracy', 0),
            'train_size': len(X_train),
            'test_size': len(X_test),
            'total_records': len(df_clean),
            'features_used': len(feature_cols),
        }

        # Step 8: Feature importance
        best_model = self.model_selector.best_model
        if hasattr(best_model, 'feature_importances_'):
            importances = best_model.feature_importances_
            self.feature_importance = dict(zip(feature_cols, importances.tolist()))

        # Step 9: Store processed df with predictions
        df_model = df_model.copy()
        best_model_fitted = best_result.get('model', None)
        if best_model_fitted is not None:
            X_all_scaled = scaler.transform(X)
            df_model['predicted'] = best_model_fitted.predict(X_all_scaled)
        else:
            df_model['predicted'] = y

        df_model['actual'] = y
        if date_col:
            df_model[date_col] = df_clean.loc[df_model.index, date_col].values

        self.df_processed = df_model
        self.scaler = scaler
        self.feature_cols = feature_cols
        self.target_col = target_col
        self.date_col = date_col

        # Step 10: Generate future forecast
        self.forecast_df = self._generate_future_forecast(df_clean, feature_cols, target_col, date_col)

        logger.info(f"AutoML complete. Best model: {best_name}, Accuracy: {self.metrics['accuracy']:.1f}%")
        return self.get_results()

    def _generate_future_forecast(self, df: pd.DataFrame, feature_cols: List[str],
                                   target_col: str, date_col: Optional[str],
                                   periods: int = 12) -> pd.DataFrame:
        try:
            best_model = self.model_selector.best_model
            if best_model is None:
                return pd.DataFrame()

            last_row = df[feature_cols].dropna().iloc[-1:].copy()
            future_rows = []
            last_date = None

            if date_col:
                try:
                    last_date = pd.to_datetime(df[date_col]).iloc[-1]
                except Exception:
                    pass

            for i in range(1, periods + 1):
                row = last_row.copy()

                # Update time features if available
                if last_date is not None:
                    try:
                        future_date = last_date + pd.DateOffset(months=i)
                        if 'year' in row.columns:
                            row['year'] = future_date.year
                        if 'month' in row.columns:
                            row['month'] = future_date.month
                        if 'quarter' in row.columns:
                            row['quarter'] = future_date.quarter
                        if 'month_sin' in row.columns:
                            row['month_sin'] = np.sin(2 * np.pi * future_date.month / 12)
                        if 'month_cos' in row.columns:
                            row['month_cos'] = np.cos(2 * np.pi * future_date.month / 12)
                    except Exception:
                        pass

                row_scaled = self.scaler.transform(row.values.reshape(1, -1))
                pred = best_model.predict(row_scaled)[0]
                pred = max(0, pred)  # No negative sales

                forecast_entry = {'predicted': float(pred)}
                if last_date is not None:
                    forecast_entry['date'] = (last_date + pd.DateOffset(months=i)).strftime('%Y-%m-%d')
                else:
                    forecast_entry['date'] = f"Period +{i}"

                future_rows.append(forecast_entry)
                last_row = row.copy()

            return pd.DataFrame(future_rows)
        except Exception as e:
            logger.warning(f"Future forecast failed: {e}")
            return pd.DataFrame()

    def get_results(self) -> Dict[str, Any]:
        results = {
            'metrics': self.metrics,
            'model_comparison': {
                name: {k: v for k, v in vals.items() if k != 'model' and k != 'predictions'}
                for name, vals in self.model_results.items()
            },
            'feature_importance': self.feature_importance,
            'profile': {k: v for k, v in self.profile.items()},
        }

        if self.df_processed is not None and not self.df_processed.empty:
            date_col = self.date_col
            target_col = self.target_col

            history_data = []
            for _, row in self.df_processed.iterrows():
                entry = {
                    'actual': float(row.get('actual', 0)),
                    'predicted': float(row.get('predicted', 0)),
                }
                if date_col and date_col in row.index:
                    try:
                        entry['date'] = pd.to_datetime(row[date_col]).strftime('%Y-%m-%d')
                    except Exception:
                        entry['date'] = str(row[date_col])
                history_data.append(entry)
            results['history'] = history_data

        if self.forecast_df is not None and not self.forecast_df.empty:
            results['forecast'] = self.forecast_df.to_dict(orient='records')

        # Summary statistics
        if self.df_processed is not None and 'actual' in self.df_processed.columns:
            y_actual = self.df_processed['actual'].values
            results['summary'] = {
                'total_sales': float(np.sum(y_actual)),
                'avg_sales': float(np.mean(y_actual)),
                'max_sales': float(np.max(y_actual)),
                'min_sales': float(np.min(y_actual)),
                'std_sales': float(np.std(y_actual)),
                'growth_rate': float(
                    ((y_actual[-1] - y_actual[0]) / (y_actual[0] + 1e-10)) * 100
                ) if len(y_actual) > 1 else 0.0,
            }

        if self.forecast_df is not None and not self.forecast_df.empty and 'predicted' in self.forecast_df.columns:
            forecast_vals = self.forecast_df['predicted'].values
            results['forecast_summary'] = {
                'total_forecast': float(np.sum(forecast_vals)),
                'avg_forecast': float(np.mean(forecast_vals)),
                'max_forecast': float(np.max(forecast_vals)),
                'min_forecast': float(np.min(forecast_vals)),
                'periods_ahead': len(forecast_vals),
            }

        return results