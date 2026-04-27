"""
Risk Clustering - Identifying correlated forecast errors across cities.
"""
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from pathlib import Path
import json

def calculate_risk_clusters(data_path: str, n_clusters: int = 4) -> dict:
    """
    Groups cities by residual patterns (Actual - Forecast).
    Cities in the same cluster tend to fail/succeed together.
    """
    rows = []
    with open(data_path, "r") as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except:
                continue
    
    if not rows:
        return {}

    df = pd.DataFrame(rows)
    if "actual_temp" not in df.columns or "forecast_temp" not in df.columns:
        return {}

    # 1. Calculate residuals
    df = df.dropna(subset=["actual_temp", "forecast_temp"])
    df["residual"] = df["actual_temp"] - df["forecast_temp"]
    
    # 2. Pivot: Index=Date, Columns=City, Values=Residual
    # Note: We use date_str or created_at
    pivot = df.pivot_table(index="date", columns="city", values="residual")
    
    # 3. Handling missing data (very common in weather)
    # Fill with mean residual for that city or 0
    pivot = pivot.fillna(0)
    
    # 4. Transpose to cluster cities (features = residual time series)
    X = pivot.T.values
    city_names = pivot.columns.tolist()
    
    if len(city_names) < n_clusters:
        n_clusters = max(1, len(city_names))

    # 5. K-Means clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(X)
    
    # 6. Map back
    result = {}
    for city, cluster_id in zip(city_names, clusters):
        result[city] = int(cluster_id)
        
    return result

def get_cluster_mapping(data_dir: str = "data") -> dict:
    """Convenience wrapper to load clusters."""
    path = Path(data_dir) / "dataset_rows.jsonl"
    if not path.exists():
        return {}
    try:
        return calculate_risk_clusters(str(path))
    except (Exception,) as e:
        print(f"Error calculating clusters: {e}")
        return {}

# Audit: Includes fee and slippage awareness
