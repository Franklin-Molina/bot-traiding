import os
import sys
import json
import joblib
import pandas as pd
import tempfile
import shutil
from loguru import logger
import xgboost as xgb
from sqlalchemy import create_engine
from sklearn.metrics import accuracy_score, classification_report, f1_score

# Asegurar que las importaciones de la raíz funcionen
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.config import settings

def load_data_from_db():
    try:
        engine = create_engine(settings.DB_DSN)
        query = "SELECT * FROM ml_training_data WHERE status = 'CLOSED' AND profit_pct IS NOT NULL ORDER BY exit_time ASC"
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        logger.error(f"Error cargando datos de la DB: {e}")
        return pd.DataFrame()

def train_xgboost():
    logger.info("Iniciando pipeline de reentrenamiento de XGBoost...")
    df = load_data_from_db()
    
    if df.empty or len(df) < 50:
        logger.warning(f"Insuficientes datos para entrenar. Encontrados: {len(df)}. Mínimo: 50.")
        return

    logger.info(f"Datos cargados: {len(df)} registros.")
    
    features = ['tech_score', 'spread', 'momentum_15s', 'local_range_15s']
    df = df.dropna(subset=features + ['target_class'])
    
    # Time Series Split 80/20
    split_idx = int(len(df) * 0.8)
    train_df = df.iloc[:split_idx]
    test_df = df.iloc[split_idx:]
    
    X_train = train_df[features]
    y_train = train_df['target_class'].astype(int)
    
    X_test = test_df[features]
    y_test = test_df['target_class'].astype(int)
    
    logger.info("Entrenando modelo XGBoost...")
    model = xgb.XGBClassifier(
        n_estimators=100,
        learning_rate=0.05,
        max_depth=3,
        objective='multi:softprob',
        num_class=4,
        eval_metric='mlogloss',
        random_state=42
    )
    
    model.fit(X_train, y_train)
    
    preds = model.predict(X_test)
    acc = accuracy_score(y_test, preds)
    f1 = f1_score(y_test, preds, average='weighted')
    
    logger.info(f"Validation Accuracy: {acc:.2f}")
    logger.info(f"Validation F1-Score: {f1:.2f}")
    logger.info(f"\n{classification_report(y_test, preds)}")
    
    if acc > 0.55 and f1 > 0.50:
        os.makedirs("models/saved", exist_ok=True)
        model_path = "models/saved/xgboost_model.bin"
        
        # Guardado Atómico para no corromper lecturas en caliente
        with tempfile.NamedTemporaryFile(delete=False, suffix='.bin') as tmp:
            model.save_model(tmp.name)
            tmp_path = tmp.name
            
        shutil.move(tmp_path, model_path)
        
        meta = {
            "features": features,
            "accuracy": float(acc),
            "f1_score": float(f1),
            "samples": len(df),
            "timestamp": pd.Timestamp.utcnow().isoformat()
        }
        with open("models/saved/xgboost_meta.json", "w") as f:
            json.dump(meta, f)
            
        logger.success(f"Modelo actualizado exitosamente y guardado en {model_path}.")
    else:
        logger.warning(f"Métricas insuficientes (Acc: {acc:.2f}, F1: {f1:.2f}). No se reemplazará el modelo.")

if __name__ == "__main__":
    try:
        import xgboost
        train_xgboost()
    except ImportError:
        logger.error("La librería xgboost o pandas no está instalada. Instálala usando: pip install xgboost pandas scikit-learn")
