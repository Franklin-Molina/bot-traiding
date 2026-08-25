import asyncio
import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import select
from infrastructure.database import async_session
from models.trading import MLTrainingData
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import classification_report, confusion_matrix
import json
from loguru import logger

async def extract_data():
    """Extrae los datos cerrados de la base de datos."""
    async with async_session() as session:
        result = await session.execute(
            select(MLTrainingData)
            .where(MLTrainingData.status == "CLOSED")
            .where(MLTrainingData.target_class.isnot(None))
            .order_by(MLTrainingData.entry_time) # CRÍTICO PARA TIMESERIESSPLIT
        )
        rows = result.scalars().all()
        
        data = []
        for r in rows:
            data.append({
                "entry_time": r.entry_time,
                "symbol": r.symbol,
                "trade_type": r.trade_type,
                "market_regime": r.market_regime,
                "tech_score": r.tech_score,
                "spread": r.spread,
                "momentum_15s": r.momentum_15s,
                "local_range_15s": r.local_range_15s,
                # EST-4: Removidas features de IA (siempre 0.0 sin orquestador activo)
                "target_class": r.target_class
            })
            
        return pd.DataFrame(data)

def preprocess_features(df):
    """Convierte features categóricas y normaliza."""
    # Market Regime a One-Hot
    df = pd.get_dummies(df, columns=["market_regime"])
    
    # EST-5 FIX: Asegurar que las columnas de régimen real existan
    for reg in ['market_regime_HOT', 'market_regime_WARM', 'market_regime_DEAD']:
        if reg not in df.columns:
            df[reg] = 0
            
    # Eliminar columnas no predictivas
    X = df.drop(columns=["entry_time", "symbol", "trade_type", "target_class"])
    y = df["target_class"]
    
    return X, y

def train():
    logger.info("🚀 Iniciando Pipeline Cuantitativo de XGBoost...")
    
    df = asyncio.run(extract_data())
    if len(df) < 40:
        logger.error(f"❌ Insuficientes datos ({len(df)}). Se recomiendan mínimo 40 muestras para entrenar.")
        return
        
    logger.info(f"✅ Extraídas {len(df)} muestras (REAL y SHADOW).")
    
    # Distribución de clases
    dist = df['target_class'].value_counts().to_dict()
    logger.info(f"Distribución de clases: {dist}")
    
    X, y = preprocess_features(df)
    
    # TimeSeriesSplit para evitar Data Leakage
    tscv = TimeSeriesSplit(n_splits=5)
    
    best_model = None
    best_score = 0
    
    for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
        
        # Ensure all 4 classes are present in y_train to avoid XGBoost ValueError
        missing_classes = list(set([0, 1, 2, 3]) - set(y_train))
        if missing_classes:
            dummy_X = X_train.iloc[:len(missing_classes)].copy()
            dummy_y = pd.Series(missing_classes)
            X_train = pd.concat([X_train, dummy_X], ignore_index=True)
            y_train = pd.concat([y_train, dummy_y], ignore_index=True)

        # Calcular sample_weights para desbalanceo
        class_weights = {
            0: len(y_train) / (4 * sum(y_train == 0) + 1),
            1: len(y_train) / (4 * sum(y_train == 1) + 1),
            2: len(y_train) / (4 * sum(y_train == 2) + 1),
            3: len(y_train) / (4 * sum(y_train == 3) + 1)
        }
        weights = y_train.map(class_weights).copy()
        
        # Set dummy weights to near zero so they don't affect training
        if missing_classes:
            weights.iloc[-len(missing_classes):] = 0.00001
        
        model = xgb.XGBClassifier(
            objective="multi:softprob",
            num_class=4,
            eval_metric="mlogloss",
            max_depth=4,
            learning_rate=0.05,
            n_estimators=100,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42
        )
        
        model.fit(X_train, y_train, sample_weight=weights)
        
        # Evaluar
        preds = model.predict(X_val)
        acc = (preds == y_val).mean()
        logger.info(f"Fold {fold+1} - Accuracy: {acc:.2f}")
        
        if acc > best_score:
            best_score = acc
            best_model = model
            
    logger.info(f"🏆 Entrenamiento completado. Mejor Fold Accuracy: {best_score:.2f}")
    
    # Evaluar en el último fold para ver métricas completas
    logger.info("\n" + classification_report(y_val, preds, zero_division=0))
    
    # Guardar modelo
    best_model.save_model("models/xgboost_model.json")
    
    # Guardar Feature Names esperadas
    with open("models/feature_names.json", "w") as f:
        json.dump(list(X.columns), f)
        
    logger.success("✅ Modelo guardado en models/xgboost_model.json")

if __name__ == "__main__":
    train()
