import json
import os
import pandas as pd
import xgboost as xgb
from loguru import logger

class HybridInferenceEngine:
    def __init__(self, model_path="models/xgboost_model.json", features_path="models/feature_names.json"):
        self.model_path = model_path
        self.features_path = features_path
        self.model = None
        self.feature_names = []
        self.is_loaded = False
        
        self.load_model()

    def load_model(self):
        """Carga el modelo XGBoost y los nombres de las features."""
        if not os.path.exists(self.model_path) or not os.path.exists(self.features_path):
            logger.warning("Modelo XGBoost no encontrado. Operando en modo degrado (Sin IA Matemática).")
            return

        try:
            # Cargar Feature Names
            with open(self.features_path, "r") as f:
                self.feature_names = json.load(f)
            
            # Cargar Modelo
            self.model = xgb.XGBClassifier()
            self.model.load_model(self.model_path)
            
            self.is_loaded = True
            logger.success("🧠 Cerebro Cuantitativo (XGBoost) cargado correctamente.")
        except Exception as e:
            logger.error(f"Error cargando XGBoost: {e}")
            self.is_loaded = False

    def predict_trade(self, ml_features: dict) -> tuple[bool, float]:
        """
        Evalúa un trade potencial.
        Retorna: (aprobado_bool, prob_exito)
        """
        if not self.is_loaded:
            return True, 0.5  # Si no hay modelo, permitimos el trade (Fallback)

        try:
            # 1. Preparar el diccionario de datos
            data = {
                "tech_score": ml_features.get("tech_score", 50),
                "spread": ml_features.get("spread", 0.001),
                "momentum_15s": ml_features.get("momentum_15s", 0.0),
                "local_range_15s": ml_features.get("local_range_15s", 0.0)
            }

            # Extraer IA Raw
            ai_raw = ml_features.get("ai_raw", {})
            data["ai_risk"] = ai_raw.get("risk", 0.0)
            data["ai_manipulation"] = ai_raw.get("manipulation", 0.0)
            data["ai_news"] = ai_raw.get("news_strength", 0.0)
            data["ai_momentum"] = ai_raw.get("momentum", 0.0)
            data["ai_confidence"] = ai_raw.get("confidence", 0.0)

            # Market Regime (One-Hot Encoding esperado por el modelo)
            regime = ml_features.get("market_regime", "NORMAL")
            for r in ['BULL', 'BEAR', 'CRAB', 'DEAD']:
                data[f"market_regime_{r}"] = 1 if regime == r else 0

            # 2. Construir DataFrame con el orden EXACTO de las features de entrenamiento
            df_features = pd.DataFrame([data])
            
            # Rellenar columnas faltantes con 0 si el modelo espera algo que no pasamos
            for col in self.feature_names:
                if col not in df_features.columns:
                    df_features[col] = 0
            
            # Asegurar el orden
            X = df_features[self.feature_names]

            # 3. Predicción de Probabilidades
            probs = self.model.predict_proba(X)[0]
            
            # XGBoost devuelve array de probabilidades [Prob_0, Prob_1, Prob_2, Prob_3]
            prob_0 = probs[0] # Stop Loss
            prob_1 = probs[1] # Ruido / Break-even
            prob_2 = probs[2] # Buen Trade
            prob_3 = probs[3] # Excepcional

            prob_exito = prob_2 + prob_3
            
            logger.debug(f"XGBoost Probs: SL={prob_0:.1%} | Noise={prob_1:.1%} | Good={prob_2:.1%} | Excel={prob_3:.1%}")

            # 4. Umbral de Aprobación
            # Queremos que la suma de probabilidades buenas sea mayor al 40% (Threshold estricto)
            is_approved = prob_exito >= 0.40

            return is_approved, prob_exito

        except Exception as e:
            logger.error(f"Error en inferencia XGBoost: {e}")
            return True, 0.5 # Fail-open
