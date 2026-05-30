import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

def calcular_prediccion(data):
    if not data:
        return {"status": "success", "predicciones_globales": [], "distrito_riesgo": "Sin datos", "valor_riesgo": 0}

    df = pd.DataFrame([
        {"distrito": d["_id"]["distrito"], "anio": d["_id"]["anio"], "mes": d["_id"]["mes"], "total": d["total"]}
        for d in data
    ])
    
    df['indice_tiempo'] = df['anio'] * 12 + df['mes']
    
    # General Prediction
    df_global = df.groupby('indice_tiempo')['total'].sum().reset_index().sort_values('indice_tiempo')
    if len(df_global) < 2:
        return {"status": "success", "predicciones_globales": [], "distrito_riesgo": "Insuficientes datos", "valor_riesgo": 0}
        
    X_global = df_global[['indice_tiempo']].values
    y_global = df_global['total'].values
    
    model_global = LinearRegression()
    model_global.fit(X_global, y_global)
    
    last_index = df_global['indice_tiempo'].max()
    predicciones_globales = []
    for i in range(1, 4):
        futuro_idx = last_index + i
        pred_val = max(0, int(model_global.predict([[futuro_idx]])[0]))
        anio_f = futuro_idx // 12
        mes_f = futuro_idx % 12
        if mes_f == 0:
            mes_f = 12
            anio_f -= 1
        predicciones_globales.append({"anio": int(anio_f), "mes": int(mes_f), "prediccion": pred_val})

    # Predict District Risk
    distritos_pred = {}
    for dist in df['distrito'].unique():
        df_dist = df[df['distrito'] == dist].sort_values('indice_tiempo')
        if len(df_dist) < 3:
            continue
        X_dist = df_dist[['indice_tiempo']].values
        y_dist = df_dist['total'].values
        model_dist = LinearRegression().fit(X_dist, y_dist)
        pred_dist = model_dist.predict([[last_index + 1]])[0]
        distritos_pred[dist] = max(0, int(pred_dist))
        
    distrito_peligro = max(distritos_pred, key=distritos_pred.get) if distritos_pred else "Desconocido"

    return {
        "status": "success",
        "predicciones_globales": predicciones_globales,
        "distrito_riesgo": distrito_peligro,
        "valor_riesgo": distritos_pred.get(distrito_peligro, 0)
    }
