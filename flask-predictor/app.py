from flask import Flask, request, jsonify
import joblib
import numpy as np
import os

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR = os.path.join(BASE_DIR, "..", "ml_model")

# Load both models at startup
lgbm_model = joblib.load(os.path.join(MODEL_DIR, "stress_prediction_model_lgbm.joblib"))
gb_model   = joblib.load(os.path.join(MODEL_DIR, "stress_prediction_model.joblib"))

MODELS = {
    "lgbm": lgbm_model,
    "gb":   gb_model,
}

STRESS_LABELS = {
    0: "No stress",
    1: "Moderate stress",
    2: "High stress",
}


@app.route('/model/api/predict', methods=['GET'])
def get_stress_level():
    try:
        x    = float(request.args.get('x'))
        y    = float(request.args.get('y'))
        z    = float(request.args.get('z'))
        eda  = float(request.args.get('eda'))
        hr   = float(request.args.get('hr'))
        temp = float(request.args.get('temp'))
    except (TypeError, ValueError) as e:
        return jsonify({"error": f"Missing or invalid parameter: {e}"}), 400

    # ?model=lgbm (default) or ?model=gb
    model_key = request.args.get('model', 'lgbm').lower().strip()
    if model_key not in MODELS:
        return jsonify({"error": f"Unknown model '{model_key}'. Choose 'lgbm' or 'gb'."}), 400

    model = MODELS[model_key]

    try:
        # Feature order must match training: X, Y, Z, HR, TEMP, EDA
        # (matches Orientation_X, Orientation_Y, Orientation_Z, Heart_Rate, Skin_Temp, EDA)
        features = np.array([x, y, z, hr, temp, eda]).reshape(1, -1)
        predicted = int(model.predict(features)[0])

        return jsonify({
            "stress_level_prediction": str(predicted),
            "stress_label":            STRESS_LABELS.get(predicted, "Unknown"),
            "model":                   model_key,
            "inputs": {
                "x": x, "y": y, "z": z,
                "eda": eda, "hr": hr, "temp": temp,
            },
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        "status": "ok",
        "models_loaded": list(MODELS.keys()),
    }), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
