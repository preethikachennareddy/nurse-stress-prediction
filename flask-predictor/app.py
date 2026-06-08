from flask import Flask, request, jsonify
import random
import joblib
import numpy as np
import os

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
model = joblib.load(os.path.join(BASE_DIR, "model", "stress_prediction_model_lgbm.joblib"))

@app.route('/model/api/predict', methods=['GET'])
def get_stress_level():
    try:
        x = float(request.args.get('x'))
        y = float(request.args.get('y'))
        z = float(request.args.get('z'))
        eda = float(request.args.get('eda'))
        hr = float(request.args.get('hr'))
        temp = float(request.args.get('temp'))

        features = np.array([x, y, z, eda, hr, temp]).reshape(1, -1)

        predicted_stress = model.predict(features)[0]

        return jsonify({"stress_level_prediction": str(predicted_stress)}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)