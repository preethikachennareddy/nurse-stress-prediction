import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score, classification_report
import joblib
import os
import kagglehub
import sys
from io import StringIO

# Configuration
KAGGLE_DATASET = "priyankraval/nurse-stress-prediction-wearable-sensors"
LOCAL_DATA_FILE = 'nurse_stress_data.csv'
KAGGLE_DATA_FILENAME = 'merged_data.csv'

# Download Kaggle Dataset
def download_kaggle_dataset():
    try:
        download_path = kagglehub.dataset_download(KAGGLE_DATASET)
        print(f"Dataset downloaded to: {download_path}")
        return download_path
    except Exception as e:
        print("Kaggle download failed:", e)
        return None

# Load Data
df = None
data_file_path = LOCAL_DATA_FILE
downloaded_path = None

if not os.path.exists(LOCAL_DATA_FILE):
    downloaded_path = download_kaggle_dataset()
    if downloaded_path:
        potential = os.path.join(downloaded_path, KAGGLE_DATA_FILENAME)
        if os.path.exists(potential):
            data_file_path = potential

try:
    if os.path.exists(data_file_path):
        df = pd.read_csv(data_file_path, low_memory=False)
    else:
        simulated = """@ id,Timestamp,# X,# Y,# Z,# HR,# TEMP,# EDA,# label
15,1704067200,-13.0,-61.0,5.0,99.43,31.17,6.769995,2
15,1704067200,-20.0,-69.0,-3.0,99.43,31.17,6.769995,2
16,1704067200,-31.0,-78.0,-15.0,85.20,30.50,4.100000,1
17,1704067200,-47.0,-65.0,-38.0,68.90,32.00,1.200000,0
15,1704067200,-67.0,-57.0,-53.0,99.43,31.17,6.769995,2"""
        df = pd.read_csv(StringIO(simulated))
except:
    sys.exit("Data loading failed.")

# Normalizing headers
COLUMN_MAP = {
    'id':'User_ID','timestamp':'Timestamp','x':'Orientation_X','y':'Orientation_Y',
    'z':'Orientation_Z','hr':'Heart_Rate','temp':'Skin_Temp','eda':'EDA','label':'Stress_Label',
    '@ id':'User_ID','# x':'Orientation_X','# y':'Orientation_Y','# z':'Orientation_Z',
    '# hr':'Heart_Rate','# temp':'Skin_Temp','# eda':'EDA','# label':'Stress_Label'
}

def normalize(c): return str(c).strip().lower().replace(" ","")
df.columns = [normalize(c) for c in df.columns]

rename_dict = {raw:clean for raw,clean in COLUMN_MAP.items() if raw in df.columns}
df.rename(columns=rename_dict, inplace=True)

# Feature Engineering
features = ['Orientation_X','Orientation_Y','Orientation_Z','Heart_Rate','Skin_Temp','EDA']
target = 'Stress_Label'

df = df.dropna(subset=features+[target])
X = df[features]
y = df[target]


# --- Feature Correlation Analysis ---
import matplotlib.pyplot as plt
import seaborn as sns

# Compute correlation matrix
corr = df[features + [target]].corr()

print("\nCorrelation Matrix:")
print(corr)

# Plot heatmap
plt.figure(figsize=(10, 6))
sns.heatmap(corr, annot=True, cmap='coolwarm', fmt=".2f")
plt.title("Feature Correlation Heatmap")
plt.show()


# Train Test Split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Train Model
model = GradientBoostingClassifier(
    n_estimators=50, max_depth=4, learning_rate=0.1, random_state=42
)
model.fit(X_train, y_train)

# Evaluation
y_pred = model.predict(X_test)
print("Accuracy:", accuracy_score(y_test, y_pred))
print("Classification Report:")
print(classification_report(y_test, y_pred))

# Save Model
joblib.dump(model, "stress_prediction_model.joblib")
"Model saved successfully!"

#HIST

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.experimental import enable_hist_gradient_boosting
from sklearn.ensemble import HistGradientBoostingClassifier

model1 = HistGradientBoostingClassifier(
    max_depth=4,
    learning_rate=0.1,
    class_weight="balanced",   
    random_state=42
)

from tqdm import tqdm
import time
# Progress bar + timing
print("Training model...")

pbar = tqdm(total=1, desc="HistGradientBoosting Training", bar_format="{l_bar}{bar} {remaining}")

start = time.time()
model1.fit(X_train, y_train)
end = time.time()

# Mark as complete
pbar.update(1)
pbar.close()

print(f"\nTraining complete. Time taken: {end - start:.2f} seconds")

y_pred = model1.predict(X_test)

print("Accuracy:", accuracy_score(y_test, y_pred))
print("\nClassification Report:")
print(classification_report(y_test, y_pred))

from sklearn.metrics import ConfusionMatrixDisplay
import matplotlib.pyplot as plt

ConfusionMatrixDisplay.from_predictions(y_test, y_pred)
plt.title("Confusion Matrix")
plt.show()

from sklearn.metrics import classification_report
print(classification_report(y_test, y_pred))

#LightGBM

!pip install lightgbm

from lightgbm import LGBMClassifier

model2 = LGBMClassifier(
    n_estimators=300,
    learning_rate=0.05,
    max_depth=-1,
    num_leaves=31,
    class_weight='balanced',
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42
)

from lightgbm import LGBMClassifier
from lightgbm import log_evaluation   # callback for progress

print("Training LightGBM...")

model2.fit(
    X_train,
    y_train,
    eval_set=[(X_test, y_test)],
    eval_metric='multi_logloss',
    callbacks=[log_evaluation(period=50)]   # print every 50 iterations
)

from sklearn.metrics import classification_report, accuracy_score, ConfusionMatrixDisplay
import matplotlib.pyplot as plt

y_pred = model2.predict(X_test)

print("Accuracy:", accuracy_score(y_test, y_pred))
print("\nClassification Report:")
print(classification_report(y_test, y_pred))

ConfusionMatrixDisplay.from_predictions(y_test, y_pred)
plt.show()

import joblib

joblib.dump(model2, "stress_prediction_model_lgbm.joblib")
print("Saved LightGBM model as stress_prediction_model_lgbm.joblib")
