# Description du projet

Ce projet implémente un pipeline complet de détection de fraude sur Databricks, incluant :

* ingestion Batch (dataset Kaggle)
* ingestion Streaming simulée  (générateur Kaggle-like)
* pipeline Bronze → Silver → Gold
* entraînement ML (SparkML + MLflow + UC Volumes)
* inférence ML en streaming
* monitoring (générateur Kaggle-like)
* dashboard SQL complet
* Tests PySpark exécutés dans GitHub Actions
* CI GitHub Actions (tests)

Ce projet est entièrement compatible Databricks **Free Edition**, **Unity Catalog**, et **Volumes UC**.


# Architecture globale

                          ┌──────────────────────────────────────────┐
                          │              DATA SOURCES                │
                          │   Kaggle Dataset (Batch)                 │
                          │   Generator Kaggle-like (Streaming)      │
                          └──────────────────────────┬───────────────┘
                                                     │
                                                     ▼
                     ┌────────────────────────────────────────────────────────┐
                     │                    BRONZE LAYER                        │
                     │ Notebook 01 - Bronze Batch                             │
                     │ Notebook 04 - Bronze Streaming                         │
                     │ UC Volume: /Volumes/main/bronze/bronze_volume          │
                     └──────────────────────────┬─────────────────────────────┘
                                                │
                                                ▼
                     ┌────────────────────────────────────────────────────────┐
                     │                    SILVER LAYER                        │
                     │ Notebook 02 - Silver Batch                             │
                     │ Notebook 05 - Silver Streaming                         │
                     │ Enrichments: event_time, minute, hour, day, is_fraud   │
                     └──────────────────────────┬─────────────────────────────┘
                                                │
                                                ▼
                     ┌────────────────────────────────────────────────────────┐
                     │                     GOLD LAYER                         │
                     │ Notebook 03 - Gold Batch                               │
                     │ Notebook 06 - Gold Streaming                           │
                     │ KPI, fraud_by_minute, fraud_by_hour                    │
                     └──────────────────────────┬─────────────────────────────┘
                                                │
                                                ▼
                     ┌────────────────────────────────────────────────────────┐
                     │                     ML TRAINING                        │
                     │ Notebook 07 - ML Training (Batch)                      │
                     │ SparkML + MLflow + UC Volume                           │
                     │ Model: models:/fraud_model/1                           │
                     └──────────────────────────┬─────────────────────────────┘
                                                │ modèle MLflow
                                                ▼
                     ┌────────────────────────────────────────────────────────┐
                     │                    ML INFERENCE                        │
                     │ Notebook 08 - ML Inference Streaming                   │
                     │ Writes to: main.gold.fraud_predictions_stream          │
                     └──────────────────────────┬─────────────────────────────┘
                                                │
                                                ▼
                     ┌────────────────────────────────────────────────────────┐
                     │                    ML MONITORING                       │
                     │ Notebook 09 - Monitoring (accuracy, FP/FN, dérive)     │
                     │ Dashboard SQL                                          │
                     └────────────────────────────────────────────────────────┘


Le projet repose sur 9 notebooks, organisés en trois pipelines :

### Pipeline Batch (historique Kaggle)
| Notebook | Rôle |
| --- | --- |
| 00 | Génération Kaggle-like (optionnel) |
| 01 | Bronze Batch |
| 02 | Silver Batch |
| 03 | Gold Batch |
| 07 | ML Training (SparkML + MLflow) |

### Pipeline Streaming (simulation "temps réel")
| Notebook | Rôle |
| --- | --- |
| 04 | Bronze Streaming |
| 05 | Silver Streaming |
| 06 | Gold Streaming |
| 08 | ML Inference Streaming |

### Pipeline Monitoring ML
| Notebook | Rôle |
| --- | --- |
| 09 | Monitoring du modèle (accuracy, FP/FN, dérive, erreurs) |


---
# Structure du repository
```schema
fraud-detection-databricks/
│
├── notebooks/
│   ├── 00_generate_kaggle.py
│   ├── 01_bronze_batch.py
│   ├── 02_silver_batch.py
│   ├── 03_gold_batch.py
│   ├── 04_bronze_stream.py
│   ├── 05_silver_stream.py
│   ├── 06_gold_stream.py
│   ├── 07_ml_training.py
│   ├── 08_ml_inference.py
│   └── 09_ml_monitoring.py
│
└── .github/
    └── workflows/
        └── ci_databricks.yml
```

# Bronze Layer
## Bronze Batch
* Ingestion du dataset Kaggle-like
* Stockage dans main.bronze.transactions_bronze_batch

## Bronze Streaming
* Ingestion continue via trigger(once)
* Stockage dans main.bronze.transactions_bronze_stream

# Silver Layer
## Enrichissement des données :

* minute, hour, day
* is_fraud

Silver Batch → utilisé pour l’entraînement ML

Silver Streaming → utilisé pour l’inférence ML


# Gold Layer
## Agrégations analytiques :

* KPI globaux
* Fraude par minute
* Fraude par heure
* Fraude par jour

Gold Batch → dashboard historique

Gold Streaming → dashboard

# ML Training (Notebook 07)

Modèle SparkML :

* VectorAssembler
* Logistic Regression
* Split train/test
* Evaluation : AUC, Accuracy, F1-score
* Confusion matrix

Sauvegarde MLflow :

* modèle SparkML
* signature
* registry
* UC Volume (/Volumes/main/ml/models_volume/tmp)

# ML Inference Streaming (Notebook 08)
* Chargement du modèle MLflow
* VectorAssembler
* Application du modèle sur Silver Streaming
* Écriture dans : main.gold.fraud_predictions_stream

# ML Monitoring (Notebook 09)
Calcul en continu :
* Accuracy
* False Positive Rate
* False Negative Rate
* Probabilité moyenne
* Dérive par heure

# Dashboard SQL
Sections :
* KPI globaux
* KPI par période (minute, heure, jour)
* KPI ML
* Courbes de dérive
* Matrice de confusion
* Analyse des erreurs

# CI — GitHub Actions → Databricks

Workflow GitHub Actions
```
.github/workflows/ci_databricks.yml
```
Déclenche :
* Format notebooks with Black
* Lint notebooks with Flake8
* Tests PySpark

# Limitations Databricks Free Edition
Databricks Free Edition ne permet pas :

* d’appeler l’API Jobs
* d’appeler l’API Repos
* d’utiliser le CLI v1 ou v2
* d’automatiser les pipelines via GitHub Actions
* d’utiliser Git Credentials ou Git Integration avancée

Les jobs doivent être exécutés manuellement dans l’UI Databricks.


# Roadmap (si passage à un workspace payant)
Si tu migres vers un workspace Databricks payant :

Tu pourras activer :
* API Jobs (déclenchement automatique)
* API Repos (sync GitHub → Databricks)
* Databricks CLI v2
* Terraform (jobs, clusters, pipelines)
* MLflow Registry complet
* Feature Store