# NYC Yellow Taxi Trips — Pipeline ELT & Machine Learning sur Google Cloud Platform

> **Projet Data Engineering** — Pipeline de bout en bout : ingestion, transformation, analyse et prédiction du montant des courses de taxis jaunes à New York City (2020–présent).

---

## Proposition de valeur

Les taxis jaunes de New York génèrent des **millions de courses par mois**, produisant une masse de données brutes inexploitées. Ce projet répond à une question concrète :

> _Comment transformer des fichiers Parquet bruts publiés chaque mois par la NYC TLC en insights analytiques actionnables et en un modèle capable de prédire le montant total d'une course ?_

**Ce que ce projet apporte :**

| Dimension | Valeur produite |
|---|---|
| **Opérationnel** | Pipeline ELT automatisé et orchestré — zéro intervention manuelle |
| **Analytique** | 15 vues SQL prêtes à connecter à un dashboard (Looker, Power BI, Tableau) |
| **Prédictif** | 4 modèles BigQuery ML + 1 modèle scikit-learn pour prédire le `total_amount` |
| **Scalabilité** | Architecture cloud-native GCP — passe à l'échelle sans refactorisation |

---

## Table des matières

1. [Architecture globale](#architecture-globale)
2. [Stack technique](#stack-technique)
3. [Structure du projet](#structure-du-projet)
4. [Pipeline ELT — étape par étape](#pipeline-elt--étape-par-étape)
5. [Datasets BigQuery](#datasets-bigquery)
6. [Analyses & questions métier](#analyses--questions-métier)
7. [Machine Learning](#machine-learning)
8. [Orchestration Airflow](#orchestration-airflow)
9. [Installation & configuration](#installation--configuration)
10. [Résultats clés](#résultats-clés)

---

## Architecture globale

```
┌─────────────────────────────────────────────────────────────────────┐
│                      SOURCE DE DONNÉES                              │
│          NYC TLC — Fichiers Parquet mensuels (2020 → aujourd'hui)   │
│          https://www.nyc.gov/site/tlc/about/tlc-trip-record-data    │
└────────────────────────────┬────────────────────────────────────────┘
                             │  download_taxi_data.py
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  GOOGLE CLOUD STORAGE (GCS)                         │
│   Bucket : nyc-yellow-trips-data-buckets                            │
│   ├── dataset/trips/          ← fichiers Parquet bruts              │
│   └── from-git/logs/          ← logs d'exécution horodatés          │
└────────────────────────────┬────────────────────────────────────────┘
                             │  load_raw_trips_data.py
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        BIGQUERY                                     │
│                                                                     │
│  raw_yellowtrips                                                    │
│  ├── trips                 ← données brutes chargées                │
│  └── taxi_zone             ← référentiel géographique (265 zones)   │
│                                    │                                │
│                                    │  transform_trips_data.py       │
│                                    ▼                                │
│  transformed_data                                                   │
│  └── cleaned_and_filtered  ← données nettoyées & filtrées           │
│                                    │                                │
│                             ┌──────┴──────┐                         │
│                             │             │                         │
│                             ▼             ▼                         │
│  views_fordashboard         │         ml_dataset                    │
│  └── 15 vues analytiques    │         ├── trips_ml_data             │
│      (Q1 → Q15)             │         ├── preprocessed_train_data   │
│                             │         ├── preprocessed_test_data    │
│                             │         └── yellow_trips_model (×4)   │
└─────────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│              ORCHESTRATION — Cloud Composer (Airflow)               │
│              DAG : elt_pipeline_nyc_taxi                            │
│              Planification : dernier vendredi du mois à 23h00       │
└─────────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│           REPORTING & VISUALISATION (Jupyter / Google Colab)        │
│           Librairies : Pandas · Plotly · scikit-learn               │
│           Rapports : Report 1 → 4 + Custom Model                    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Stack technique

| Couche | Technologie |
|---|---|
| **Stockage objet** | Google Cloud Storage (GCS) |
| **Data Warehouse** | Google BigQuery |
| **Orchestration** | Apache Airflow via Cloud Composer |
| **Traitement Python** | Python 3.x · Pandas · PyArrow · PySpark |
| **Machine Learning** | BigQuery ML · scikit-learn |
| **Visualisation** | Plotly Express · Plotly Graph Objects |
| **Notebooks** | Jupyter / Google Colab |
| **Référentiel** | GitHub → GCS (scripts déployés dans le bucket) |

---

## Structure du projet

```
nyc-yellow-taxi-trips/
│
├── create_datasets.py           # Crée les 4 datasets BigQuery
├── download_taxi_data.py        # Extrait les fichiers Parquet → GCS
├── load_raw_trips_data.py       # Charge les fichiers GCS → BigQuery (raw)
├── transform_trips_data.py      # Nettoie et filtre les données
├── create_ml_dataset_table.py   # Prépare la table pour le ML
├── exploratory_data_analysis.py # Exploration initiale (PySpark + PyArrow)
├── elt_dag_pipeline.py          # DAG Airflow (orchestration du pipeline)
│
├── queries/
│   ├── MarketDemand_and_CustomerBehavior.sql  # Vues analytiques Q1–Q6
│   ├── Financial_and_Pricing.sql              # Vues analytiques Q7–Q11
│   ├── CompetitiveInsights.sql                # Vues analytiques Q12–Q15
│   └── modeling_queries.sql                   # Entraînement des modèles BigQuery ML
│
├── notebooks/
│   ├── Report Notebook.ipynb   # Analyse I & II (Demande & Comportement client)
│   ├── Report 2.ipynb          # Analyse III (Finance & Tarification) — Q7–Q9
│   ├── Report 3.ipynb          # Analyse III suite (Pourboires & Surcharges) — Q10–Q11
│   ├── Report 4.ipynb          # Analyse IV (Compétitivité & Efficacité) — Q12–Q15
│   └── Custom Model.ipynb      # Modèle scikit-learn (prétraitement + upload BigQuery)
│
├── data/
│   └── taxi_zone_lookup.csv    # Référentiel des 265 zones NYC
│
└── requirements.txt            # Dépendances Python
```

---

## Pipeline ELT — étape par étape

### Étape 0 — Initialisation des datasets

```bash
python create_datasets.py
```

Crée les 4 datasets BigQuery s'ils n'existent pas :
- `raw_yellowtrips` — données brutes
- `transformed_data` — données nettoyées
- `views_fordashboard` — vues analytiques
- `ml_dataset` — données pour le Machine Learning

---

### Étape 1 — Extract : téléchargement vers GCS

```bash
python download_taxi_data.py
```

- Télécharge les fichiers Parquet mensuels depuis `data.cityofnewyork.us` (2020 → année courante)
- Vérifie si le fichier existe déjà dans GCS avant de télécharger (idempotent)
- Upload directement en mémoire vers `gs://nyc-yellow-trips-data-buckets/dataset/trips/`
- Journalise chaque opération et uploade les logs dans `from-git/logs/`

**Format des fichiers source :** `yellow_tripdata_YYYY-MM.parquet`

---

### Étape 2 — Load : chargement dans BigQuery

```bash
python load_raw_trips_data.py
```

- Compare les fichiers présents dans GCS avec les fichiers déjà chargés (`source_file`)
- Charge uniquement les **nouveaux fichiers** (chargement incrémental)
- Utilise une table temporaire pour gérer les conflits de type (`passenger_count`)
- Insère dans `raw_yellowtrips.trips` avec une colonne `source_file` pour la traçabilité

**Schéma de la table `trips` :**

| Colonne | Type | Description |
|---|---|---|
| `VendorID` | INT | Identifiant du fournisseur TPEP |
| `tpep_pickup_datetime` | TIMESTAMP | Heure de prise en charge |
| `tpep_dropoff_datetime` | TIMESTAMP | Heure de dépose |
| `passenger_count` | FLOAT | Nombre de passagers |
| `trip_distance` | FLOAT | Distance en miles |
| `RatecodeID` | FLOAT | Code tarifaire (1=Standard, 2=JFK…) |
| `PULocationID` / `DOLocationID` | INT | Zones de pickup/dropoff |
| `payment_type` | INT | Mode de paiement |
| `fare_amount` | FLOAT | Tarif de base |
| `tip_amount` | FLOAT | Pourboire |
| `total_amount` | FLOAT | Montant total |
| `congestion_surcharge` | FLOAT | Surcharge de congestion |
| `airport_fee` | FLOAT | Frais aéroport |
| `source_file` | STRING | Nom du fichier source |

---

### Étape 3 — Transform : nettoyage et filtrage

```bash
python transform_trips_data.py
```

Crée la table `transformed_data.cleaned_and_filtered` en appliquant les filtres :

```sql
WHERE passenger_count > 0      -- Exclut les courses sans passager
  AND trip_distance > 0        -- Exclut les courses sans distance
  AND payment_type != 6        -- Exclut les paiements "Voided trip"
  AND total_amount > 0         -- Exclut les montants nuls ou négatifs
```

---

### Étape 4 — Préparation du dataset ML

```bash
python create_ml_dataset_table.py
```

Filtre les données de novembre 2024 à aujourd'hui, avec uniquement les paiements par carte (1) ou espèces (2), dans `ml_dataset.trips_ml_data`.

---

## Datasets BigQuery

### `raw_yellowtrips`
| Table | Description |
|---|---|
| `trips` | Toutes les courses chargées depuis GCS (données brutes) |
| `taxi_zone` | Référentiel géographique (265 zones, 6 boroughs) |

### `transformed_data`
| Table | Description |
|---|---|
| `cleaned_and_filtered` | Courses valides après filtrage qualité |

### `ml_dataset`
| Table | Description |
|---|---|
| `trips_ml_data` | Données récentes pour entraînement (Nov 2024+) |
| `preprocessed_train_data` | Features préparées — 70% (2,1M lignes) |
| `preprocessed_test_data` | Features préparées — 15% (464K lignes) |
| `yellow_trips_model` | Modèle Boosted Tree Regressor |
| `yellow_trips_rf` | Modèle Random Forest Regressor |
| `yellow_trips_dnn` | Modèle DNN Regressor |
| `yellow_trips_automl` | Modèle AutoML Regressor |

### `views_fordashboard`
15 vues SQL optimisées pour les dashboards — voir section suivante.

---

## Analyses & questions métier

Le projet répond à **15 questions analytiques** réparties en 4 thèmes.

### I — Demande & Saisonnalité (`Report Notebook.ipynb`)

| # | Question | Vue BigQuery |
|---|---|---|
| Q1 | Comment la demande fluctue-t-elle dans le temps (jour, semaine, mois, saison) ? | `demand_over_time` |
| Q2 | Quelles sont les heures de pointe par borough et zone ? | `peak_hours_by_zone` |
| Q3 | Impact des événements météo / jours fériés ? | _(à investiguer)_ |

**Insights clés :**
- Effondrement de 94% de la demande en avril 2020 (COVID-19)
- Récupération progressive avec un plateau à ~8–10M courses/trimestre en 2022–2024
- Heures de pointe : 14h–18h à Manhattan (Upper East Side, Midtown)

---

### II — Comportement client & Caractéristiques des courses (`Report Notebook.ipynb`)

| # | Question | Vue BigQuery |
|---|---|---|
| Q4 | Quels sont les lieux de prise en charge et de dépose les plus populaires ? | `popular_pickup_dropoff` |
| Q5 | Quelle est la distance moyenne selon le borough, l'heure et la saison ? | `avg_trip_distance_analysis` |
| Q6 | Proportion de courses avec 1 vs plusieurs passagers par saison ? | `passenger_trends_by_season` |

**Insights clés :**
- Top 3 pickup zones : Upper East Side South, JFK Airport, Upper East Side North
- 76% des courses sont effectuées avec un seul passager
- Distance moyenne : ~10 miles, légèrement plus élevée la nuit

---

### III — Finance & Tarification (`Report 2.ipynb` & `Report 3.ipynb`)

| # | Question | Vue BigQuery |
|---|---|---|
| Q7 | Comment évolue le revenu total dans le temps ? | `total_fare_revenue_over_time` |
| Q8 | Quel est le tarif moyen selon le borough, l'heure et la distance ? | `avg_fare_analysis` |
| Q9 | Quelle est la part de chaque mode de paiement, et comment évolue-t-elle ? | `payment_type_trends` |
| Q10 | Quelle est la fréquence de pourboire, et quels facteurs l'influencent ? | `tipping_behavior_analysis` |
| Q11 | Combien rapportent les surcharges additionnelles (MTA, congestion, aéroport) ? | `additional_charges_revenue` |

**Insights clés :**
- 74% des paiements se font par carte bancaire (vs 26% espèces)
- Taux de pourboire moyen : ~84% des courses par carte donnent lieu à un pourboire
- Pourboire moyen : ~15% du montant total
- La surcharge de congestion représente la principale surcharge additionnelle

---

### IV — Compétitivité & Efficacité opérationnelle (`Report 4.ipynb`)

| # | Question | Vue BigQuery |
|---|---|---|
| Q12 | Quels boroughs ont le plus/moins de volume de courses ? | `trip_volume_by_borough` |
| Q13 | À quelle fréquence les taxis desservent-ils les aéroports (JFK, LGA, EWR) ? | `airport_trips_analysis` |
| Q14 | Quelle est l'utilisation des différents codes tarifaires par borough ? | `rate_code_analysis` |
| Q15 | Quelle est la durée typique d'une course, et évolue-t-elle dans le temps ? | `trip_duration_analysis` |

**Insights clés :**
- Manhattan représente >95% du volume total de courses
- JFK : tarif moyen ~53$, LGA : ~38$, Newark : ~94$
- Durée moyenne d'une course : 12–15 minutes
- Le Standard rate (code 1) représente >98% des courses

---

## Machine Learning

### Objectif
Prédire le **montant total d'une course** (`total_amount`) à partir des caractéristiques connues au moment du départ.

### Features utilisées

| Feature | Description |
|---|---|
| `PULocationID` | Zone de départ |
| `DOLocationID` | Zone d'arrivée |
| `passenger_count` | Nombre de passagers |
| `trip_distance` | Distance en miles |
| `trip_duration` | Durée calculée en minutes |
| `pickup_hour` | Heure de départ |
| `pickup_dayofweek` | Jour de la semaine (0=Lundi) |
| `pickup_month` | Mois |
| `pickup_year` | Année |
| `is_weekend` | Indicateur week-end (0/1) |
| `is_credit_card` | Mode de paiement carte (0/1) |

### Split des données
- **Entraînement** : 70% → 2 167 941 lignes
- **Validation** : 15% → 464 560 lignes
- **Test** : 15% → 464 560 lignes

### Modèles BigQuery ML (`modeling_queries.sql`)

```sql
-- Boosted Tree Regressor (modèle principal)
CREATE OR REPLACE MODEL `ml_dataset.yellow_trips_model`
OPTIONS (model_type="BOOSTED_TREE_REGRESSOR",
         enable_global_explain=TRUE,
         input_label_cols=["total_amount"])
AS SELECT * FROM `ml_dataset.preprocessed_train_data`;

-- Random Forest Regressor
-- DNN Regressor
-- AutoML Regressor
```

**Évaluation :**
```sql
SELECT * FROM ML.EVALUATE(MODEL `ml_dataset.yellow_trips_model`,
  (SELECT * FROM `ml_dataset.preprocessed_test_data`));
```

**Explainabilité :**
```sql
SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `ml_dataset.yellow_trips_model`);
```

### Modèle Python custom (`Custom Model.ipynb`)

Pipeline scikit-learn complet avec :
1. Prétraitement des features temporelles
2. Split train/validation/test (70/15/15)
3. Upload des sets prétraités dans BigQuery
4. Entraînement et évaluation

---

## Orchestration Airflow

Le DAG `elt_pipeline_nyc_taxi` orchestre l'exécution automatique du pipeline.

**Planification :** chaque vendredi à 23h00 (dernier vendredi du mois)

```
wait_for_last_friday → download_taxi_data → load_raw_trips_data → transform_trips_data
```

Chaque tâche :
1. Récupère le script Python depuis GCS (`gsutil cp`)
2. L'exécute directement sur le worker Airflow

**Politique de retry :** 2 tentatives, délai de 5 minutes entre chaque.

---

## Installation & configuration

### Prérequis

- Compte Google Cloud Platform avec un projet actif
- Droits : BigQuery Admin, Storage Admin, Composer Admin
- Python 3.9+

### 1. Cloner le dépôt

```bash
git clone https://github.com/tchindajoel/nyc-yellow-taxi-trips.git
cd nyc-yellow-taxi-trips
```

### 2. Installer les dépendances

```bash
pip install -r requirements.txt
```

### 3. Configurer l'authentification GCP

```bash
gcloud auth application-default login
gcloud config set project nyc-yellow-trips
```

### 4. Initialiser les datasets BigQuery

```bash
python create_datasets.py
```

### 5. Lancer le pipeline manuellement

```bash
# Étape 1 — Extraction
python download_taxi_data.py

# Étape 2 — Chargement
python load_raw_trips_data.py

# Étape 3 — Transformation
python transform_trips_data.py

# Étape 4 — Préparation ML (optionnel)
python create_ml_dataset_table.py
```

### 6. Déployer le DAG Airflow

```bash
# Copier les scripts dans le bucket GCS
gsutil cp *.py gs://nyc-yellow-trips-data-buckets/from-git/

# Copier le DAG dans le dossier Composer
gsutil cp elt_dag_pipeline.py gs://<votre-bucket-composer>/dags/
```

### 7. Créer les vues analytiques

Exécuter les fichiers SQL dans BigQuery dans cet ordre :
```
queries/MarketDemand_and_CustomerBehavior.sql
queries/Financial_and_Pricing.sql
queries/CompetitiveInsights.sql
```

---

## Résultats clés

### Volume de données
- **~156 millions** de courses NYC Yellow Taxi analysées (2020–2024)
- **~5 ans** de données historiques
- **265 zones** géographiques couvertes

### Pipeline
- Chargement incrémental : seuls les nouveaux fichiers sont traités
- Logs horodatés uploadés automatiquement dans GCS
- Exécution idempotente : relançable sans duplication

### Machine Learning
- **3 097 061** observations dans le dataset ML (Nov 2024+)
- 4 modèles BigQuery ML + 1 modèle scikit-learn entraînés
- Explainabilité via `ML.GLOBAL_EXPLAIN` pour identifier les variables les plus influentes

### Analytique
- **15 questions métier** répondues via des vues SQL dédiées
- Visualisations interactives avec Plotly (line, bar, heatmap, treemap, scatter, area)
- Rapports organisés en 4 notebooks thématiques

---

## Données source

Les données proviennent du **NYC Taxi & Limousine Commission (TLC)** :
- Format : Parquet (colonnar, optimisé pour BigQuery)
- Fréquence de publication : mensuelle
- Couverture : janvier 2020 → mois courant
- URL : `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`

---

## Licence

Ce projet est distribué sous licence MIT. Voir le fichier [LICENSE](LICENSE) pour plus de détails.
