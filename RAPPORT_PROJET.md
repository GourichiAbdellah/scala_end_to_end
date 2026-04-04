# Rapport Complet — NASA Log Analysis Pipeline
## Scala / Apache Spark / Kafka / PostgreSQL

---

## 1. Présentation du projet

Ce projet analyse les **logs HTTP du serveur NASA Kennedy Space Center** (juillet–août 1995).
Le dataset contient **~2 965 561 lignes** de requêtes HTTP issues de clients du monde entier.

### Objectifs
- Ingérer et nettoyer les logs bruts (CSV)
- Enrichir chaque log avec la géolocalisation IP (pays, continent)
- Produire 15 analyses batch agrégées
- Stocker les résultats dans PostgreSQL et en CSV/Parquet
- Ajouter un pipeline **temps réel** avec Kafka + Spark Structured Streaming

---

## 2. Stack technique

| Composant | Version |
|---|---|
| Scala | 2.12.18 |
| Apache Spark | 3.5.1 |
| Spark Streaming (Kafka) | spark-sql-kafka-0-10 3.5.1 |
| PostgreSQL JDBC | 42.7.3 |
| ipaddress (CIDR parsing) | 5.5.0 |
| Apache Kafka | Confluent 7.5.0 (Docker) |
| Zookeeper | Confluent 7.5.0 (Docker) |
| sbt | 1.9.9 |
| Java | 21 |

---

## 3. Structure du projet

```
projet-sc/
├── build.sbt                                          Dépendances + options Java 21
├── docker-compose.yml                                 Kafka + Zookeeper via Docker
├── data/
│   ├── data.csv                                       ~2.96M logs NASA (bruts)
│   ├── GeoLite2-Country-Blocks-IPv4.csv               ~172K blocs CIDR IPv4
│   └── GeoLite2-Country-Locations-en.csv              ~250 pays/continents
├── output/                                            Résultats batch (CSV + Parquet)
│   ├── traffic_by_country/
│   ├── traffic_by_continent/
│   ├── top_pages/
│   ├── error_analysis/
│   ├── hourly_traffic/
│   ├── daily_traffic/
│   ├── day_of_week_traffic/
│   ├── bandwidth_by_category/
│   ├── top_hosts/
│   ├── url_category_distribution/
│   ├── response_distribution/
│   ├── country_hour_heatmap/
│   ├── top_error_pages/
│   ├── daily_bandwidth/
│   ├── method_distribution/
│   └── enriched_logs_parquet/                         Logs enrichis complets
├── checkpoints/                                       State Spark Streaming
└── src/main/scala/nasa/
    ├── Main.scala                                     Orchestrateur pipeline batch
    ├── utils/IpUtils.scala
    ├── ingestion/DataIngestion.scala
    ├── cleaning/DataCleaning.scala
    ├── geolocation/IpGeolocation.scala
    ├── transformation/DataTransformation.scala
    ├── analysis/LogAnalysis.scala
    ├── storage/DatabaseWriter.scala
    └── streaming/
        ├── StreamingConfig.scala
        ├── KafkaLogProducer.scala
        ├── StreamingDataCleaning.scala
        ├── StreamingAnalysis.scala
        ├── StreamingDatabaseWriter.scala
        ├── GracefulShutdown.scala
        └── StreamingMain.scala
```

---

## 4. Pipeline Batch — détail

### 4.1 Architecture

```
data.csv  ──►  DataIngestion  ──►  DataCleaning  ──►  IpGeolocation
                                                           │
                     GeoLite2 CIDR ──►  buildGeoRanges ───┘
                                                           │
                                                    DataTransformation
                                                           │
                                                     LogAnalysis (×15)
                                                           │
                                              DatabaseWriter + CSV/Parquet
```

### 4.2 Modules Scala (batch)

#### `DataIngestion.scala`
- Lit `data.csv` (logs NASA) avec `spark.read.csv`
- Lit `GeoLite2-Country-Blocks-IPv4.csv` (172K blocs CIDR)
- Lit `GeoLite2-Country-Locations-en.csv` (250 pays)
- Gestion des guillemets et encodage

#### `DataCleaning.scala`
- Supprime la colonne d'index
- Cast des types : `time` → Long, `response` → Integer, `bytes` → Long
- Filtre les lignes malformées (response ou time null)
- Remplace les bytes null par 0

#### `IpGeolocation.scala`
- Utilise la librairie `ipaddress` pour convertir les blocs CIDR (ex: `192.168.1.0/24`) en plages IP (ip_start, ip_end) sous forme de Long
- UDFs enregistrées : `isIpAddress`, `ipToLong`
- Broadcast Range Join : `ip_long BETWEEN ip_start AND ip_end`
- Jointure left join → chaque log reçoit `country_name`, `country_iso_code`, `continent_name`, `continent_code`

#### `DataTransformation.scala`
Enrichit chaque ligne avec des colonnes dérivées :

| Colonne ajoutée | Description |
|---|---|
| `timestamp` | Unix timestamp → Timestamp Spark |
| `hour_of_day` | Heure (0–23) |
| `day_of_week` | Jour en lettres (Monday, Tuesday...) |
| `date` | Date seule (yyyy-MM-dd) |
| `url_category` | Catégorie URL : Shuttle, History, Images, Facilities, Homepage, Software, Facts, Persons, ELV, CGI, Other |
| `response_category` | Success / Redirect / Client Error / Server Error / Other |
| `bytes_kb` | Taille en KB (bytes / 1024) |

#### `LogAnalysis.scala` — 15 analyses

| # | Analyse | Table PostgreSQL / Dossier CSV |
|---|---|---|
| 1 | Trafic par pays (top 20) | `traffic_by_country` |
| 2 | Trafic par continent | `traffic_by_continent` |
| 3 | Top 50 pages les plus demandées | `top_pages` |
| 4 | Distribution codes HTTP + % | `error_analysis` |
| 5 | Trafic par heure (0–23) | `hourly_traffic` |
| 6 | Trafic journalier (évolution) | `daily_traffic` |
| 7 | Trafic par jour de la semaine | `day_of_week_traffic` |
| 8 | Bande passante par catégorie URL | `bandwidth_by_category` |
| 9 | Top 50 hosts les plus actifs | `top_hosts` |
| 10 | Distribution catégories URL + % | `url_category_distribution` |
| 11 | Distribution codes de réponse | `response_distribution` |
| 12 | Heatmap pays × heure (top 10 pays) | `country_hour_heatmap` |
| 13 | Top 30 pages en erreur 404 | `top_error_pages` |
| 14 | Bande passante journalière (MB) | `daily_bandwidth` |
| 15 | Distribution méthodes HTTP + % | `method_distribution` |

#### `DatabaseWriter.scala`
- `writeToDb` : écriture PostgreSQL en mode overwrite
- `writeLargeToDb` : batchsize=10000, numPartitions=8 (pour les gros DataFrames)
- `exportToCsv` : export CSV coalesce(1), un seul fichier par analyse
- Connexion : `jdbc:postgresql://localhost:5432/nasa_pipeline` (user: spark_user)

---

## 5. Pipeline Streaming — détail

### 5.1 Architecture

```
data.csv ──► KafkaLogProducer ──► Kafka "nasa-logs" ──► StreamingMain
                                                              │
                                              Parse JSON (schema explicite)
                                                              │
                                              StreamingDataCleaning.clean()
                                                              │
                                     IpGeolocation.joinWithGeo() [stream-static join]
                                                              │
                                              DataTransformation.enrich()
                                                              │
                              ┌───────────────┬──────────────┬───────────────┐
                              ▼               ▼              ▼               ▼
                    requestsByCountry  requestsByUrl    errorRate      trafficVolume
                      Windowed          CategoryWindowed  Windowed       Windowed
                              │               │              │               │
                              └───────────────┴──────────────┴───────────────┘
                                                              │
                                                    foreachBatch (toutes les 30s)
                                                              │
                                              Console + PostgreSQL (append)
```

### 5.2 Modules Scala (streaming)

#### `StreamingConfig.scala`
Constantes centralisées :
- `KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"`
- `KAFKA_TOPIC = "nasa-logs"`
- `WINDOW_DURATION = "5 minutes"` (fenêtres tumbling)
- `WATERMARK_DURATION = "10 minutes"` (tolérance retard)
- `CHECKPOINT_BASE_DIR = "checkpoints"`

#### `KafkaLogProducer.scala`
- Application standalone (sans Spark)
- Lit `data.csv` ligne par ligne
- Construit un JSON par ligne : `{"host":"...","time":...,"method":"...","url":"...","response":...,"bytes":...}`
- Publie sur le topic `nasa-logs` avec le host comme clé
- Délai configurable : `sbt "runMain nasa.streaming.KafkaLogProducer 10"` (10ms/message)

#### `StreamingDataCleaning.scala`
- Identique à `DataCleaning` mais sans la suppression de la colonne index (absente des messages Kafka)
- Cast types, filtre lignes malformées, bytes null → 0

#### `StreamingAnalysis.scala` — 4 analyses fenetrées

| Analyse | Fenêtre | Watermark | Table PostgreSQL |
|---|---|---|---|
| Requêtes par pays | 5 min tumbling | 10 min | `stream_requests_by_country` |
| Requêtes par catégorie URL | 5 min tumbling | 10 min | `stream_requests_by_url_category` |
| Taux d'erreur | 5 min tumbling | 10 min | `stream_error_rate` |
| Volume de trafic | 5 min tumbling | 10 min | `stream_traffic_volume` |

Colonnes communes : `window_start`, `window_end` (bornes de la fenêtre temporelle)

#### `StreamingDatabaseWriter.scala`
- Pattern `foreachBatch` : appelé toutes les 30 secondes (`Trigger.ProcessingTime`)
- Affiche les résultats dans la console
- Écrit en mode **append** dans PostgreSQL (accumulation)
- Try/catch : le pipeline continue si PostgreSQL est indisponible

#### `GracefulShutdown.scala`
- Hook JVM sur Ctrl+C
- Arrête proprement toutes les streaming queries

#### `StreamingMain.scala`
Orchestrateur :
1. Charge les données géo au démarrage, cache en mémoire
2. Lit le flux Kafka (`readStream.format("kafka")`)
3. Parse le JSON avec schema explicite (`from_json`)
4. Applique : Clean → Geolocate → Transform
5. Lance les 4 analyses fenetrées en parallèle
6. 4 `writeStream.foreachBatch(...)` avec checkpoints séparés
7. `spark.streams.awaitAnyTermination()` — tourne indéfiniment

### 5.3 Infrastructure Docker

```yaml
# docker-compose.yml
services:
  zookeeper: confluentinc/cp-zookeeper:7.5.0  port 2181
  kafka:     confluentinc/cp-kafka:7.5.0       port 9092
```

Commandes :
```bash
docker compose up -d      # Démarrer Kafka
docker compose down       # Arrêter Kafka
```

---

## 6. Tables PostgreSQL

### Tables batch (15 tables)

| Table | Colonnes clés | Description |
|---|---|---|
| `traffic_by_country` | country_name, country_iso_code, requests, total_bytes, unique_hosts | Top 20 pays |
| `traffic_by_continent` | continent_name, continent_code, requests, total_bytes, unique_hosts | Par continent |
| `top_pages` | url, requests, total_bytes | Top 50 URLs |
| `error_analysis` | response, response_category, requests, percentage | Codes HTTP |
| `hourly_traffic` | hour_of_day, requests, total_bytes | 24 heures |
| `daily_traffic` | date, requests, unique_hosts | Évolution quotidienne |
| `day_of_week_traffic` | day_of_week, requests, total_bytes | Par jour semaine |
| `bandwidth_by_category` | url_category, requests, total_bytes, avg_bytes | Par catégorie |
| `top_hosts` | host, requests, total_bytes | Top 50 IPs |
| `url_category_distribution` | url_category, requests, percentage | Distribution % |
| `response_distribution` | response, response_category, requests | Codes HTTP |
| `country_hour_heatmap` | country_name, hour_of_day, requests | Heatmap |
| `top_error_pages` | url, requests | Pages 404 |
| `daily_bandwidth` | date, total_bytes, total_mb | MB/jour |
| `method_distribution` | method, requests, percentage | GET/POST/etc. |

### Tables streaming (4 tables)

| Table | Colonnes | Granularité |
|---|---|---|
| `stream_requests_by_country` | window_start, window_end, country_name, requests, total_bytes | Fenêtre 5 min |
| `stream_requests_by_url_category` | window_start, window_end, url_category, requests, total_bytes, avg_bytes | Fenêtre 5 min |
| `stream_error_rate` | window_start, window_end, total_requests, error_requests, error_rate_pct | Fenêtre 5 min |
| `stream_traffic_volume` | window_start, window_end, requests, total_bytes, total_mb, unique_hosts | Fenêtre 5 min |

---

## 7. Commandes de lancement

```bash
# 1. Démarrer Kafka
docker compose up -d

# 2. Pipeline batch complet
sbt "runMain nasa.Main"

# 3. Pipeline streaming — Consumer (Terminal 1)
sbt "runMain nasa.streaming.StreamingMain"

# 4. Pipeline streaming — Producer (Terminal 2)
sbt "runMain nasa.streaming.KafkaLogProducer 10"

# 5. Arrêter Kafka
docker compose down
```

---

## 8. Visualisation Power BI

### 8.1 Connexion PostgreSQL

Dans Power BI Desktop :
1. **Obtenir les données** → **Base de données PostgreSQL**
2. Serveur : `localhost`
3. Base de données : `nasa_pipeline`
4. Mode de connectivité : **Import** (batch) ou **DirectQuery** (streaming)
5. Identifiants : `spark_user` / `motdepasse`

### 8.2 Tables recommandées et visualisations

#### Tableau de bord 1 — Vue globale du trafic

| Table | Visualisation | Axes |
|---|---|---|
| `traffic_by_country` | Carte choroplèthe | Pays = country_iso_code, Taille = requests |
| `traffic_by_continent` | Graphique à barres | Axe X = continent_name, Valeur = requests |
| `hourly_traffic` | Courbe en aires | Axe X = hour_of_day, Valeur = requests |
| `daily_traffic` | Courbe temporelle | Axe X = date, Valeur = requests + unique_hosts |

#### Tableau de bord 2 — Analyse du contenu

| Table | Visualisation | Axes |
|---|---|---|
| `top_pages` | Graphique à barres horizontales | Valeur = requests, Catégorie = url |
| `url_category_distribution` | Graphique en anneau (donut) | Légende = url_category, Valeur = percentage |
| `bandwidth_by_category` | Graphique à barres empilées | Catégorie = url_category, Valeur = total_bytes |
| `method_distribution` | Graphique en secteurs (camembert) | Légende = method, Valeur = percentage |

#### Tableau de bord 3 — Erreurs et qualité

| Table | Visualisation | Axes |
|---|---|---|
| `error_analysis` | Graphique à barres | Catégorie = response_category, Valeur = requests |
| `response_distribution` | Treemap | Groupe = response_category, Taille = requests |
| `top_error_pages` | Tableau | Colonnes = url, requests |

#### Tableau de bord 4 — Patterns temporels

| Table | Visualisation | Axes |
|---|---|---|
| `day_of_week_traffic` | Graphique à barres | Axe X = day_of_week, Valeur = requests |
| `daily_bandwidth` | Courbe | Axe X = date, Valeur = total_mb |
| `country_hour_heatmap` | Matrice (heatmap) | Lignes = country_name, Colonnes = hour_of_day, Valeurs = requests |

#### Tableau de bord 5 — Streaming temps réel (DirectQuery)

| Table | Visualisation | Axes |
|---|---|---|
| `stream_traffic_volume` | Courbe temps réel | Axe X = window_start, Valeur = requests + total_mb |
| `stream_error_rate` | Jauge / KPI | Valeur = error_rate_pct, dernière fenêtre |
| `stream_requests_by_country` | Carte animée | Pays = country_name, Taille = requests |
| `stream_requests_by_url_category` | Graphique à barres | Catégorie = url_category, Valeur = requests |

### 8.3 Mesures DAX recommandées

```dax
-- Taux d'erreur global (batch)
Taux Erreur % = 
    DIVIDE(
        CALCULATE(SUM(error_analysis[requests]), error_analysis[response_category] = "Client Error"),
        SUM(error_analysis[requests])
    ) * 100

-- Total MB transféré
Total MB = SUM(daily_bandwidth[total_mb])

-- Requêtes heure de pointe
Heure de Pointe = 
    TOPN(1, hourly_traffic, hourly_traffic[requests], DESC)

-- Taux d'erreur streaming (dernière fenêtre)
Dernier Taux Erreur = 
    CALCULATE(
        LASTNONBLANK(stream_error_rate[error_rate_pct], 1),
        TOPN(1, stream_error_rate, stream_error_rate[window_start], DESC)
    )
```

### 8.4 Filtres et slicers suggérés

- **Slicer date** : sur `daily_traffic[date]` pour filtrer la période (juillet vs août 1995)
- **Slicer pays** : sur `traffic_by_country[country_name]` pour isoler un pays
- **Slicer catégorie URL** : sur `url_category_distribution[url_category]`
- **Slicer heure** : sur `hourly_traffic[hour_of_day]` (0–23)

---

## 9. Résultats clés observés

- **2 965 561** requêtes HTTP traitées
- Fenêtres de 5 minutes sur données événementielles 1995
- **4 analyses streaming** actives toutes les 30 secondes
- **15 analyses batch** exportées en CSV et stockées en PostgreSQL
- Pipeline robuste : continue si PostgreSQL est indisponible (try/catch)
- Géolocalisation IP par broadcast join sur 172K plages CIDR

---

*Généré le 2026-04-04*
