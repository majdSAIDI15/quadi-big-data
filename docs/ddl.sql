CREATE TABLE IF NOT EXISTS delta.`/data/lakehouse/bronze/transactions` (
  transaction_id STRING NOT NULL COMMENT 'Identifiant unique (clé de dédoublonnage)',
  source_account STRING NOT NULL COMMENT 'Compte émetteur (sommet source)',
  target_account STRING NOT NULL COMMENT 'Compte bénéficiaire (sommet cible)',
  montant DOUBLE NOT NULL COMMENT 'Montant de la transaction',
  devise STRING COMMENT 'Devise ISO 4217',
  type_operation STRING COMMENT 'Nature de l\'opération',
  event_time TIMESTAMP NOT NULL COMMENT 'Horodatage métier (temps événement)',
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP COMMENT 'Horodatage d\'ingestion Kafka',
  ingested_at TIMESTAMP COMMENT 'Écriture dans le lakehouse',
  dt STRING NOT NULL COMMENT 'Partition jour YYYY-MM-DD'
)
USING DELTA
PARTITIONED BY (dt)
COMMENT 'Bronze - transactions Kafka validées et dédoublonnées'
TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 30 days'
);
ALTER TABLE delta.`/data/lakehouse/bronze/transactions` ADD CONSTRAINT montant_positif CHECK (montant > 0);
ALTER TABLE delta.`/data/lakehouse/bronze/transactions` ADD CONSTRAINT pas_auto_virement CHECK (source_account <> target_account);

CREATE TABLE IF NOT EXISTS delta.`/data/lakehouse/bronze/transactions_quarantine` (
  raw_value STRING COMMENT 'Message Kafka brut',
  error STRING NOT NULL COMMENT 'Motif du rejet',
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP,
  ingested_at TIMESTAMP,
  dt STRING NOT NULL
)
USING DELTA
PARTITIONED BY (dt)
COMMENT 'Bronze - messages rejetés par le contrôle qualité'
TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 30 days'
);

CREATE TABLE IF NOT EXISTS delta.`/data/lakehouse/silver/account_metrics` (
  snapshot_id STRING NOT NULL COMMENT 'Identifiant de l\'instantané du chemin lent',
  snapshot_time TIMESTAMP NOT NULL,
  account_id STRING NOT NULL,
  pagerank DOUBLE COMMENT 'PageRank sur le graphe des gros flux',
  pagerank_pct DOUBLE COMMENT 'Percentile du PageRank (0-1)',
  in_degree INT,
  out_degree INT,
  inflow DOUBLE COMMENT 'Montant reçu sur la fenêtre',
  outflow DOUBLE COMMENT 'Montant émis sur la fenêtre',
  flow_through DOUBLE COMMENT 'outflow / inflow (compte de passage si proche de 1)',
  scc_id STRING COMMENT 'Composante fortement connexe (graphe des gros flux)',
  scc_size INT,
  cluster_id STRING COMMENT 'Composante connexe du graphe suspect',
  n_alerts INT COMMENT 'Alertes impliquant le compte sur la fenêtre',
  dt STRING NOT NULL
)
USING DELTA
PARTITIONED BY (dt)
COMMENT 'Silver - métriques de graphe par compte et par instantané'
TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 30 days'
);

CREATE TABLE IF NOT EXISTS delta.`/data/lakehouse/silver/traffic_windows` (
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  tx_count BIGINT,
  total_amount DOUBLE,
  high_value_count BIGINT,
  active_accounts BIGINT COMMENT 'Émetteurs distincts (approx.)',
  updated_at TIMESTAMP,
  dt STRING NOT NULL
)
USING DELTA
PARTITIONED BY (dt)
COMMENT 'Silver - trafic agrégé en fenêtre glissante 10 min (pas 1 min)'
TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 30 days'
);

CREATE TABLE IF NOT EXISTS delta.`/data/lakehouse/gold/fraud_alerts` (
  alert_id STRING NOT NULL COMMENT 'Empreinte déterministe : idempotence en cas de rejeu',
  alert_type STRING NOT NULL COMMENT 'CYCLE, CYCLE_LONG, MULE, SCATTER_GATHER, PIVOT, CLUSTER',
  risk_score INT NOT NULL COMMENT 'Score 0-100',
  risk_level STRING NOT NULL COMMENT 'LOW, MEDIUM, HIGH, CRITICAL',
  accounts ARRAY<STRING> NOT NULL,
  transaction_ids ARRAY<STRING>,
  total_amount DOUBLE,
  devise STRING,
  cycle_length INT,
  window_start TIMESTAMP COMMENT 'Première transaction du motif',
  window_end TIMESTAMP COMMENT 'Dernière transaction du motif',
  detected_at TIMESTAMP NOT NULL,
  latency_ms BIGINT COMMENT 'Délai ingestion Kafka -> alerte',
  explanation STRING COMMENT 'Explication en langage clair',
  features STRING COMMENT 'Indicateurs du score (JSON)',
  detector STRING COMMENT 'fast-path ou slow-path',
  dt STRING NOT NULL
)
USING DELTA
PARTITIONED BY (risk_level, dt)
COMMENT 'Gold - alertes de fraude (piste d\'audit immuable)'
TBLPROPERTIES (
  'delta.appendOnly' = 'true',
  'delta.logRetentionDuration' = 'interval 365 days',
  'delta.deletedFileRetentionDuration' = 'interval 30 days'
);
ALTER TABLE delta.`/data/lakehouse/gold/fraud_alerts` ADD CONSTRAINT score_borne CHECK (risk_score BETWEEN 0 AND 100);
ALTER TABLE delta.`/data/lakehouse/gold/fraud_alerts` ADD CONSTRAINT niveau_valide CHECK (risk_level IN ('LOW','MEDIUM','HIGH','CRITICAL'));

CREATE TABLE IF NOT EXISTS delta.`/data/lakehouse/gold/anomaly_subgraph_vertices` (
  alert_id STRING NOT NULL,
  account_id STRING NOT NULL,
  role STRING COMMENT 'ORIGINE, RELAIS, MULE, PIVOT, SOURCE, DESTINATION, MEMBRE',
  pagerank DOUBLE,
  risk_level STRING NOT NULL,
  dt STRING NOT NULL
)
USING DELTA
PARTITIONED BY (risk_level, dt)
COMMENT 'Gold - sommets des sous-graphes d\'anomalies'
TBLPROPERTIES (
  'delta.appendOnly' = 'true',
  'delta.logRetentionDuration' = 'interval 365 days',
  'delta.deletedFileRetentionDuration' = 'interval 30 days'
);

CREATE TABLE IF NOT EXISTS delta.`/data/lakehouse/gold/anomaly_subgraph_edges` (
  alert_id STRING NOT NULL,
  transaction_id STRING NOT NULL,
  src STRING NOT NULL,
  dst STRING NOT NULL,
  montant DOUBLE,
  event_time TIMESTAMP,
  type_operation STRING,
  risk_level STRING NOT NULL,
  dt STRING NOT NULL
)
USING DELTA
PARTITIONED BY (risk_level, dt)
COMMENT 'Gold - arêtes (transactions) des sous-graphes d\'anomalies'
TBLPROPERTIES (
  'delta.appendOnly' = 'true',
  'delta.logRetentionDuration' = 'interval 365 days',
  'delta.deletedFileRetentionDuration' = 'interval 30 days'
);

