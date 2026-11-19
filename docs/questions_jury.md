# Préparation aux questions du jury

Réponses courtes, à dire en 30 secondes chacune. Les détails sont dans
[architecture.md](architecture.md) et [resultats.md](resultats.md).

---

**Pourquoi GraphFrames plutôt que Neo4j ?**
Neo4j est une base transactionnelle : excellente pour des requêtes ponctuelles, mais elle
n'est pas faite pour ingérer un flux continu et recalculer PageRank sur tout le graphe
chaque minute. GraphFrames s'intègre à Spark Structured Streaming et à Delta Lake : un seul
moteur distribué pour l'ingestion, les algorithmes et l'historisation. En production, on
pourrait exporter les sous-graphes gold vers Neo4j pour l'investigation interactive.

**Pourquoi une fenêtre de 10 minutes et un watermark de 2 minutes ?**
Ce sont les valeurs du sujet. Elles ont un sens métier : un carrousel automatisé boucle en
quelques minutes ; au-delà, le risque de faux positifs augmente (le commerce légitime finit
aussi par former des boucles). Le watermark borne la mémoire et définit la tolérance aux
retards réseau. Les deux sont paramétrables (`WINDOW_MINUTES`, `WATERMARK_MINUTES`).

**GraphFrames ne fonctionne pas en streaming : comment faites-vous ?**
`foreachBatch` : à chaque micro-batch, on ajoute les nouvelles arêtes à un graphe glissant
et on lance le motif. Pour que le coût ne dépende pas de la taille de la fenêtre, on ne
cherche que les triangles fermés par une arête du lot (élagage des candidats).

**Comment évitez-vous de détecter le même cycle plusieurs fois ?**
Trois garanties : le filtre temporel e1 < e2 < e3 ne laisse qu'une rotation ; la recherche
exige au moins une arête nouvelle (un cycle n'est complet qu'une fois) ; l'`alert_id` est
l'empreinte des transactions, et le chemin lent ignore les identifiants déjà émis.

**Votre détection est-elle vraiment « sous-seconde » ?**
Non, et nous le mesurons honnêtement : sur un portable (8 vCPU partagés entre Kafka,
Spark et le dashboard), la latence médiane d'une alerte carrousel est de quelques secondes
(voir le benchmark). Le motif lui-même s'exécute en moins d'une seconde ; le reste est le
coût fixe des micro-batches et des écritures Delta. Sur un cluster, on isolerait le chemin
rapide (exécuteurs dédiés, écritures Delta asynchrones) ; pour viser la milliseconde, ce
seul chemin pourrait passer sur un moteur événement par événement (Flink), le reste
restant en Spark.

**Que faites-vous des faux positifs ?**
Chaque alerte a un score expliqué (montants quasi égaux, commission régulière, vitesse,
montant, PageRank). Seules les alertes ≥ MEDIUM partent dans Kafka ; les LOW restent dans
le lakehouse pour l'audit. La précision est mesurée contre la vérité terrain et l'étude
d'ablation montre ce que chaque filtre apporte.

**Comment distinguez-vous une mule d'un commerçant ?**
Tous deux reçoivent de nombreux petits paiements puis font un gros virement. La différence est
le rythme : un commerçant reçoit en continu, une mule reçoit une rafale soudaine. Nous comparons
les dépôts des 6 minutes avant le retrait au rythme habituel du compte sur les 20 minutes
précédentes (pic de vélocité ≥ 3×). Mesuré : 0 faux positif, contre 18 à 22 sans ce critère.
Contrepartie : 12 minutes d'historique sont nécessaires au démarrage.

**Pourquoi PageRank sur le graphe des gros flux seulement ?**
Sur tout le graphe, les commerçants légitimes (très sollicités) dominent le classement.
Le blanchiment déplace des montants élevés : le graphe ≥ 20 000 MAD est plus petit, plus
rapide à traiter, et c'est là que les pivots ressortent.

**Pourquoi des SCC en plus des Connected Components demandées ?**
Les CC ignorent le sens des virements : elles regroupent des comptes reliés, sans dire
s'il y a une boucle. Une SCC garantit qu'on peut partir d'un compte et y revenir : c'est la
définition même d'un circuit de blanchiment. Les SCC servent de préfiltre aux cycles longs ;
les CC servent à regrouper les alertes en réseaux.

**Le système tient-il dans la durée ?**
Nous l'avons appris à nos dépens : après 30 minutes, les DataFrames matérialisés par
`localCheckpoint` (un par micro-batch) saturaient la mémoire, car `unpersist()` ne les libère
pas. Nous les libérons désormais explicitement, avec un GC Spark toutes les 2 minutes, et nous
surveillons la mémoire pendant les runs d'évaluation de 40 minutes.

**Que se passe-t-il si Spark tombe ?**
On le montre en démo (`docker compose kill spark-streaming`). Au redémarrage, Spark reprend
aux offsets Kafka du checkpoint, le graphe glissant est reconstruit depuis le bronze, les
écritures Delta rejouées sont ignorées (`txnAppId/txnVersion`) : aucune perte, aucun
doublon dans le lakehouse.

**Comment passer à l'échelle d'une vraie banque ?**
Cluster Spark, Kafka partitionné par compte émetteur, chemin lent dans une application
séparée, lakehouse sur stockage objet. L'algorithme incrémental reste le même : son coût
suit le volume du micro-batch.

**Les données sont-elles réalistes ?**
Synthétiques mais structurées : activité en loi de puissance, commerçants « hubs »,
bénéficiaires habituels, montants log-normaux par type d'opération, événements en retard
et messages invalides. Les fraudes reprennent la typologie de l'IBM AML dataset
(Altman et al., NeurIPS 2023) : cycle, fan-in, scatter-gather.

**Pourquoi Delta Lake plutôt que des fichiers Parquet ?**
Transactions ACID (écritures concurrentes du chemin rapide et du chemin lent), écritures
idempotentes, contraintes `CHECK`, `appendOnly` pour l'audit, historique et *time travel*,
compactage (`OPTIMIZE`). Avec des Parquet bruts, rien de cela.

**Limites connues ?**
- Les cycles de plus de 5 sauts ne sont pas recherchés (coût combinatoire).
- Le score est à base de règles : un modèle supervisé (GNN, gradient boosting sur les
  métriques de graphe) pourrait l'améliorer avec des données étiquetées.
- Une fraude étalée sur plus de 10 minutes échappe au chemin rapide (le chemin lent et
  les métriques de compte restent pertinents).
