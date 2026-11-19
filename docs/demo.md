# Script de la démo (7 minutes)

## Avant la soutenance (J-1 et H-30 min)

1. Docker Desktop démarré, **au moins 6 Go de RAM** alloués (Settings → Resources).
2. `.\scripts\demo.ps1 start` **20 minutes avant** : le graphe glissant se remplit, le chemin
   lent a tourné plusieurs fois, le dashboard a de l'historique.
3. Ouvrir trois onglets : dashboard `http://localhost:8501`, Spark UI `http://localhost:4040`,
   et un terminal PowerShell à la racine du projet.
4. Fermer les applications gourmandes (navigateur avec 40 onglets, Teams…).
5. Avoir la **vidéo de secours** prête à lancer (voir en bas).

## Déroulé

| Temps | Action | Ce qu'on dit |
|---|---|---|
| 0:00 | Slide architecture | « Kafka, Spark Structured Streaming, GraphFrames, Delta Lake : deux vitesses de détection. » |
| 0:45 | Dashboard, onglet **Vue d'ensemble** | « 50 transactions par seconde, fenêtre glissante de 10 minutes. Le réseau suspect est vide ou presque. » |
| 1:15 | Barre latérale → **Carrousel 3 comptes** → *Injecter maintenant* | « J'injecte un carrousel : 3 sociétés, l'argent revient au point de départ avec 2 % de commission à chaque étape. » |
| 1:30 | Attendre la notification 🚨 et le ✅ dans la barre latérale | « Détecté en quelques secondes, au moment où le 3e virement ferme la boucle. » |
| 2:00 | Onglet **Alertes & investigation** → sélectionner l'alerte | « Chaque point du score est expliqué en français. » Montrer le sous-graphe PyVis (★ origine). |
| 2:40 | Bouton **Fiche d'investigation (PDF)** | « L'analyste conformité repart avec une fiche prête à signer. » |
| 3:00 | Injecter **Pivot au centre de 3 carrousels** | « Plus subtil : un compte au centre de trois boucles. » |
| 3:15 | Pendant l'attente : Spark UI → onglet *Structured Streaming* | « Trois requêtes : ingestion à la seconde, trafic fenêtré, analyses globales toutes les 90 s. » |
| 4:00 | Retour dashboard : 3 alertes CYCLE puis **PIVOT** + **CLUSTER** | « PageRank et Connected Components relient les trois boucles en un seul réseau. » |
| 4:30 | Onglet **Explorateur de réseau**, compte pivot, voisinage 2 sauts ; graphique PageRank | « PageRank seul ne suffit pas : en bleu, des commerçants légitimes ; en rouge, notre pivot. » |
| 5:00 | Terminal : `.\scripts\demo.ps1 crash` | « Je tue Spark brutalement, en pleine ingestion. » |
| 6:00 | Résultat du contrôle d'intégrité : **0 doublon** | « Checkpoints, écritures Delta idempotentes, alert_id déterministe : ni perte ni doublon. » |
| 6:20 | Onglet **Audit Delta Lake** : partitions `risk_level/dt`, historique, time travel | « Les alertes sont en appendOnly : une piste d'audit qu'on ne peut pas falsifier. » |
| 6:40 | Slide résultats (rappel, précision, latence, benchmark) | « Et voici ce que ça donne, mesuré contre la vérité terrain. » |

## Si quelque chose se passe mal

| Symptôme | Réaction |
|---|---|
| Pas d'alerte après 30 s | Onglet *Santé du pipeline* : vérifier que l'ingestion tourne ; sinon lancer la vidéo. |
| Dashboard figé | Désactiver/réactiver « Rafraîchissement temps réel » dans la barre latérale. |
| Spark ne redémarre pas après le crash | `docker compose up -d spark-streaming` (il reprendra où il en était). |
| Machine saturée | `docker compose stop producer` puis injecter les fraudes à la main : la détection reste instantanée. |

## Vidéo de secours

Enregistrer la démo complète la veille avec la barre de jeu Windows (**Win + Alt + R**) ou
OBS Studio, en 1080p. La garder hors ligne sur le bureau et sur une clé USB.
