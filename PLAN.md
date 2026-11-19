# Feuille de route

| Phase | Contenu | État |
|---|---|---|
| 0. Socle | Docker Compose (Kafka KRaft, Spark, dashboard), image Java 17 + jars hors ligne, git | ✅ |
| 1. Producteur | Population réaliste, 5 typologies (IBM AML), vérité terrain, retards, messages invalides | ✅ |
| 2. Schéma Delta | Médaillon, `risk_level/dt`, contraintes `CHECK`, `appendOnly` | ✅ |
| 3. Détection de cycles | Motif GraphFrames incrémental, filtre temporel, score expliqué, alertes Kafka | ✅ |
| 4. PageRank & composantes | PageRank, SCC + élagage, cycles 4-5, mules, scatter-gather, pivots, CC | ✅ |
| 5. Dashboard | Temps réel, injection, sous-graphes PyVis, PDF, santé, time travel | ✅ |
| 6. Évaluation | Précision 99 %, rappel 93 %, carrousel en 3,9 s, ablation, benchmark, test de panne | ✅ |
| 7. Présentation | README, rapport PDF, diaporama, script de démo, questions du jury, CI | ✅ |
| — | Vidéo de secours de la démo | ⬜ à enregistrer (Win + Alt + R) |
| — | Captures d'écran du dashboard (docs/img) | ✅ |
| — | Noms, encadrant et date dans le rapport et la page de garde du diaporama | ⬜ à compléter |

## Avant la soutenance

1. `.\scripts\demo.ps1 reset` puis `.\scripts\demo.ps1 start` 20 minutes avant.
2. Répéter la démo avec [docs/demo.md](docs/demo.md) (chronométrer : 7 minutes).
3. Relire [docs/questions_jury.md](docs/questions_jury.md).
4. Relancer `.\scripts\demo.ps1 evaluate` pour des chiffres frais.
