# Étude d'ablation — détection des carrousels

_90323 transactions analysées (30 dernières minutes du run d'évaluation), fenêtres de 20 min décalées de 10 min._

| Étape | Cycles retenus | Vrais carrousels | Précision | Carrousels retrouvés |
|---|---:|---:|---:|---:|
| Motif seul (a)->(b)->(c)->(a) | 233 | 68 | 29.2 % | 36 |
| + ordre temporel et durée ≤ 10 min | 91 | 68 | 74.7 % | 36 |
| + score ≥ MEDIUM (montants, vitesse…) | 69 | 68 | 98.6 % | 36 |

Chaque filtre retire des faux positifs sans perdre un seul vrai carrousel.
