# Checklist d'Évaluation - WeatherBot (Audit v2.5.1)

Ce document définit les critères de passage du mode Paper au mode Live. Les critères sont strictement factuels et exigent une validation statistique hors-échantillon.

## 1. Cadre d'Audit & Intégrité
- [x] **Anti-Leakage** : Scanner automatisé validant la séparation temporelle features/labels.
- [x] **Reproductibilité** : Hash de code et de configuration présent dans chaque rapport d'audit.
- [x] **Calibration** : Audit Brier Score et Log Loss implémenté.
- [x] **Benchmark** : Performance systématiquement comparée à une baseline naïve et aléatoire.

## 2. Gestion du Risque (Solo Desk)
- [x] **Exposure Caps** : Limites strictes par ville et par portefeuille global.
- [x] **Clusters Régionaux** : Limites d'exposition par zones géographiques (Europe, US East, Pacific, etc.).
- [x] **Indice de Diversification** : Suivi via l'indice HHI (Herfindahl-Hirschman).
- [x] **Stress Testing** : Simulation de chocs de liquidité et de scénarios "Black Swan".

## 3. Critères Go/No-Go (Validation Empirique)
*Ces critères doivent être maintenus sur une période de 30 à 60 jours en Paper Trading. Etat actuel au 2026-05-06 : No-Go live.*

- [ ] **Volume dataset** : >= 200 décisions et >= 200 lignes résolues.
- [ ] **Volume trading** : > 100 trades résolus en conditions réelles (Paper).
- [ ] **Profit Factor** : > 1.20 (net de frais et slippage simulés).
- [ ] **Max Drawdown** : < 10% du capital total alloué.
- [ ] **Stabilité Opérationnelle** : Uptime > 99.5% sur 30 jours glissants.
- [ ] **Drift Monitoring** : Statut "Stable" sur les 7 derniers jours glissants.
- [ ] **Significativité** : P-value < 0.05 sur l'outperformance du benchmark.
- [ ] **Calibration** : `calibration_fitted=yes` et Brier/LogLoss acceptés en holdout.
- [ ] **Ouroboros** : `Autoimprovement ready=yes` avec retrain/rollback validés.
- [ ] **Backtest ranking** : Top-K non négatif face aux benchmarks naïf et aléatoire.
- [ ] **Edge net positif** : les signaux exécutés doivent avoir `net_ev > 0` après frais et slippage estimés.
- [ ] **Accounting paper cohérent** : stake verrouillé uniquement à l'entrée, frais/slippage comptés dans le PnL de résolution.

## 4. Chantiers Prioritaires (v3.0)
- [x] Clusters de risque appris (K-means sur résidus).
- [ ] Walk-forward rolling automatisé.
- [ ] Gouvernance de retraining versionnée.
- [ ] Calibration holdout avec volume suffisant avant augmentation des tailles.

---
**Posture Finale :** Le système dispose d’un cadre d’audit sérieux et crédible. Il est prêt pour une phase prolongée de paper trading monitorée. Avant toute exposition à du capital réel, il reste nécessaire de confirmer la robustesse multi-régimes, la stabilité opérationnelle, la gestion du risque portefeuille et la persistance statistique de la performance.
