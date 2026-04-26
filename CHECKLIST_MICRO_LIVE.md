# Checklist Micro-Live

Cette checklist définit les critères stricts pour passer du mode Paper au mode Micro-Live.

## Critères Obligatoires

- [ ] **100+ resolved rows** exploitables dans le dataset
- [ ] **14 jours paper verts** consécutifs (ou équivalent en trades)
- [ ] **Calibration fitted = yes** (Brier score acceptable)
- [ ] **Hyperopt accepted = yes** (ou rejected avec reason documentée)
- [ ] **Top-K > baseline** dans ranking backtest (outperformance ≥ 0)
- [ ] **Max Drawdown < 10%** en conditions paper
- [ ] **Win rate >= 30%** en conditions paper
- [ ] **Telegram alerts OK** (testé et fonctionnel)
- [ ] **MAX_LIVE_BET_USD=10** configuré (hard cap)
- [ ] **Auto-restart configuré** (systemd ou tmux)

## Critères Optionnels (Recommandés)

- [ ] **30+ trades résolus** en paper
- [ ] **Profit Factor > 1.2** (net de frais)
- [ ] **P-value < 0.05** sur outperformance
- [ ] **Stabilité opérationnelle > 99%** uptime sur 14 jours
- [ ] **Drift monitoring = stable** sur 7 jours

## Roadmap Micro-Live

### Phase 1: Micro-Live Initial
| Paramètre | Valeur |
|----------|-------|
| Taille max/trade | $10 |
| Exposure total max | $50 |
| Durée | 2-4 semaines |

### Phase 2: Validation
| Paramètre | Valeur |
|----------|-------|
| Taille max/trade | $25 |
| Duration | jusqu'à validation |

### Phase 3: Edge Confirmé
| Paramètre | Valeur |
|----------|-------|
| Taille max/trade | $50 |
| Duration | après validation stricte |

### Phase 4: Scaling Réel
| Paramètre | Valeur |
|----------|-------|
| Taille max/trade | $100+ |
| Condition | centaines de trades + edge confirmé |

---

## Règles d'Or Micro-Live

1. **NE JAMAIS dépasser MAX_LIVE_BET_USD=10** au début
2. **Toujours utiliser Kelly réduit (10%)** comme base
3. **Surveiller le drawdown quotidiennement**
4. **Arrêter si drawdown > 15%**
5. **Documenter chaque trade** dans les logs
6. **Ne pas lever le cap avant 14+ jours de validation**

---

## Posture Finale

Le système est prét pour Micro-Live uniquement si TOUS les critères obligatoires sont satisfaits.