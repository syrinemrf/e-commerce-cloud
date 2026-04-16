# Guide SQL — E-Commerce Analytics BigQuery

Ce document explique chaque requête SQL du projet avec son objectif métier et une description ligne par ligne.

---

## Fichier 01 — Création des tables

### Table `clients`

**Objectif** : Stocker les informations maîtres des clients avec partitionnement par date d'inscription pour optimiser les requêtes de segmentation temporelle.

```sql
CREATE OR REPLACE TABLE `ecommerce_analytics.clients`
(
  client_id         STRING,
  last_name         STRING,
  first_name        STRING,
  email             STRING,
  age               INT64,
  gender            STRING,
  country           STRING,
  city              STRING,
  phone             STRING,
  registration_date DATETIME,
  segment           STRING
)
PARTITION BY DATE(registration_date)
CLUSTER BY country, segment
```

- `PARTITION BY DATE(registration_date)` : BigQuery découpe la table en partitions journalières. Toute requête avec un filtre `WHERE DATE(registration_date) >= ...` n'analyse que les partitions concernées, réduisant les coûts de lecture de 90–99%.
- `CLUSTER BY country, segment` : au sein de chaque partition, les données sont physiquement triées par `country` puis `segment`. Cela accélère les GROUP BY et WHERE sur ces colonnes.

---

## Fichier 02 — Vues analytiques

### Vue 1 : `v_revenue_by_region`

**Objectif métier** : Suivre l'évolution mensuelle du chiffre d'affaires par région géographique, calculer la part de marché de chaque région et détecter la croissance mois sur mois.

```sql
WITH
  base AS (
    SELECT
      region,
      DATE_TRUNC(DATE(order_date), MONTH) AS order_month,
      SUM(total_amount)                   AS monthly_revenue,
      COUNT(*)                            AS order_count
    FROM `ecommerce_analytics.orders`
    WHERE
      status != 'Cancelled'
      AND DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH)
    GROUP BY region, order_month
  ),
  ...
```

- `DATE_TRUNC(..., MONTH)` : regroupe toutes les commandes au 1er du mois pour agréger par mois.
- `WHERE status != 'Cancelled'` : exclut les commandes annulées du chiffre d'affaires.
- `DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH)` : filtre sur la colonne de partition — BigQuery ne scanne que les 13 derniers mois de données.
- `global_rev` CTE : calcule le revenu total global pour le calcul des pourcentages.
- `SAFE_DIVIDE(...) * 100` : division sécurisée (retourne NULL si diviseur = 0) multipliée par 100 pour obtenir un pourcentage.
- `LAG(b.monthly_revenue) OVER (PARTITION BY b.region ORDER BY b.order_month)` : fonction de fenêtre qui récupère le revenu du mois précédent pour le même groupe `region`, permettant de calculer la croissance mois sur mois.

---

### Vue 2 : `v_inactive_clients`

**Objectif métier** : Identifier les clients qui n'ont pas passé de commande depuis 60 jours pour déclencher des campagnes de réactivation ciblées. Triés par revenu historique décroissant pour prioriser les plus rentables.

```sql
WITH
  last_order AS (
    SELECT
      client_id,
      MAX(DATE(order_date)) AS last_purchase_date,
      SUM(total_amount)     AS historical_revenue,
      COUNT(*)              AS order_count
    FROM `ecommerce_analytics.orders`
    WHERE DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
    GROUP BY client_id
  ),
  ...
WHERE lo.last_purchase_date < DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
ORDER BY lo.historical_revenue DESC
```

- `MAX(DATE(order_date))` : dernière date de commande par client.
- `DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)` : seuil d'inactivité à 60 jours glissants.
- `LEFT JOIN incident_counts` : joint les incidents pour enrichir le profil client sans exclure les clients sans incident.
- `ORDER BY lo.historical_revenue DESC` : les clients les plus rentables apparaissent en premier pour prioriser les actions commerciales.

---

### Vue 3 : `v_top_products`

**Objectif métier** : Identifier les produits les plus performants par catégorie et calculer leur taux d'annulation pour détecter d'éventuels problèmes qualité.

```sql
RANK() OVER (PARTITION BY category ORDER BY revenue DESC) AS category_rank
```

- `RANK() OVER (PARTITION BY category ...)` : calcule le classement du produit au sein de sa catégorie. Si deux produits ont le même revenu, ils reçoivent le même rang.
- `SAFE_DIVIDE(cancelled_count, NULLIF(total_items, 0)) * 100` : taux d'annulation en pourcentage. `NULLIF(..., 0)` évite la division par zéro.

---

### Vue 4 : `v_recurring_incidents`

**Objectif métier** : Analyser les incidents récurrents par catégorie pour identifier les points douloureux du parcours client et mesurer l'efficacité du service client (taux d'escalade, temps de résolution).

```sql
SAFE_DIVIDE(COUNTIF(status = 'Escalated'), COUNT(*)) * 100      AS pct_escalated,
SAFE_DIVIDE(COUNTIF(priority = 'Critical'), COUNT(*)) * 100     AS pct_critical,
SAFE_DIVIDE(COUNTIF(order_id IS NOT NULL), COUNT(*)) * 100      AS pct_linked_to_order
```

- `COUNTIF(status = 'Escalated')` : compte uniquement les lignes où la condition est vraie (syntaxe BigQuery).
- `pct_linked_to_order` : permet de mesurer la proportion d'incidents directement liés à une commande (vs. problèmes non transactionnels comme les problèmes de connexion).

---

### Vue 5 : `v_navigation_funnel`

**Objectif métier** : Mesurer l'engagement des visiteurs page par page et calculer un score d'engagement combinant durée et volume de sessions.

```sql
ROUND(AVG(duration_seconds) * COUNT(*) / 1000.0, 4) AS engagement_score
```

- `engagement_score` : métrique composite — une page avec beaucoup de sessions courtes peut avoir le même score qu'une page avec peu de sessions longues. Permet de comparer des pages de nature différente.
- `COUNT(DISTINCT client_id)` : compte les utilisateurs uniques identifiés (exclut les visiteurs anonymes dont `client_id` est NULL).

---

### Vue 6 : `v_weekly_kpis`

**Objectif métier** : Tableau de bord hebdomadaire avec revenu, commandes, nouveaux clients, incidents et delta semaine sur semaine pour le suivi opérationnel.

```sql
ow.revenue - LAG(ow.revenue) OVER (ORDER BY ow.week) AS wow_revenue_delta
```

- `DATE_TRUNC(DATE(order_date), WEEK(MONDAY))` : regroupe par semaine ISO commençant le lundi.
- `LAG(ow.revenue) OVER (ORDER BY ow.week)` : récupère le revenu de la semaine précédente pour calculer le delta absolu (en euros).
- `LEFT JOIN` sur les trois CTE : certaines semaines peuvent ne pas avoir de nouveaux clients ou d'incidents — `COALESCE(..., 0)` remplace les NULLs par 0.

---

### Vue 7 : `v_client_360`

**Objectif métier** : Vue consolidée à 360° de chaque client combinant comportement d'achat, historique d'incidents et navigation web. Le score de valeur permet de segmenter les clients en VIP / Regular / At risk.

```sql
ROUND(
  COALESCE(os.total_revenue, 0) * 0.5
  + SAFE_DIVIDE(1.0, NULLIF(COALESCE(is2.incident_count, 0), 0)) * 20
  + COALESCE(os.order_count, 0) * 2,
  2
) AS value_score
```

- `total_revenue * 0.5` : le revenu contribue à 50% du score (pondération principale).
- `SAFE_DIVIDE(1.0, incident_count) * 20` : terme inversement proportionnel aux incidents — un client sans incident reçoit un bonus infini tronqué par le SAFE_DIVIDE. Un client avec beaucoup d'incidents voit ce terme tendre vers 0.
- `order_count * 2` : prime pour la fidélité (nombre de commandes).
- `APPROX_TOP_COUNT(page, 1)[OFFSET(0)].value` : fonction d'agrégation approximative qui retourne le top-1 de la colonne `page` (page favorite). Utilise une structure `[{value, count}]`.

---

## Fichier 03 — Advanced analytics

### Requête 1 : Segmentation RFM

**Objectif** : Classer les clients selon leur Récence (R), Fréquence (F) et Valeur Monétaire (M) pour identifier les segments Champions, Loyal, At Risk et Lost.

```sql
NTILE(4) OVER (ORDER BY recency_days    ASC)  AS r_score,  -- lower = better
NTILE(4) OVER (ORDER BY frequency       DESC) AS f_score,
NTILE(4) OVER (ORDER BY monetary        DESC) AS m_score
```

- `NTILE(4)` : divise les clients en 4 quartiles. Le quartile 4 = meilleurs clients selon le critère.
- `recency_days ASC` : pour la récence, un client récent (faibles jours) reçoit un score élevé, donc on trie ASC.
- `frequency DESC` + `monetary DESC` : plus de commandes et plus de revenu = meilleur score, donc tri DESC.
- Score composite : `r_score + f_score + m_score` va de 3 (pires) à 12 (Champions).

---

### Requête 2 : Analyse de cohorte mensuelle

**Objectif** : Suivre le comportement d'achat des clients groupés par mois de première inscription sur 12 mois pour mesurer la rétention et le LTV (Lifetime Value).

```sql
DATE_DIFF(
  DATE_TRUNC(DATE(o.order_date), MONTH),
  c.cohort_month,
  MONTH
) AS month_index
```

- `cohort_month` : mois d'inscription du client — c'est la cohorte de référence.
- `month_index` : nombre de mois écoulés entre l'inscription (0) et la commande. Permet de tracer des courbes de rétention (M0, M1, M2...).

---

### Requête 3 : Tendance rolling 4 semaines

**Objectif** : Lisser les variations hebdomadaires du revenu en calculant une moyenne mobile sur 4 semaines glissantes.

```sql
AVG(weekly_revenue) OVER (
  ORDER BY week
  ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
) AS rolling_4w_avg_revenue
```

- `ROWS BETWEEN 3 PRECEDING AND CURRENT ROW` : fenêtre glissante de 4 lignes (la ligne courante + les 3 précédentes = 4 semaines).
- Cette métrique lisse les pics de Black Friday ou creux du mois d'août pour révéler la tendance de fond.

---

### Requête 4 : Détection d'anomalies régionales

**Objectif** : Identifier les commandes dont le montant est anormal (> moyenne + 2 écarts-types) par région pour détecter des fraudes ou erreurs de saisie.

```sql
STDDEV_POP(total_amount) OVER (PARTITION BY region) AS stddev_amount
...
WHERE o.total_amount > s.mean_amount + 2 * s.stddev_amount
```

- `STDDEV_POP` : écart-type de population (sur toutes les valeurs, non échantillon).
- `mean + 2 * stddev` : seuil statistique standard (règle empirique des 2σ — couvre ~95% de la distribution normale). Les 5% restants sont des anomalies potentielles.
- `z_score` : score standardisé = `(valeur - moyenne) / écart-type`. Un z_score > 2 confirme l'anomalie.

---

### Requête 5 : Entonnoir de conversion navigation

**Objectif** : Calculer les taux de conversion entre les étapes clés du parcours d'achat : `/products` → `/cart` → `/checkout`.

```sql
SAFE_DIVIDE(cart_sessions,     NULLIF(products_sessions, 0)) * 100 AS pct_products_to_cart,
SAFE_DIVIDE(checkout_sessions, NULLIF(cart_sessions, 0))     * 100 AS pct_cart_to_checkout,
SAFE_DIVIDE(checkout_sessions, NULLIF(products_sessions, 0)) * 100 AS overall_conversion_pct
```

- `pct_products_to_cart` : % des visiteurs de /products qui ont cliqué sur /cart (ajout au panier).
- `pct_cart_to_checkout` : % des visiteurs de /cart qui ont initié le paiement.
- `overall_conversion_pct` : taux de conversion global de la découverte au paiement.
- La structure en CTEs `page_agg → funnel` permet de pivoter les sessions par page en colonnes.

---

## Fichier 04 — Scheduled Queries

### Query 1 : `kpis_daily`

**Objectif** : Rafraîchir chaque matin la table de KPIs quotidiens en ne scannant que les 2 derniers jours de partitions (économie de lecture maximale).

```sql
WHERE DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
```

- Filtre sur la colonne de partition → BigQuery scanne seulement 2 jours de données au lieu de 2+ ans.
- `WRITE_TRUNCATE` : remplace entièrement la table de destination à chaque exécution.

### Query 2 : `rfm_weekly`

**Objectif** : Mettre à jour la segmentation RFM chaque lundi en se basant sur les 90 derniers jours de commandes pour capturer l'évolution comportementale récente.

```sql
WHERE
  status != 'Cancelled'
  AND DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
```

- Fenêtre de 90 jours : capture les 3 derniers mois d'activité, suffisant pour une segmentation RFM pertinente sans scanner toute l'historique.
- `CURRENT_DATETIME() AS computed_at` : timestamp de calcul pour traçabilité.
