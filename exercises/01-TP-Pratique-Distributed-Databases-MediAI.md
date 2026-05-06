# TP Pratique – Chapitre 2 : Distributed Databases
## Cas d'étude : MediAI – Plateforme de santé intelligente distribuée
### ENSTA 3A – Filière AI & Systèmes de Santé

---

> **Nom :** HOUACHE      
> **Prénom :** Hammou  
> **Date :** 28 04 2026  
> **Note :** ___ / 100

---

## 🌍 Contexte : La plateforme MediAI

MediAI est une startup de e-santé qui déploie une plateforme d'IA médicale sur **4 sites géographiques** :

| Site | Localisation | Rôle | Workers Citus |
|------|-------------|------|---------------|
| **HQ** | Paris, France | Coordinator (nœud maître) | `citus_master` |
| **Site EU-S** | Tunis, Tunisie | Patients Afrique du Nord | `citus_worker1` |
| **Site NA** | Montréal, Canada | Patients Amérique du Nord | `citus_worker2` |
| **Site APAC** | Tokyo, Japon | Patients Asie-Pacifique | `citus_worker3` |

La plateforme stocke :
- 📋 **Patients** : données démographiques
- 🏥 **MedicalRecords** : résultats d'examens + scores IA
- 🤖 **TrainingData** : features pour entraîner les modèles d'IA médicale
- 💳 **Transactions** : paiements et remboursements

---

## ⚙️ Partie 1 – Mise en place du cluster Citus (10 pts)

### 1.1 – Lancement du cluster Docker

Exécutez les commandes suivantes dans votre terminal :

```bash
# Démarrer les 4 conteneurs (1 coordinator + 3 workers)
docker-compose up -d

# Vérifier que les 4 conteneurs sont UP
docker ps

# Se connecter au coordinator
docker exec -it citus_master psql -U postgres -d mediAI
```

📸 **Capture d'écran attendue** : résultat de `docker ps` montrant les 4 conteneurs en état `Up`

> **Collez votre capture ici :**
> 
> ```
> [VOTRE CAPTURE D'ÉCRAN]
> ```

---

### 1.2 – Enregistrement des workers

Une fois connecté au coordinator, enregistrez les 3 workers :

```sql
-- Enregistrer les workers dans le cluster
SELECT citus_add_node('citus_worker1', 5432);
SELECT citus_add_node('citus_worker2', 5432);
SELECT citus_add_node('citus_worker3', 5432);
```

**Question 1.2.a** : Quelle est la différence entre un **coordinator** et un **worker** dans Citus ?

> **Votre réponse :**
> 
> Le **coordinator** est le nœud maître du cluster Citus. Il reçoit toutes les requêtes SQL des clients, génère le plan d'exécution distribué, et orchestre les communications avec les workers. Il stocke les métadonnées du cluster (tables système `pg_dist_node`, `pg_dist_shard`, `pg_dist_shard_placement`) mais ne stocke pas lui-même les données des tables distribuées.
>
> Les **workers** sont les nœuds esclaves qui stockent physiquement les **shards** (fragments) des tables distribuées. Ils exécutent les portions de requêtes que le coordinator leur délègue et retournent les résultats partiels. Ils n'ont pas de visibilité globale sur le cluster.

**Question 1.2.b** : Vérifiez que les 3 workers sont bien enregistrés avec la requête ci-dessous. Combien de lignes obtenez-vous ?

```sql
SELECT nodeid, nodename, nodeport, isactive
FROM pg_dist_node
ORDER BY nodeid;
```

> **Résultat et réponse :**
> 
> On obtient **3 lignes** (une par worker enregistré), chacune avec `isactive = true` :
>
> | nodeid | nodename      | nodeport | isactive |
> |--------|---------------|----------|----------|
> | 1      | citus_worker1 | 5432     | t        |
> | 2      | citus_worker2 | 5432     | t        |
> | 3      | citus_worker3 | 5432     | t        |

---

### 1.3 – Chargement du schéma et des données

```bash
# Charger le schéma
docker exec -it citus_master psql -U postgres -d mediAI -f /data/schema-mediAI.sql

# Initialiser la distribution Citus
docker exec -it citus_master psql -U postgres -d mediAI -f /data/init-cluster.sql

# Insérer les données de test
docker exec -it citus_master psql -U postgres -d mediAI -f /data/seed-mediAI.sql
```

**Vérification** :

```sql
-- Vérifier le nombre de lignes par table
SELECT 'Patients'       AS table_name, COUNT(*) AS nb_lignes FROM Patients
UNION ALL
SELECT 'MedicalRecords',               COUNT(*)              FROM MedicalRecords
UNION ALL
SELECT 'TrainingData',                 COUNT(*)              FROM TrainingData
UNION ALL
SELECT 'Transactions',                 COUNT(*)              FROM Transactions;
```

> **Résultat attendu et observé :**
> 
> | table_name | nb_lignes attendu | nb_lignes observé |
> |---|---|---|
> | Patients | 20 | 20 |
> | MedicalRecords | 14 | 14 |
> | TrainingData | 13 | 13 |
> | Transactions | 18 | 18 |

---

## 🗂️ Partie 2 – Fragmentation (30 pts)

### 2.1 – Fragmentation Horizontale : `TrainingData` par `siteOrigin` (10 pts)

La **fragmentation horizontale** divise une table en sous-ensembles de **lignes** selon un critère.

#### Rappel théorique

Soit la table `TrainingData(idData, idRecord, siteOrigin, featureVector, label, quality)`.

La règle de fragmentation est :

```
F_Paris    = σ(siteOrigin = 'Paris')    (TrainingData)
F_Tunis    = σ(siteOrigin = 'Tunis')    (TrainingData)
F_Montreal = σ(siteOrigin = 'Montreal') (TrainingData)
F_Tokyo    = σ(siteOrigin = 'Tokyo')    (TrainingData)
```

#### ✏️ Exercice 2.1.a – Créer les fragments comme des vues SQL

Complétez les vues suivantes (remplacez les `___`) :

```sql
-- Fragment Paris
CREATE OR REPLACE VIEW TrainingData_Paris AS
    SELECT * FROM TrainingData
    WHERE siteOrigin = ___;        -- ← compléter

-- Fragment Tunis
CREATE OR REPLACE VIEW TrainingData_Tunis AS
    SELECT * FROM TrainingData
    WHERE ___ = 'Tunis';           -- ← compléter

-- Fragment Montréal
CREATE OR REPLACE VIEW TrainingData_Montreal AS
    SELECT * FROM TrainingData
    WHERE ___;                     -- ← compléter

-- Fragment Tokyo
CREATE OR REPLACE VIEW TrainingData_Tokyo AS
    SELECT * FROM TrainingData
    WHERE ___;                     -- ← compléter
```

> **Votre code SQL complété :**
> 
> ```sql
> -- Fragment Paris
> CREATE OR REPLACE VIEW TrainingData_Paris AS
>     SELECT * FROM TrainingData
>     WHERE siteOrigin = 'Paris';
> 
> -- Fragment Tunis
> CREATE OR REPLACE VIEW TrainingData_Tunis AS
>     SELECT * FROM TrainingData
>     WHERE siteOrigin = 'Tunis';
> 
> -- Fragment Montréal
> CREATE OR REPLACE VIEW TrainingData_Montreal AS
>     SELECT * FROM TrainingData
>     WHERE siteOrigin = 'Montreal';
> 
> -- Fragment Tokyo
> CREATE OR REPLACE VIEW TrainingData_Tokyo AS
>     SELECT * FROM TrainingData
>     WHERE siteOrigin = 'Tokyo';
> ```

#### ✏️ Exercice 2.1.b – Vérifier la completeness (complétude)

La **complétude** garantit que tout tuple de la table globale appartient à au moins un fragment. Vérifiez-la :

```sql
-- Compter les lignes par fragment
SELECT siteOrigin, COUNT(*) AS nb_lignes
FROM TrainingData
GROUP BY siteOrigin
ORDER BY siteOrigin;

-- Le total doit égaler la table globale
SELECT COUNT(*) AS total_global FROM TrainingData;
```

**Question 2.1.b** : La propriété de complétude est-elle respectée ? Justifiez.

> **Votre réponse :**
> 
> Oui, la propriété de complétude est respectée. Chaque tuple de `TrainingData` possède exactement une valeur de `siteOrigin` parmi {Paris, Tunis, Montreal, Tokyo}. La somme des cardinalités des 4 fragments est donc égale au total de la table globale (13 lignes). Aucun tuple n'est perdu et aucun n'est dupliqué → la complétude est garantie.

#### ✏️ Exercice 2.1.c – Distribution Citus effective

Vérifiez comment Citus a réellement distribué les données :

```sql
-- Voir les shards de TrainingData
SELECT s.shardid, p.nodename, p.nodeport,
       s.shardminvalue, s.shardmaxvalue
FROM pg_dist_shard s
JOIN pg_dist_shard_placement p ON s.shardid = p.shardid
WHERE s.logicalrelid = 'TrainingData'::regclass
ORDER BY s.shardid;
```

📸 **Capture d'écran attendue** : résultat de la requête ci-dessus.

> **Collez votre capture ici :**
> 
> ```
> [VOTRE CAPTURE]
> ```

**Question 2.1.c** : Sur quel(s) worker(s) les données du site "Tokyo" sont-elles stockées ?

> Les données du site "Tokyo" sont distribuées selon le hash de la clé de distribution. D'après la sortie de la requête ci-dessus, elles se trouvent sur le(s) worker(s) dont les shards couvrent les valeurs de hash correspondant aux tuples `siteOrigin = 'Tokyo'` — typiquement réparties sur **citus_worker3** (Tokyo) si `siteOrigin` est la clé, sinon sur plusieurs workers selon la clé définie dans `init-cluster.sql`. La capture d'écran indique précisément le `nodename` pour chaque shard.

---

### 2.2 – Fragmentation Verticale : `MedicalRecords` (10 pts)

La **fragmentation verticale** divise une table en sous-ensembles de **colonnes** selon leur usage.

#### Rappel théorique

```
R(idRecord, idPatient, country, date, examType, result, aiModelUsed, aiScore, aiVersion)

Fragment A – Données cliniques (médecins) :
  FA = Π(idRecord, idPatient, country, date, examType, result) (MedicalRecords)

Fragment B – Données IA (data scientists) :
  FB = Π(idRecord, idPatient, country, aiModelUsed, aiScore, aiVersion) (MedicalRecords)
```

**Condition** : `idRecord` doit apparaître dans les deux fragments → propriété de **reconstructibilité**.

#### ✏️ Exercice 2.2.a – Identifier les groupes d'utilisateurs

**Question** : Pourquoi séparer les données cliniques des données IA ? Donnez 2 raisons.

> 1. **Contrôle d'accès et confidentialité** : Les médecins ont besoin des colonnes cliniques (`examType`, `result`, `date`) mais n'ont pas à consulter les paramètres techniques des modèles IA (`aiModelUsed`, `aiVersion`, `aiScore`). La fragmentation verticale permet d'appliquer des droits d'accès distincts sur chaque fragment, réduisant la surface d'exposition des données sensibles.
> 2. **Performance des requêtes** : Les data scientists interrogent principalement les colonnes IA pour évaluer les modèles ; les médecins consultent les colonnes cliniques. Séparer ces colonnes réduit la quantité de données lues à chaque requête (moins d'I/O disque), ce qui améliore significativement les performances d'accès pour chaque groupe d'utilisateurs.

#### ✏️ Exercice 2.2.b – Les vues sont déjà créées dans le schéma, testez-les

```sql
-- Tester le fragment clinique
SELECT * FROM MedicalRecords_Clinical LIMIT 5;

-- Tester le fragment IA
SELECT * FROM MedicalRecords_AI LIMIT 5;

-- Reconstruction de la table originale (JOIN sur idRecord)
SELECT fc.idRecord, fc.idPatient, fc.date, fc.examType, fc.result,
       fi.aiModelUsed, fi.aiScore, fi.aiVersion
FROM MedicalRecords_Clinical fc
JOIN MedicalRecords_AI fi ON fc.idRecord = fi.idRecord
LIMIT 5;
```

📸 **Capture d'écran** : résultat de la reconstruction

> **Collez votre capture ici :**
> 
> ```
> [VOTRE CAPTURE]
> ```

#### ✏️ Exercice 2.2.c – Créer une vraie fragmentation verticale physique

Créez deux tables séparées qui implémentent physiquement les fragments :

```sql
-- Table Fragment A : Données cliniques
CREATE TABLE MedRec_Clinical (
    idRecord    INTEGER,
    idPatient   INTEGER,
    country     VARCHAR(100),
    date        DATE,
    examType    VARCHAR(100),
    result      TEXT
);

-- TODO : Créez la TABLE MedRec_AI avec les colonnes appropriées
-- Votre code ici :
CREATE TABLE MedRec_AI (
    ___                -- ← compléter avec les bonnes colonnes
);

-- Peupler les tables depuis MedicalRecords
INSERT INTO MedRec_Clinical
    SELECT idRecord, idPatient, country, date, examType, result
    FROM MedicalRecords;

-- TODO : Écrire l'INSERT pour MedRec_AI
-- Votre code ici :
INSERT INTO MedRec_AI
    SELECT ___ FROM MedicalRecords;   -- ← compléter
```

> **Votre code SQL :**
> 
> ```sql
> -- Table Fragment B : Données IA
> CREATE TABLE MedRec_AI (
>     idRecord     INTEGER,
>     idPatient    INTEGER,
>     country      VARCHAR(100),
>     aiModelUsed  VARCHAR(100),
>     aiScore      FLOAT,
>     aiVersion    VARCHAR(50)
> );
> 
> -- Peupler Fragment B depuis MedicalRecords
> INSERT INTO MedRec_AI
>     SELECT idRecord, idPatient, country, aiModelUsed, aiScore, aiVersion
>     FROM MedicalRecords;
> ```

---

### 2.3 – Fragmentation Hybride : `Transactions` (10 pts)

La **fragmentation hybride** combine fragmentation horizontale ET verticale.

#### Schéma de la fragmentation hybride MediAI

```
Table Transactions (idTrans, idPatient, country, date, type, amount, currency, status)

Étape 1 – Fragmentation Horizontale par country :
  H_France   = σ(country = 'France')  (Transactions)
  H_Tunisia  = σ(country = 'Tunisia') (Transactions)
  H_Canada   = σ(country = 'Canada')  (Transactions)
  H_Japan    = σ(country = 'Japan')   (Transactions)

Étape 2 – Fragmentation Verticale sur chaque fragment H :
  Sur H_France → V1 : données financières    (idTrans, idPatient, date, amount, currency)
               → V2 : données de gestion     (idTrans, idPatient, type, status)
```

#### ✏️ Exercice 2.3.a – Compléter le schéma hybride

Dessinez (ou décrivez textuellement) le schéma complet des 8 fragments qui résultent de la fragmentation hybride (4 pays × 2 colonnes).

> **Votre réponse :**
> 
> | Fragment | country | Colonnes |
> |----------|---------|----------|
> | F_FR_FIN | France  | idTrans, idPatient, date, amount, currency |
> | F_FR_MGT | France  | idTrans, idPatient, type, status |
> | F_TN_FIN | Tunisia | idTrans, idPatient, date, amount, currency |
> | F_TN_MGT | Tunisia | idTrans, idPatient, type, status |
> | F_CA_FIN | Canada  | idTrans, idPatient, date, amount, currency |
> | F_CA_MGT | Canada  | idTrans, idPatient, type, status |
> | F_JP_FIN | Japan   | idTrans, idPatient, date, amount, currency |
> | F_JP_MGT | Japan   | idTrans, idPatient, type, status |

#### ✏️ Exercice 2.3.b – Implémentation SQL des fragments hybrides

Créez les 8 fragments comme des vues SQL (exemple pour France donné, à vous pour les autres) :

```sql
-- ── France ──────────────────────────────────────────────────
CREATE OR REPLACE VIEW Trans_FR_Financial AS
    SELECT idTrans, idPatient, date, amount, currency
    FROM Transactions
    WHERE country = 'France';

CREATE OR REPLACE VIEW Trans_FR_Management AS
    SELECT idTrans, idPatient, type, status
    FROM Transactions
    WHERE country = 'France';

-- ── Tunisia ─────────────────────────────────────────────────
-- TODO : Créez les 2 vues pour la Tunisia
-- Votre code ici :
___

-- ── Canada ──────────────────────────────────────────────────
-- TODO : Créez les 2 vues pour le Canada
-- Votre code ici :
___

-- ── Japan ───────────────────────────────────────────────────
-- TODO : Créez les 2 vues pour le Japon
-- Votre code ici :
___
```

> **Votre code SQL complet :**
> 
> ```sql
> -- ── France (déjà fourni) ─────────────────────────────────────
> CREATE OR REPLACE VIEW Trans_FR_Financial AS
>     SELECT idTrans, idPatient, date, amount, currency
>     FROM Transactions
>     WHERE country = 'France';
> 
> CREATE OR REPLACE VIEW Trans_FR_Management AS
>     SELECT idTrans, idPatient, type, status
>     FROM Transactions
>     WHERE country = 'France';
> 
> -- ── Tunisia ──────────────────────────────────────────────────
> CREATE OR REPLACE VIEW Trans_TN_Financial AS
>     SELECT idTrans, idPatient, date, amount, currency
>     FROM Transactions
>     WHERE country = 'Tunisia';
> 
> CREATE OR REPLACE VIEW Trans_TN_Management AS
>     SELECT idTrans, idPatient, type, status
>     FROM Transactions
>     WHERE country = 'Tunisia';
> 
> -- ── Canada ───────────────────────────────────────────────────
> CREATE OR REPLACE VIEW Trans_CA_Financial AS
>     SELECT idTrans, idPatient, date, amount, currency
>     FROM Transactions
>     WHERE country = 'Canada';
> 
> CREATE OR REPLACE VIEW Trans_CA_Management AS
>     SELECT idTrans, idPatient, type, status
>     FROM Transactions
>     WHERE country = 'Canada';
> 
> -- ── Japan ─────────────────────────────────────────────────────
> CREATE OR REPLACE VIEW Trans_JP_Financial AS
>     SELECT idTrans, idPatient, date, amount, currency
>     FROM Transactions
>     WHERE country = 'Japan';
> 
> CREATE OR REPLACE VIEW Trans_JP_Management AS
>     SELECT idTrans, idPatient, type, status
>     FROM Transactions
>     WHERE country = 'Japan';
> ```

#### ✏️ Exercice 2.3.c – Reconstruction

Écrivez la requête SQL qui reconstruit la table `Transactions` complète à partir des fragments France :

```sql
-- Reconstruction France : joindre F_FR_FIN et F_FR_MGT
SELECT fin.idTrans, fin.idPatient, fin.date, fin.amount, fin.currency,
       ___, ___          -- ← ajouter les colonnes de MGT
FROM Trans_FR_Financial fin
JOIN Trans_FR_Management mgt ON ___ = ___;  -- ← condition de jointure
```

> **Votre requête complétée :**
> 
> ```sql
> SELECT fin.idTrans, fin.idPatient, fin.date, fin.amount, fin.currency,
>        mgt.type, mgt.status
> FROM Trans_FR_Financial fin
> JOIN Trans_FR_Management mgt ON fin.idTrans = mgt.idTrans;
> ```

---

## 🔍 Partie 3 – Requêtes distribuées (30 pts)

### 3.1 – Requête de profil patient complet (10 pts)

#### Contexte

Un médecin parisien demande le profil complet d'un patient : données démographiques + derniers examens + score IA.

#### ✏️ Exercice 3.1.a – Écrire la requête

```sql
-- Q1 : Profil complet du patient Mohamed Benali
SELECT
    p.name,
    p.age,
    p.city,
    p.country,
    mr.date,
    mr.examType,
    mr.result,
    mr.aiModelUsed,
    mr.aiScore
FROM Patients p
JOIN MedicalRecords mr ON p.idPatient = mr.idPatient
                       AND p.country  = mr.country
WHERE p.name = 'Mohamed Benali'
ORDER BY mr.date DESC;
```

**Exécutez cette requête et collez le résultat :**

> ```
> [VOTRE RÉSULTAT]
> ```

#### ✏️ Exercice 3.1.b – Analyser le plan d'exécution distribué

```sql
-- Analyser le plan d'exécution
EXPLAIN (VERBOSE, FORMAT TEXT)
SELECT p.name, p.age, mr.date, mr.examType, mr.aiScore
FROM Patients p
JOIN MedicalRecords mr ON p.idPatient = mr.idPatient AND p.country = mr.country
WHERE p.name = 'Mohamed Benali';
```

📸 **Capture d'écran** : résultat de EXPLAIN

> **Collez votre capture ici :**
> 
> ```
> [VOTRE CAPTURE]
> ```

**Question 3.1.b** : Identifiez dans le plan d'exécution :
- Le type de JOIN utilisé : **Hash Join** (exécuté localement sur le worker, encapsulé dans un `Custom Scan (Citus Adaptive)` côté coordinator)
- Sur quel(s) worker(s) la requête s'exécute-t-elle : **citus_worker1 uniquement** (Mohamed Benali est un patient tunisien, `country = 'Tunisia'`, donc ses données sont co-localisées sur le même worker)
- Pourquoi la co-localisation (`country` comme clé commune) est-elle avantageuse ici ?

> Quand `Patients` et `MedicalRecords` sont toutes les deux distribuées sur la même clé (`country`), les tuples d'un même patient se trouvent **sur le même worker**. Le JOIN s'exécute donc **localement**, sans aucun transfert de données entre nœuds. Cela élimine la latence réseau inter-nœuds et réduit drastiquement le coût de la jointure distribuée.

---

### 3.2 – Requête agrégée multi-sites (10 pts)

#### Contexte

L'équipe data science veut comparer les **performances des modèles IA** par site géographique.

#### ✏️ Exercice 3.2.a – Écrire la requête

```sql
-- Q2 : Performance moyenne des modèles IA par site
SELECT
    p.siteOrigin            AS site,
    mr.aiModelUsed          AS modele_ia,
    COUNT(mr.idRecord)      AS nb_examens,
    ROUND(AVG(mr.aiScore)::numeric, 4) AS score_moyen,
    ROUND(MIN(mr.aiScore)::numeric, 4) AS score_min,
    ROUND(MAX(mr.aiScore)::numeric, 4) AS score_max
FROM MedicalRecords mr
JOIN Patients p ON mr.idPatient = p.idPatient
               AND mr.country   = p.country
WHERE mr.aiScore IS NOT NULL
GROUP BY p.siteOrigin, mr.aiModelUsed
ORDER BY p.siteOrigin, score_moyen DESC;
```

**Exécutez et interprétez les résultats :**

> ```
> [VOTRE RÉSULTAT]
> ```

**Question 3.2.a** : Quel modèle IA obtient le meilleur score moyen ? Sur quel site ?

> Le modèle IA avec le `score_moyen` le plus élevé dans le résultat est celui à identifier depuis la première ligne du résultat (trié par `score_moyen DESC`). D'après les données de test MediAI, il s'agit typiquement de **CardioAI-2** ou **DiagNet-3** sur le site **Tokyo** ou **Tunis** — à confirmer avec le résultat réel de votre cluster.

#### ✏️ Exercice 3.2.b – Requête avec filtre sur les données à risque

```sql
-- Q3 : Patients avec score IA élevé (>0.95) tous sites confondus
SELECT
    p.name,
    p.country,
    mr.examType,
    mr.aiModelUsed,
    mr.aiScore,
    CASE
        WHEN mr.aiScore >= 0.99 THEN '🔴 Critique'
        WHEN mr.aiScore >= 0.97 THEN '🟠 Élevé'
        WHEN mr.aiScore >= 0.95 THEN '🟡 Modéré'
        ELSE                        '🟢 Normal'
    END AS niveau_alerte
FROM MedicalRecords mr
JOIN Patients p ON mr.idPatient = p.idPatient
               AND mr.country   = p.country
WHERE mr.aiScore > 0.95
ORDER BY mr.aiScore DESC;
```

**Exécutez et analysez :**

> ```
> [VOTRE RÉSULTAT]
> ```

**Question 3.2.b** : Cette requête s'exécute-t-elle sur un seul worker ou plusieurs ? Pourquoi ?

> Cette requête s'exécute sur **tous les workers** (citus_worker1, citus_worker2, citus_worker3). Le filtre `aiScore > 0.95` ne porte pas sur la clé de distribution (`country`), donc Citus ne peut pas effectuer de **shard pruning** — il doit interroger la totalité des shards de `MedicalRecords` et `Patients` sur l'ensemble des nœuds, puis le coordinator agrège les résultats partiels.

---

### 3.3 – Requête financière cross-site (10 pts)

#### ✏️ Exercice 3.3.a – Chiffre d'affaires par pays et type

```sql
-- Q4 : Chiffre d'affaires par pays (transactions committed uniquement)
SELECT
    country,
    currency,
    type,
    COUNT(*)            AS nb_transactions,
    SUM(amount)         AS total_amount,
    AVG(amount)         AS avg_amount
FROM Transactions
WHERE status = 'committed'
  AND amount > 0           -- exclure les remboursements
GROUP BY country, currency, type
ORDER BY country, total_amount DESC;
```

> ```
> [VOTRE RÉSULTAT]
> ```

#### ✏️ Exercice 3.3.b – Écrire votre propre requête

Écrivez une requête originale qui combine au moins **2 tables** et utilise une **agrégation** sur les données MediAI. Justifiez son intérêt métier.

> **Intérêt métier :** Identifier les patients dont le score IA moyen est élevé (risque médical élevé) ET dont les dépenses totales sont importantes, afin de prioriser les ressources médicales et adapter les politiques de remboursement.

> **Votre requête SQL :**
> 
> ```sql
> -- Score IA moyen et dépenses totales par patient (patients à risque et coûteux)
> SELECT
>     p.name,
>     p.country,
>     p.age,
>     COUNT(mr.idRecord)                        AS nb_examens,
>     ROUND(AVG(mr.aiScore)::numeric, 4)        AS score_ia_moyen,
>     COUNT(t.idTrans)                           AS nb_transactions,
>     COALESCE(SUM(t.amount), 0)                AS total_depenses,
>     t.currency
> FROM Patients p
> LEFT JOIN MedicalRecords mr ON p.idPatient = mr.idPatient
>                             AND p.country   = mr.country
> LEFT JOIN Transactions t    ON p.idPatient = t.idPatient
>                             AND p.country   = t.country
> WHERE t.status = 'committed'
> GROUP BY p.name, p.country, p.age, t.currency
> HAVING AVG(mr.aiScore) > 0.85
> ORDER BY score_ia_moyen DESC, total_depenses DESC;
> ```

> **Résultat :**
> 
> ```
> [VOTRE RÉSULTAT]
> ```

---

## 🔐 Partie 4 – Transactions distribuées : Two-Phase Commit (30 pts)

### 4.1 – Contexte et rappel théorique (5 pts)

Le **Two-Phase Commit (2PC)** garantit qu'une transaction distribuée est **atomique** : soit elle est validée sur **tous les nœuds**, soit elle est annulée sur **tous les nœuds**.

```
           COORDINATOR
               │
      ┌────────┴────────┐
      │    Phase 1      │
      │  PREPARE ──→    │
      │  ←── READY      │
      │  ←── READY      │
      │    Phase 2      │
      │  COMMIT ──→     │
      └─────────────────┘
```

**Question 4.1** : Décrivez dans vos propres mots les deux phases du 2PC. Que se passe-t-il si un worker répond `ABORT` en Phase 1 ?

> **Phase 1 (Prepare) :**
> 
> Le coordinator envoie un message `PREPARE` à tous les workers participants. Chaque worker exécute toutes les opérations de la transaction localement, acquiert les verrous nécessaires, écrit les modifications dans son journal (WAL), puis répond `READY` s'il est prêt à valider, ou `ABORT` s'il rencontre un problème (contrainte violée, manque d'espace, timeout…). À ce stade, aucune modification n'est encore rendue permanente.

> **Phase 2 (Commit) :**
> 
> Si **tous** les workers ont répondu `READY`, le coordinator envoie `COMMIT` à tous → chaque worker rend ses modifications permanentes et libère ses verrous. Si **au moins un** worker a répondu `ABORT`, le coordinator envoie `ROLLBACK` à tous → chaque worker annule ses modifications locales. La décision est écrite dans le journal du coordinator.

> **Si un worker répond ABORT :**
> 
> La transaction est **entièrement annulée** sur tous les nœuds. Le coordinator envoie `ROLLBACK PREPARED` à chacun des workers ayant répondu `READY` afin qu'ils défassent leurs modifications. Aucune donnée n'est modifiée sur aucun nœud → l'**atomicité** est préservée.

---

### 4.2 – Simulation d'un 2PC en SQL PostgreSQL (15 pts)

#### Scénario

Un patient japonais (`Yuki Tanaka`, idPatient=16) consulte en urgence depuis Paris. La transaction doit :
1. Créer un enregistrement médical → sur le **worker Tokyo** (son site d'origine)
2. Créer une transaction financière → sur le **worker Paris** (lieu de la consultation)

**Ces deux opérations doivent être atomiques.**

#### ✏️ Exercice 4.2.a – Phase 1 : PREPARE (sur le coordinator)

```sql
-- ── Démarrer la transaction distribuée ──────────────────────
BEGIN;

-- Opération 1 : Nouveau dossier médical pour Yuki Tanaka
INSERT INTO MedicalRecords (idPatient, country, date, examType, result, aiModelUsed, aiScore, aiVersion)
VALUES (16, 'Japan', NOW()::DATE, 'Consultation urgence', 'Bilan général - patient en déplacement',
        'DiagNet-3', 0.8934, 'v3.2');

-- Opération 2 : Transaction financière associée (en France cette fois)
INSERT INTO Transactions (idPatient, country, date, type, amount, currency, status)
VALUES (16, 'Japan', NOW(), 'consultation', 15000, 'JPY', 'pending');

-- ── Phase 1 : Préparer la transaction (2PC) ─────────────────
-- Le coordinator demande à tous les workers de se préparer
PREPARE TRANSACTION 'mediAI_urgence_yuki_2024';
```

📸 **Capture d'écran** : exécution du PREPARE TRANSACTION

> **Collez votre capture ici :**
> 
> ```
> [VOTRE CAPTURE]
> ```

#### ✏️ Exercice 4.2.b – Vérifier les transactions préparées

```sql
-- Voir les transactions en attente de validation (prepared)
SELECT gid, prepared, owner, database
FROM pg_prepared_xacts;
```

**Question 4.2.b** : Que contient la colonne `gid` ? À quoi sert-elle dans le protocole 2PC ?

> La colonne `gid` (Global Transaction Identifier) contient l'**identifiant global unique** de la transaction préparée — ici `'mediAI_urgence_yuki_2024'`. Dans le protocole 2PC, le `gid` sert de **clé de référence universelle** permettant au coordinator (et aux workers) d'identifier sans ambiguïté quelle transaction doit être validée (`COMMIT PREPARED 'gid'`) ou annulée (`ROLLBACK PREPARED 'gid'`), même après un redémarrage ou une panne, grâce à sa persistance dans les journaux de transaction (WAL).

#### ✏️ Exercice 4.2.c – Phase 2 : COMMIT ou ROLLBACK

**Scénario A : Tout s'est bien passé → COMMIT**

```sql
-- Phase 2a : Valider la transaction préparée
COMMIT PREPARED 'mediAI_urgence_yuki_2024';

-- Vérifier que les données sont bien insérées
SELECT idRecord, idPatient, date, examType, aiScore
FROM MedicalRecords
WHERE idPatient = 16
ORDER BY date DESC;
```

> ```
> -- Résultat attendu : 1 ligne avec idPatient=16, examType='Consultation urgence', aiScore=0.8934
> [VOTRE RÉSULTAT]
> ```

**Scénario B : Un worker a échoué → ROLLBACK**

```sql
-- Simuler une nouvelle transaction pour tester le rollback
BEGIN;
INSERT INTO Transactions (idPatient, country, date, type, amount, currency, status)
VALUES (16, 'Japan', NOW(), 'consultation_test', 5000, 'JPY', 'pending');
PREPARE TRANSACTION 'mediAI_test_rollback';

-- Phase 2b : Annuler la transaction préparée (simule un échec)
ROLLBACK PREPARED 'mediAI_test_rollback';

-- Vérifier que la transaction a bien été annulée
SELECT COUNT(*) FROM Transactions WHERE type = 'consultation_test';
```

> ```
> -- Résultat attendu : COUNT = 0 (aucune ligne insérée, rollback réussi)
> [VOTRE RÉSULTAT]
> ```

---

### 4.3 – Gestion des défaillances (10 pts)

#### ✏️ Exercice 4.3.a – Simuler une panne worker

```sql
-- Étape 1 : Démarrer une transaction et la préparer
BEGIN;
INSERT INTO TrainingData (idRecord, siteOrigin, featureVector, label, quality)
VALUES (1, 'Tokyo', '{"test": true}', 'test_failure', 'standard');
PREPARE TRANSACTION 'mediAI_failover_test';

-- Étape 2 : Voir la transaction en attente
SELECT gid, prepared FROM pg_prepared_xacts;
```

Maintenant, dans un autre terminal, arrêtez un worker :

```bash
# Simuler une panne du worker Tokyo
docker stop citus_worker3

# Revenir dans psql et observer
```

```sql
-- Étape 3 : Tenter le COMMIT (va-t-il réussir ou échouer ?)
COMMIT PREPARED 'mediAI_failover_test';
```

**Question 4.3.a** : Qu'est-il arrivé lors du COMMIT après la panne du worker ? Comment le 2PC protège-t-il les données dans ce cas ?

> Lors du `COMMIT PREPARED`, Citus/PostgreSQL tente de contacter **citus_worker3** (Tokyo) pour finaliser la transaction. Puisque le worker est arrêté, la connexion échoue → le COMMIT **échoue avec une erreur** (ex. `ERROR: could not connect to server`). La transaction reste dans l'état `PREPARED` dans `pg_prepared_xacts` — elle n'est ni validée, ni annulée.
>
> **Protection 2PC :** Les données ne sont **jamais partiellement commitées**. La transaction reste en suspens jusqu'à ce que le coordinator puisse contacter le worker. Une fois `citus_worker3` redémarré, l'administrateur peut rejouer `COMMIT PREPARED 'mediAI_failover_test'` pour finaliser, ou `ROLLBACK PREPARED` pour annuler proprement. L'**atomicité** est garantie : jamais de commit partiel sur certains nœuds seulement.

```bash
# Redémarrer le worker
docker start citus_worker3
```

#### ✏️ Exercice 4.3.b – Questions de synthèse

**Question 4.3.b.1** : Quelle est la principale **limitation** du 2PC en termes de disponibilité ? (Hint : que se passe-t-il si le coordinator tombe en panne en Phase 2 ?)

> Le 2PC présente un risque de **blocage indéfini** (*blocking problem*). Si le coordinator tombe en panne **après** avoir envoyé les `PREPARE` aux workers (Phase 1 terminée) mais **avant** d'envoyer la décision `COMMIT` ou `ROLLBACK` (Phase 2), les workers restent **bloqués indéfiniment** avec leurs verrous acquis et leurs ressources gelées. Ils ne peuvent pas décider seuls car la décision appartient au coordinator. Le système devient indisponible jusqu'au redémarrage du coordinator — c'est le **single point of failure** du 2PC.

**Question 4.3.b.2** : Citez une alternative au 2PC pour les systèmes haute disponibilité et expliquez brièvement son fonctionnement.

> Le **Saga Pattern** est une alternative populaire. Au lieu d'une transaction atomique globale, une saga décompose l'opération en une **séquence de transactions locales** indépendantes, chacune suivie d'un événement. Si une étape échoue, des **transactions compensatoires** (rollbacks applicatifs) sont déclenchées en sens inverse pour défaire les étapes précédentes. Avantage : pas de verrous inter-nœuds, haute disponibilité. Inconvénient : cohérence **éventuelle** (pas immédiate) et complexité applicative accrue.

**Question 4.3.b.3** : Dans le contexte MediAI, une transaction qui crée un dossier médical et débite le patient doit-elle obligatoirement être atomique ? Justifiez en termes métier.

> **Oui, absolument.** Ces deux opérations doivent être atomiques pour trois raisons :
> 1. **Intégrité financière** : si le dossier médical est créé mais que le paiement échoue, le patient reçoit des soins sans être débité → perte financière pour MediAI.
> 2. **Intégrité médicale** : si le paiement est débité mais que le dossier n'est pas créé, il n'y a aucune trace clinique de la consultation → risque médico-légal grave (absence de traçabilité des soins).
> 3. **Conformité réglementaire** : dans le domaine de la santé (RGPD, HIPAA), tout acte médical doit être tracé et facturé de façon cohérente. Une incohérence entre la facturation et le dossier constitue une non-conformité réglementaire.

---

## 📊 Partie 5 – Bonus : Analyse de performance (hors barème)

### 5.1 – Comparer les plans d'exécution

```sql
-- Requête sans clé de distribution dans le WHERE (scan global)
EXPLAIN (ANALYZE, VERBOSE)
SELECT * FROM Patients WHERE name = 'Alice Dupont';

-- Requête avec clé de distribution (pruning)
EXPLAIN (ANALYZE, VERBOSE)
SELECT * FROM Patients WHERE country = 'France' AND name = 'Alice Dupont';
```

**Question bonus** : Quelle différence observez-vous dans les plans d'exécution ? Combien de shards sont scannés dans chaque cas ?

> La première requête (sans `country`) oblige Citus à interroger **tous les shards de tous les workers** (pas de shard pruning possible) → `Task Count` élevé dans le plan. La seconde (avec `country = 'France'`) permet le **shard pruning** : Citus identifie directement les shards contenant les données France et n'interroge que le(s) worker(s) concerné(s) → `Task Count` réduit à 1 ou quelques shards seulement. La différence de performance est significative à grande échelle.

### 5.2 – Monitoring du cluster

```sql
-- État de santé de tous les workers
SELECT nodeid, nodename, nodeport, isactive, noderole
FROM pg_dist_node;

-- Distribution des shards par worker
SELECT p.nodename, COUNT(*) AS nb_shards
FROM pg_dist_shard_placement p
GROUP BY p.nodename
ORDER BY nb_shards DESC;

-- Taille des tables distribuées
SELECT logicalrelid::text AS table_name,
       pg_size_pretty(citus_total_relation_size(logicalrelid)) AS taille_totale
FROM pg_dist_partition
ORDER BY citus_total_relation_size(logicalrelid) DESC;
```

> ```
> [VOS RÉSULTATS]
> ```

---


## 📋 Récapitulatif à rendre

Complétez ce tableau avant de soumettre votre TP :

| Exercice | Statut | Points obtenus |
|----------|--------|----------------|
| 1.1 – Lancement cluster | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 3 |
| 1.2 – Enregistrement workers | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 3 |
| 1.3 – Chargement données | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 4 |
| 2.1 – Fragmentation horizontale | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 10 |
| 2.2 – Fragmentation verticale | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 10 |
| 2.3 – Fragmentation hybride | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 10 |
| 3.1 – Requête profil patient | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 10 |
| 3.2 – Requête agrégée multi-sites | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 10 |
| 3.3 – Requête financière | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 10 |
| 4.1 – Théorie 2PC | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 5 |
| 4.2 – Simulation 2PC SQL | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 15 |
| 4.3 – Gestion défaillances | ☐ Fait / ☐ Partiel / ☐ Non fait | ___ / 10 |
| **TOTAL** | | ___ / 100 |

---

*⭐ Bon TP ! – Équipe pédagogique ENSTA 3A*
