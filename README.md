# Skello Intercom Data Pipeline

Projet réalisé dans le cadre du case study Data Analyst de Skello.
Objectif : mettre en place un pipeline dbt sur les données Intercom, proposer un modèle de données fiable et construire un template de dashboard pour l’équipe Support.

## Consignes d’installation

1. Copiez profiles.yml.example vers ~/.dbt/profiles.yml
2. Remplacez les valeurs par défaut par vos identifiants Snowflake
3. Vérifiez la connexion avec :
```bash
dbt debug
```

## Construisez les modèles avec :
```bash
dbt run
```

## Prérequis

1. Compte Snowflake avec accès ACCOUNTADMIN (pour le setup initial)
2. dbt installé en local
3. Données Intercom brutes chargées dans le schéma SKELLO_INTERCOM.RAW
4. Tableau Desktop / Server pour la visualisation (connexion directe à Snowflake ou via CSV export)

## Initialisation Snowflake

Un script d’initialisation (`setup/01_snowflake_permissions.sql`) est fourni pour créer les schémas
(`RAW`, `STG`, `MART`) et accorder les droits nécessaires au rôle `DBT_ROLE`.  

Ce script doit être exécuté **une seule fois avec le rôle `ACCOUNTADMIN`** avant de lancer dbt.

## Configuration dbt

Exemple de ~/.dbt/profiles.yml :

skello_intercom:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account
      user: your_username
      password: your_password
      role: DBT_ROLE
      database: SKELLO_INTERCOM
      warehouse: COMPUTE_WH
      schema: mart

## Exécution du pipeline

1. Installer les dépendances
```bash
dbt deps
```

2. Lancer les modèles de staging
```bash
dbt run --select staging
```

3. Lancer les modèles de marts (inclut le nettoyage pour Tableau)
```bash
dbt run --select marts
```

4. Vérifier avec les tests
```bash
dbt test
```

## Structure du projet
```text
├── models/
│   ├── staging/          # Transformations de données brutes
│   └── marts/            # Logique métier & métriques
├── setup/                # Scripts de configuration Snowflake
└── README.md             # Ce fichier
```

## Dashboard

### Données

Le projet repose sur deux fichiers fournis pour le case study :

- CONVERSATIONS.csv
- CONVERSATION_PARTS.csv

Ces fichiers sont inclus dans ce dépôt pour faciliter la reproduction du projet.
Dans ce projet, j’ai importé ces CSV dans Snowflake, puis construit un pipeline dbt pour les transformer.

### Nettoyage

Le projet inclut un nettoyage spécifique dans models/marts/fct_conversations.sql afin de corriger les problèmes liés à l’export CSV :

- Suppression des sauts de ligne : remarques CSAT contenant des retours chariot qui perturbaient le parsing

- Gestion des valeurs vides : harmonisation des champs nuls/vides

- Nettoyage des champs texte : suppression de caractères problématiques tout en préservant l’intégrité des données

Deux options sont possibles dans Tableau :

- Connexion directe à Snowflake (recommandée en production)

- Connexion via fichiers CSV exportés du schéma MART (utilisée dans ce projet)

### Utilisation dans Tableau

Dans ce rendu, j’ai connecté Tableau aux exports CSV pour créer le visuel demandé.

En production, Tableau pourrait se connecter directement à Snowflake grâce au connecteur natif, ce qui permettrait d’alimenter le reporting de manière dynamique et automatisée.

## Questions / hypothèses pour Lorette

- Période analysée : j’ai comparé l’avant-dernière semaine avec la semaine précédente, car la dernière semaine semble incomplète (volume nettement inférieur à la moyenne).

- First Response Time : j’ai constaté des valeurs aberrantes dont une négative. Pour fiabiliser l’analyse, j’ai utilisé la médiane plutôt que la moyenne et converti la valeur négative en 0 (problème de qualité de données).

- Hypothèse SLA : le SLA cible correspond-il bien à 70 % des réponses en moins de 5 minutes ?

- Hypothèse CSAT : le CSAT cible est-il fixé à 4.0/5 (soit 80 %) ?

- Date de réunion : quand a lieu la réunion ? J’ai analysé l’avant-dernière semaine car la dernière semble incomplète, mais ce serait bien d’avoir confirmation afin de savoir si je peux l’inclure ou non.