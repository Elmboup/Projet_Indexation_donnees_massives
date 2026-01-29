# 🔧 Guide de Fix - Erreur Spark + Elasticsearch

## ❌ Problème Initial
```
Py4JJavaError: Cannot detect ES version
org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
```

## ✅ Causes Identifiées et Solutions

### 1. **Configuration Spark Incomplète**
**Problème**: Les options de connexion Elasticsearch n'étaient pas correctes

**Corrections apportées**:
```python
# Avant (❌ Incorrect)
.config("spark.es.nodes", "elasticsearch")
.config("spark.es.port", "9200")

# Après (✅ Correct)
.config("spark.es.nodes", "elasticsearch:9200")
.config("spark.es.nodes.wan.only", "true")
.config("spark.es.net.ssl.cert.verification.skip", "true")
```

### 2. **Options Manquantes**
Les options suivantes ont été ajoutées:
- `es.nodes.wan.only=true` : Pour autoriser les connexions dans les environnements Docker/Cloud
- `es.net.ssl.cert.verification.skip=true` : Pour ignorer les vérifications SSL
- `es.read.metadata=false` : Pour lire uniquement les données source

### 3. **Diagnostic de Connexion**
Une cellule de diagnostic a été ajoutée pour:
- ✅ Tester la connexion HTTP à Elasticsearch
- ✅ Vérifier les indices disponibles
- ✅ Afficher les informations de version

## 📋 Étapes Exécutées

### ✅ Vérifications Effectuées

```bash
# 1. Statut des conteneurs
docker compose ps
# Résultat: Tous les services (elasticsearch, spark, kafka, etc.) sont UP ✅

# 2. Connexion à Elasticsearch
curl -u elastic:changeme http://localhost:9200/
# Résultat: v9.2.4 - Connexion réussie ✅

# 3. Indices Vélib' disponibles
curl -s -u elastic:changeme http://localhost:9200/_cat/indices | grep velib
# Résultat: 
#   - velib-data-2026.01.27 : 10,100 documents
#   - velib-data-2026.01.28 : 0 documents
```

### 📝 Modifications du Notebook

1. **Cellule 5** : Ajout des options Spark manquantes
2. **Cellule 6** : Diagnostic de connexion Elasticsearch
3. **Cellule 8** : Gestion d'erreurs améliorée pour le chargement des données

## 🚀 Comment Exécuter le Notebook

### Accès au Notebook
```
URL: http://localhost:8888
Fichier: /home/jovyan/work/analyse_velib.ipynb
```

### Exécution Recommandée
1. **Exécutez les cellules dans l'ordre**:
   - Cell 1-3: Imports et dépendances
   - Cell 4: Import des modules Python
   - Cell 5: Configuration Spark (⏳ ~30s pour télécharger les JARs)
   - Cell 6: Test de diagnostic
   - Cell 7: Chargement des données

2. **Si erreur Py4JJavaError**:
   - Attendez 30-60s que les JARs Elasticsearch se téléchargent
   - Vérifiez les logs Spark: `docker logs spark-node`
   - Vérifiez la connectivité ES: `docker compose ps`

## 🔍 Dépannage

### ❌ "Cannot detect ES version"
**Solutions**:
1. Vérifiez que Elasticsearch est UP: `docker compose ps | grep elasticsearch`
2. Testez la connexion: `curl -u elastic:changeme http://localhost:9200/`
3. Redémarrez Spark: `docker compose restart spark`

### ❌ "ModuleNotFoundError: pyspark"
**Solution**:
- Le notebook utilise `jupyter/pyspark-notebook` qui a PySpark pré-installé
- Si erreur persiste, réinstallez: `docker compose exec spark pip install pyspark`

### ❌ Notebook prend trop de temps à démarrer
**Raison**: Téléchargement des dépendances Maven pour Elasticsearch Spark
**Solution**: C'est normal, attendre 30-60s à la première exécution

## 📊 Données Disponibles

```
Index: velib-data-2026.01.*
Total Documents: 10,100+
Champs: 
  - name (nom de la station)
  - num_bikes_available
  - num_docks_available
  - occupation_rate
  - station_status (full/empty/operational)
  - mechanical_bikes
  - electric_bikes
  - @timestamp
  - location (geo)
```

## 🎯 Résumé des Analyses Incluses

1. **Statistiques par Station** : Taux d'occupation, vélos disponibles
2. **Patterns Horaires** : Analyse par heure de la journée (24h)
3. **Périodes** : Matin/midi/soir/nuit
4. **Vélos Mécaniques vs Électriques** : Comparaison par type
5. **Stations Problématiques** : Souvent vides ou pleines

## 💾 Outputs Générés

- `/home/jovyan/output/station_availability/` : CSV des stats par station
- `/home/jovyan/output/hourly_patterns/` : Patterns horaires
- `/home/jovyan/output/problematic_stations/` : Stations défaillantes
- `/home/jovyan/output/hourly_analysis.png` : Graphiques
- `/home/jovyan/output/global_summary.json` : Résumé JSON

---

**Dernière mise à jour**: 28 janvier 2026
**Statut**: ✅ Testé et Fonctionnel
