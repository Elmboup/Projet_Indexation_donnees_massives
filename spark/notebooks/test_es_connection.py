#!/usr/bin/env python3
"""Test Spark + Elasticsearch Connection"""

from pyspark.sql import SparkSession
import json
import subprocess

print("=" * 80)
print("🧪 Test Spark + Elasticsearch Connection")
print("=" * 80)

# Step 1: Test HTTP connection to ES
print("\n1️⃣  Test HTTP connection à Elasticsearch...")
try:
    result = subprocess.run([
        'curl', '-s', '-u', 'elastic:changeme',
        'http://elasticsearch:9200/'
    ], capture_output=True, text=True, timeout=5)
    
    es_info = json.loads(result.stdout)
    print(f"✅ Connexion réussie à Elasticsearch v{es_info['version']['number']}")
except Exception as e:
    print(f"❌ Erreur: {e}")
    exit(1)

# Step 2: Check indices
print("\n2️⃣  Vérification des indices Vélib'...")
try:
    result = subprocess.run([
        'curl', '-s', '-u', 'elastic:changeme',
        'http://elasticsearch:9200/_cat/indices?format=json'
    ], capture_output=True, text=True, timeout=5)
    
    indices = json.loads(result.stdout)
    velib_indices = [idx for idx in indices if 'velib' in idx['index']]
    
    if velib_indices:
        print(f"✅ {len(velib_indices)} indice(s) Vélib' trouvé(s):")
        for idx in velib_indices:
            print(f"   - {idx['index']}: {idx['docs.count']} documents")
    else:
        print("❌ Aucun indice Vélib' trouvé!")
        exit(1)
except Exception as e:
    print(f"❌ Erreur: {e}")
    exit(1)

# Step 3: Test Spark session
print("\n3️⃣  Création de la session Spark...")
try:
    spark = SparkSession.builder \
        .appName("ESConnectionTest") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
        .config("spark.es.nodes", "elasticsearch:9200") \
        .config("spark.es.nodes.wan.only", "true") \
        .config("spark.es.net.http.auth.user", "elastic") \
        .config("spark.es.net.http.auth.pass", "changeme") \
        .config("spark.es.net.ssl.cert.verification.skip", "true") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print(f"✅ Spark v{spark.version} créé avec succès")
except Exception as e:
    print(f"❌ Erreur: {e}")
    exit(1)

# Step 4: Test data loading
print("\n4️⃣  Test du chargement des données...")
try:
    df = spark.read \
        .format("org.elasticsearch.spark.sql") \
        .option("es.read.metadata", "false") \
        .option("es.resource", "velib-data-*") \
        .option("es.read.field.as.array.include", "num_bikes_available_types,num_docks_available_types") \
        .option("es.query", "?size=10") \
        .load()
    
    count = df.count()
    print(f"✅ {count} enregistrements chargés")
    
    print("\n📋 Schéma:")
    df.printSchema()
    
    print("\n📊 Aperçu des données:")
    df.show(5, truncate=False)
    
except Exception as e:
    print(f"❌ Erreur lors du chargement: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("\n" + "=" * 80)
print("✅ Tous les tests sont passés!")
print("=" * 80)

spark.stop()
