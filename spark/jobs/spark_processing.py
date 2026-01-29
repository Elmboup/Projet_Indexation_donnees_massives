#!/usr/bin/env python3
"""
Script Spark - Traitement des données Vélib' depuis Elasticsearch
Analyse de l'utilisation des stations de vélos en partage à Paris
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min, max, count, sum as spark_sum,
    hour, dayofweek, date_format, when, round as spark_round
)
from pyspark.sql.window import Window
import json
from datetime import datetime


def create_spark_session():
    """Crée une session Spark avec connector Elasticsearch"""
    spark = SparkSession.builder \
        .appName("VelibDataProcessing") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
        .config("spark.es.nodes", "elasticsearch") \
        .config("spark.es.port", "9200") \
        .config("spark.es.net.http.auth.user", "elastic") \
        .config("spark.es.net.http.auth.pass", "changeme") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("Session Spark créée avec succès")
    return spark


def load_data_from_elasticsearch(spark):
    """Charge les données depuis Elasticsearch"""
    print("\n Chargement des données depuis Elasticsearch...")
    
    df = spark.read \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", "velib-data-*") \
        .option("es.read.field.as.array.include", "location,num_bikes_available_types,num_docks_available_types") \
        .load()
    
    total_records = df.count()
    print(f"{total_records:,} enregistrements chargés")
    
    print("\n Schéma des données:")
    df.printSchema()
    
    # Afficher quelques exemples
    print("\n Aperçu des données:")
    df.select("name", "num_bikes_available", "num_docks_available", 
              "occupation_rate", "station_status").show(5, truncate=False)
    
    return df


def analyze_station_availability(df):
    """Analyse de la disponibilité par station"""
    print("\n" + "="*80)
    print(" ANALYSE 1: Statistiques de Disponibilité par Station")
    print("="*80)
    
    station_stats = df.groupBy("name", "station_id") \
        .agg(
            count("*").alias("nb_mesures"),
            avg("num_bikes_available").alias("bikes_avg"),
            min("num_bikes_available").alias("bikes_min"),
            max("num_bikes_available").alias("bikes_max"),
            avg("num_docks_available").alias("docks_avg"),
            avg("occupation_rate").alias("occupation_avg"),
            spark_sum(when(col("station_status") == "empty", 1).otherwise(0)).alias("times_empty"),
            spark_sum(when(col("station_status") == "full", 1).otherwise(0)).alias("times_full"),
            avg("mechanical_bikes").alias("mechanical_avg"),
            avg("electric_bikes").alias("electric_avg")
        ) \
        .withColumn("bikes_avg", spark_round(col("bikes_avg"), 2)) \
        .withColumn("occupation_avg", spark_round(col("occupation_avg"), 2)) \
        .orderBy(col("occupation_avg").desc())
    
    print(f"\n Nombre total de stations: {station_stats.count()}")
    print("\n Top 20 stations avec le plus fort taux d'occupation:")
    station_stats.select(
        "name", "bikes_avg", "docks_avg", "occupation_avg", 
        "times_empty", "times_full"
    ).show(20, truncate=False)
    
    # Sauvegarder
    station_stats.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("/opt/spark-jobs/output/station_availability")
    
    print(" Résultats sauvegardés: /opt/spark-jobs/output/station_availability/")
    
    return station_stats


def analyze_hourly_patterns(df):
    """Analyse des patterns horaires"""
    print("\n" + "="*80)
    print(" ANALYSE 2: Patterns d'Utilisation par Heure")
    print("="*80)
    
    # Ajouter colonne heure
    df_with_hour = df.withColumn("hour", hour(col("@timestamp")))
    
    hourly_stats = df_with_hour.groupBy("hour") \
        .agg(
            count("*").alias("nb_mesures"),
            avg("num_bikes_available").alias("bikes_avg"),
            avg("num_docks_available").alias("docks_avg"),
            avg("occupation_rate").alias("occupation_avg"),
            spark_sum(when(col("station_status") == "empty", 1).otherwise(0)).alias("stations_empty"),
            spark_sum(when(col("station_status") == "full", 1).otherwise(0)).alias("stations_full")
        ) \
        .withColumn("bikes_avg", spark_round(col("bikes_avg"), 2)) \
        .withColumn("occupation_avg", spark_round(col("occupation_avg"), 2)) \
        .orderBy("hour")
    
    print("\n Statistiques horaires (sur 24h):")
    hourly_stats.show(24, truncate=False)
    
    # Identifier les heures de pointe
    peak_hours = hourly_stats.orderBy(col("occupation_avg").desc()).limit(5)
    print("\n Heures de pointe (occupation maximale):")
    peak_hours.select("hour", "occupation_avg", "stations_empty", "stations_full").show()
    
    # Sauvegarder
    hourly_stats.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("/opt/spark-jobs/output/hourly_patterns")
    
    print(" Résultats sauvegardés: /opt/spark-jobs/output/hourly_patterns/")
    
    return hourly_stats


def analyze_by_time_period(df):
    """Analyse par période de la journée"""
    print("\n" + "="*80)
    print(" ANALYSE 3: Statistiques par Période de la Journée")
    print("="*80)
    
    period_stats = df.groupBy("time_period") \
        .agg(
            count("*").alias("nb_mesures"),
            avg("num_bikes_available").alias("bikes_avg"),
            avg("occupation_rate").alias("occupation_avg"),
            spark_sum(when(col("station_status") == "empty", 1).otherwise(0)).alias("empty_count"),
            spark_sum(when(col("station_status") == "full", 1).otherwise(0)).alias("full_count")
        ) \
        .withColumn("bikes_avg", spark_round(col("bikes_avg"), 2)) \
        .withColumn("occupation_avg", spark_round(col("occupation_avg"), 2))
    
    print("\n Statistiques par période:")
    period_stats.show(truncate=False)
    
    # Sauvegarder
    period_stats.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("/opt/spark-jobs/output/period_analysis")
    
    print(" Résultats sauvegardés: /opt/spark-jobs/output/period_analysis/")
    
    return period_stats


def analyze_bike_types(df):
    """Analyse des types de vélos (mécaniques vs électriques)"""
    print("\n" + "="*80)
    print(" ANALYSE 4: Répartition Vélos Mécaniques vs Électriques")
    print("="*80)
    
    bike_types = df.filter(
        col("mechanical_bikes").isNotNull() & col("electric_bikes").isNotNull()
    ).agg(
        avg("mechanical_bikes").alias("mechanical_avg"),
        avg("electric_bikes").alias("electric_avg"),
        spark_sum("mechanical_bikes").alias("mechanical_total"),
        spark_sum("electric_bikes").alias("electric_total")
    )
    
    print("\n🚲 Moyenne et totaux par type de vélo:")
    bike_types.show(truncate=False)
    
    # Par station
    bike_by_station = df.filter(
        col("mechanical_bikes").isNotNull() & col("electric_bikes").isNotNull()
    ).groupBy("name") \
        .agg(
            avg("mechanical_bikes").alias("mechanical_avg"),
            avg("electric_bikes").alias("electric_avg")
        ) \
        .withColumn("mechanical_avg", spark_round(col("mechanical_avg"), 2)) \
        .withColumn("electric_avg", spark_round(col("electric_avg"), 2)) \
        .withColumn("ratio_electric", 
                   spark_round(col("electric_avg") / (col("mechanical_avg") + col("electric_avg")) * 100, 2))
    
    print("\n Top 10 stations avec le plus de vélos électriques:")
    bike_by_station.orderBy(col("electric_avg").desc()).show(10, truncate=False)
    
    # Sauvegarder
    bike_by_station.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("/opt/spark-jobs/output/bike_types_analysis")
    
    print(" Résultats sauvegardés: /opt/spark-jobs/output/bike_types_analysis/")
    
    return bike_by_station


def identify_problematic_stations(df):
    """Identifier les stations problématiques (souvent vides ou pleines)"""
    print("\n" + "="*80)
    print(" ANALYSE 5: Stations Problématiques")
    print("="*80)
    
    problematic = df.groupBy("name", "station_id") \
        .agg(
            count("*").alias("total_observations"),
            spark_sum(when(col("station_status") == "empty", 1).otherwise(0)).alias("empty_count"),
            spark_sum(when(col("station_status") == "full", 1).otherwise(0)).alias("full_count")
        ) \
        .withColumn("empty_rate", 
                   spark_round(col("empty_count") / col("total_observations") * 100, 2)) \
        .withColumn("full_rate", 
                   spark_round(col("full_count") / col("total_observations") * 100, 2)) \
        .withColumn("problem_rate", col("empty_rate") + col("full_rate"))
    
    print("\n  Top 20 stations avec le plus de problèmes (souvent vides ou pleines):")
    problematic.orderBy(col("problem_rate").desc()) \
        .select("name", "empty_rate", "full_rate", "problem_rate") \
        .show(20, truncate=False)
    
    # Stations souvent vides
    print("\n Top 10 stations souvent vides:")
    problematic.orderBy(col("empty_rate").desc()) \
        .select("name", "empty_rate") \
        .show(10, truncate=False)
    
    # Stations souvent pleines
    print("\n Top 10 stations souvent pleines:")
    problematic.orderBy(col("full_rate").desc()) \
        .select("name", "full_rate") \
        .show(10, truncate=False)
    
    # Sauvegarder
    problematic.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("/opt/spark-jobs/output/problematic_stations")
    
    print(" Résultats sauvegardés: /opt/spark-jobs/output/problematic_stations/")
    
    return problematic


def generate_global_summary(df, station_stats, hourly_stats):
    """Génère un résumé global de l'analyse"""
    print("\n" + "="*80)
    print(" RÉSUMÉ GLOBAL")
    print("="*80)
    
    # Calculer des métriques globales
    total_stations = df.select("station_id").distinct().count()
    total_observations = df.count()
    
    global_stats = df.agg(
        avg("num_bikes_available").alias("global_bikes_avg"),
        avg("num_docks_available").alias("global_docks_avg"),
        avg("occupation_rate").alias("global_occupation_avg"),
        spark_sum("capacity").alias("total_capacity")
    ).collect()[0]
    
    summary = {
        "generated_at": datetime.now().isoformat(),
        "analysis_period": {
            "total_observations": total_observations,
            "total_stations": total_stations
        },
        "global_statistics": {
            "avg_bikes_available": round(global_stats["global_bikes_avg"], 2),
            "avg_docks_available": round(global_stats["global_docks_avg"], 2),
            "avg_occupation_rate": round(global_stats["global_occupation_avg"], 2),
            "total_system_capacity": int(global_stats["total_capacity"])
        },
        "top_occupied_stations": station_stats.limit(10).toPandas().to_dict('records'),
        "hourly_pattern": hourly_stats.toPandas().to_dict('records')
    }
    
    # Sauvegarder le résumé JSON
    with open('/opt/spark-jobs/output/global_summary.json', 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n Statistiques Globales:")
    print(f"  • Nombre total de stations: {total_stations}")
    print(f"  • Nombre total d'observations: {total_observations:,}")
    print(f"  • Vélos disponibles (moyenne): {round(global_stats['global_bikes_avg'], 2)}")
    print(f"  • Taux d'occupation moyen: {round(global_stats['global_occupation_avg'], 2)}%")
    print(f"  • Capacité totale du système: {int(global_stats['total_capacity']):,} vélos")
    
    print("\n Résumé global sauvegardé: /opt/spark-jobs/output/global_summary.json")


def main():
    """Fonction principale"""
    print("="*80)
    print("🚴 ANALYSE SPARK - DONNÉES VÉLIB' PARIS")
    print("="*80)
    print(f" Début de l'analyse: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Créer session Spark
    spark = create_spark_session()
    
    try:
        # Charger les données
        df = load_data_from_elasticsearch(spark)
        
        # Cache pour optimiser les performances
        df.cache()
        
        # Analyses
        station_stats = analyze_station_availability(df)
        hourly_stats = analyze_hourly_patterns(df)
        period_stats = analyze_by_time_period(df)
        bike_types = analyze_bike_types(df)
        problematic = identify_problematic_stations(df)
        
        # Résumé global
        generate_global_summary(df, station_stats, hourly_stats)
        
        print("\n" + "="*80)
        print("ANALYSE TERMINÉE AVEC SUCCÈS")
        print("="*80)
        print(f" Fin de l'analyse: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("\n Tous les résultats sont disponibles dans: /opt/spark-jobs/output/")
        
    except Exception as e:
        print(f"\n ERREUR lors du traitement: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()
        print("\n Session Spark fermée")


if __name__ == "__main__":
    main()