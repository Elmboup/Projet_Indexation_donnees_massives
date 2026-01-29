#!/usr/bin/env python3
"""
Kafka Producer - API Vélib' Métropole (Paris)
Collecte les données des stations Vélib' en temps réel
"""

import os
import json
import time
import logging
import requests
from kafka import KafkaProducer
from datetime import datetime

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'velib-data')
FETCH_INTERVAL = int(os.getenv('FETCH_INTERVAL', '60'))  # secondes
MAX_STATIONS = int(os.getenv('MAX_STATIONS', '50'))  # Limiter le nombre de stations

# URLs API Vélib' (pas de clé requise !)
VELIB_STATION_STATUS_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
VELIB_STATION_INFO_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"


def create_kafka_producer():
    """Crée et retourne un producteur Kafka"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1,
            max_request_size=10485760,  # 10MB
            buffer_memory=33554432,  # 32MB
            compression_type='gzip'  # Compresser les messages
        )
        logger.info(f"✅ Producteur Kafka créé - Serveurs: {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"❌ Erreur création producteur Kafka: {e}")
        raise


def fetch_station_info():
    """Récupère les informations statiques des stations (une seule fois)"""
    try:
        response = requests.get(VELIB_STATION_INFO_URL, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        stations_info = {}
        
        for station in data.get('data', {}).get('stations', []):
            stations_info[station['station_id']] = {
                'name': station.get('name', 'Unknown'),
                'lat': station.get('lat', 0),
                'lon': station.get('lon', 0),
                'capacity': station.get('capacity', 0),
                'rental_methods': station.get('rental_methods', [])
            }
        
        logger.info(f"✅ Infos de {len(stations_info)} stations chargées")
        return stations_info
        
    except Exception as e:
        logger.error(f"❌ Erreur chargement infos stations: {e}")
        return {}


def fetch_velib_data(stations_info):
    """Récupère l'état en temps réel des stations Vélib'"""
    try:
        response = requests.get(VELIB_STATION_STATUS_URL, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        timestamp = datetime.utcnow().isoformat()
        
        stations_data = []
        
        # Limiter le nombre de stations pour éviter les messages trop gros
        all_stations = data.get('data', {}).get('stations', [])
        limited_stations = all_stations[:MAX_STATIONS]
        
        logger.info(f"📊 Total disponible: {len(all_stations)}, Traitement: {len(limited_stations)} stations")
        
        for station in limited_stations:
            station_id = station.get('station_id')
            
            # Enrichir avec les infos statiques
            info = stations_info.get(station_id, {})
            
            station_record = {
                # IDs et timestamps
                'station_id': station_id,
                'collected_at': timestamp,
                'last_reported': station.get('last_reported', 0),
                
                # Informations de la station
                'name': info.get('name', 'Unknown'),
                'latitude': info.get('lat', 0),
                'longitude': info.get('lon', 0),
                'capacity': info.get('capacity', 0),
                
                # État en temps réel
                'num_bikes_available': station.get('num_bikes_available', 0),
                'num_bikes_available_types': station.get('num_bikes_available_types', []),
                'num_docks_available': station.get('num_docks_available', 0),
                
                # Statut
                'is_installed': station.get('is_installed', 0),
                'is_returning': station.get('is_returning', 0),
                'is_renting': station.get('is_renting', 0),
                
                # Calculer le taux d'occupation
                'occupation_rate': round(
                    (station.get('num_bikes_available', 0) / info.get('capacity', 1)) * 100, 2
                ) if info.get('capacity', 0) > 0 else 0,
                
                # Métadonnées
                'source': 'velib_api',
                'city': 'Paris'
            }
            
            # Ajouter types de vélos si disponible
            for bike_type in station.get('num_bikes_available_types', []):
                if bike_type.get('mechanical') is not None:
                    station_record['mechanical_bikes'] = bike_type['mechanical']
                if bike_type.get('ebike') is not None:
                    station_record['electric_bikes'] = bike_type['ebike']
            
            stations_data.append(station_record)
        
        logger.info(f"✅ {len(stations_data)} stations récupérées")
        return stations_data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erreur requête API Vélib': {e}")
        return []
    except Exception as e:
        logger.error(f"❌ Erreur inattendue: {e}")
        return []


def send_to_kafka(producer, stations_data):
    """Envoie les données des stations vers Kafka"""
    success_count = 0
    error_count = 0
    
    for station in stations_data:
        try:
            future = producer.send(KAFKA_TOPIC, value=station)
            future.get(timeout=10)
            success_count += 1
            
        except Exception as e:
            error_count += 1
            logger.error(f"❌ Erreur envoi station {station.get('station_id')}: {e}")
    
    if success_count > 0:
        logger.info(f"✅ {success_count} stations envoyées à Kafka")
    if error_count > 0:
        logger.warning(f"⚠️  {error_count} erreurs d'envoi")
    
    return success_count > 0


def main():
    """Fonction principale"""
    logger.info("🚴 Démarrage du collecteur Vélib'...")
    logger.info(f"📊 Configuration: TOPIC={KAFKA_TOPIC}, INTERVAL={FETCH_INTERVAL}s")
    
    # Attendre que Kafka soit prêt
    logger.info("⏳ Attente de Kafka (10s)...")
    time.sleep(10)
    
    # Créer le producteur
    producer = create_kafka_producer()
    
    # Charger les infos statiques des stations (une seule fois)
    logger.info("📍 Chargement des informations des stations...")
    stations_info = fetch_station_info()
    
    if not stations_info:
        logger.warning("⚠️  Aucune info de station chargée, continuation avec données limitées")
    
    cycle_count = 0
    
    try:
        while True:
            cycle_count += 1
            logger.info(f"\n{'='*60}")
            logger.info(f"🔄 Cycle {cycle_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"{'='*60}")
            
            # Récupérer les données en temps réel
            stations_data = fetch_velib_data(stations_info)
            
            if stations_data:
                # Envoyer vers Kafka
                if send_to_kafka(producer, stations_data):
                    logger.info(f"✅ Cycle {cycle_count} complété avec succès")
                else:
                    logger.warning(f"⚠️  Cycle {cycle_count} complété avec erreurs")
            else:
                logger.warning(f"⚠️  Aucune donnée récupérée pour le cycle {cycle_count}")
            
            # Statistiques rapides
            if stations_data:
                total_bikes = sum(s.get('num_bikes_available', 0) for s in stations_data)
                total_docks = sum(s.get('num_docks_available', 0) for s in stations_data)
                logger.info(f"📊 Total: {total_bikes} vélos disponibles, {total_docks} places libres")
            
            # Attendre avant le prochain cycle
            logger.info(f"⏳ Attente de {FETCH_INTERVAL} secondes...")
            time.sleep(FETCH_INTERVAL)
            
    except KeyboardInterrupt:
        logger.info("\n🛑 Arrêt du producteur (Ctrl+C)")
    except Exception as e:
        logger.error(f"❌ Erreur fatale: {e}")
    finally:
        producer.close()
        logger.info("👋 Producteur Kafka fermé")


if __name__ == "__main__":
    main()