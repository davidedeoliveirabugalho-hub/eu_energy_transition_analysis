# === Gestion de la configuration ===
import os                          # Lire les variables d'environnement
from dotenv import load_dotenv     # Charger le fichier .env
import yaml                        # Lire le fichier config.yaml

# === Manipulation des données ===
import requests                    # Faire des requêtes HTTP à l'API
import pandas as pd                # Transformer les données en DataFrame

# === BigQuery ===
from google.cloud import bigquery  # Écrire dans BigQuery

# === Gestion des dates ===
from datetime import datetime, timedelta  # Manipuler les dates

# === Étape 1 : Charger la configuration ===

def load_configuration():
    """Charge la configuration depuis .env et config.yaml"""
    
    # 1.1 Charger les variables d'environnement
    load_dotenv()
    
    # 1.2 Récupérer les secrets depuis .env
    api_key = os.getenv("ENTSOE_API_KEY")
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BIGQUERY_DATASET")
    table = os.getenv("BIGQUERY_TABLE")
    
    # 1.3 Charger le fichier config.yaml
    with open('config.yaml') as f:
        config = yaml.safe_load(f)
        countries = config['countries']
        documents = config['documents']
    
    return {
        "api_key": api_key,
        "project_id": project_id,
        "dataset": dataset,
        "table": table,
        "countries": countries,
        "documents": documents
    }

# === Étape 2 : construire l'URL de l'API ===

def build_api_url(document_type, process_type, in_domain, period_start, period_end, api_key):
    """Construit l'URL de l'API ENTSO-E avec les paramètres"""
    
    base_url = "https://web-api.tp.entsoe.eu/api"
    
    url = f"{base_url}?documentType={document_type}&processType={process_type}&in_Domain={in_domain}&periodStart={period_start}&periodEnd={period_end}&securityToken={api_key}"
    
    return url

# === Étape 3 : Faire la requête HTTP ===

def fetch_data_from_api(url):
    """Fait une requête HTTP à l'API ENTSO-E et retourne les données"""
    
    print(f"🔄 Appel de l'API en cours...")
    
    response = requests.get(url)
    
    # Vérifier le statut de la réponse
    if response.status_code == 200:
        print("✅ Données récupérées avec succès !")
        return response.text  # Les données sont en format texte (XML)
    else:
        print(f"❌ Erreur {response.status_code}: {response.text}")
        return None
    
# === Étape 4 : Parser la réponse en DataFrame ===
