import json
import time
import os
import requests
from kafka import KafkaProducer
from datetime import datetime

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'worldbank-data')
REFRESH_INTERVAL = int(os.getenv('REFRESH_INTERVAL', 60))

# Countries to monitor (ISO 3166-1 alpha-3 codes)
COUNTRIES = ['PH', 'US', 'CN', 'JP', 'IN', 'DE', 'BR', 'GB', 'FR', 'ID']

# World Bank indicators
INDICATORS = {
    'SP.POP.TOTL': 'Total Population',
    'NY.GDP.MKTP.CD': 'GDP (current US$)',
    'SP.DYN.LE00.IN': 'Life Expectancy at Birth',
    'NY.GDP.PCAP.CD': 'GDP per Capita (current US$)',
    'SL.UEM.TOTL.ZS': 'Unemployment Rate'
}

def fetch_indicator(country, indicator_code):
    """Fetch latest available data for a specific country and indicator."""
    url = f"http://api.worldbank.org/v2/country/{country}/indicator/{indicator_code}"
    params = {
        'format': 'json',
        'per_page': 5,
        'mrv': 1  # Most recent value only
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if len(data) < 2 or not data[1]:
            return None
            
        for entry in data[1]:
            if entry['value'] is not None:
                return {
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                    'country_code': country,
                    'country_name': entry['country']['value'],
                    'indicator_code': indicator_code,
                    'indicator_name': INDICATORS.get(indicator_code, indicator_code),
                    'year': entry['date'],
                    'value': float(entry['value']),
                    'unit': data[0].get('unit', ''),
                    'source': 'World Bank API',
                    'sensor_id': f"wb-{country}-{indicator_code}",
                    'metric_type': 'development_indicator'
                }
        return None
    except Exception as e:
        print(f"Error fetching {indicator_code} for {country}: {e}")
        return None

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# --- Producer Setup with Retry ---
max_retries = 12
retry_delay = 5

for attempt in range(max_retries):
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=json_serializer
        )
        print(f"Connected to Kafka at {KAFKA_BROKER}")
        break
    except Exception as e:
        print(f"Attempt {attempt+1}/{max_retries}: Kafka not ready. Retrying in {retry_delay}s...")
        time.sleep(retry_delay)
else:
    print("Failed to connect to Kafka. Exiting.")
    exit(1)

print(f"Starting World Bank data producer. Publishing to {TOPIC_NAME} every {REFRESH_INTERVAL}s...")
print(f"Countries: {COUNTRIES}")
print(f"Indicators: {list(INDICATORS.keys())}")

try:
    while True:
        count = 0
        for country in COUNTRIES:
            for indicator_code in INDICATORS.keys():
                record = fetch_indicator(country, indicator_code)
                if record:
                    producer.send(TOPIC_NAME, value=record)
                    print(f"Sent: {record['country_name']} - {INDICATORS[indicator_code]}: {record['value']}")
                    count += 1
                time.sleep(0.2)  # Rate limiting to avoid API throttling
        
        print(f"Published {count} records. Waiting {REFRESH_INTERVAL}s...")
        time.sleep(REFRESH_INTERVAL)
except KeyboardInterrupt:
    print("\nShutting down producer...")
finally:
    producer.close()