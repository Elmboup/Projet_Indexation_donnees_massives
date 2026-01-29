FROM python:3.11-slim

WORKDIR /app

# Installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le script
COPY producer.py .

# Rendre le script exécutable
RUN chmod +x producer.py

CMD ["python", "-u", "producer.py"]