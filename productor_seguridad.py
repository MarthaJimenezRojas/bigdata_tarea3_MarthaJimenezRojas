from kafka import KafkaProducer
import time
import json
import random

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

municipios = ["Yopal", "Aguazul", "Villanueva", "Tauramena", "Paz de Ariporo"]
generos = ["Femenino", "Masculino"]
beneficios = ["Huerta de autoconsumo", "Paquete nutricional", "Capacitación en nutrición"]

while True:
    data = {
        "municipio": random.choice(municipios),
        "genero": random.choice(generos),
        "beneficio": random.choice(beneficios)
    }
    producer.send('seguridad_topic', data)
    print(f"Enviado: {data}")
    time.sleep(2)
