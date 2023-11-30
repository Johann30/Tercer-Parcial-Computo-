from confluent_kafka import Consumer

# Configuración del consumidor de Kafka
consumer_conf = {
    'bootstrap.servers': 'localhost:9092', 
    'group.id': 'cacao_consumer_group',  # Identificador del grupo de consumidores
    'auto.offset.reset': 'earliest'  # Comenzar desde el offset más antiguo
}

# Crea una instancia del consumidor
consumer = Consumer(consumer_conf)

# Suscripción al topic de Kafka
topic_name = 'cacao_topic'  
consumer.subscribe([topic_name])

try:
    while True:
        # Espera por mensajes
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Error al recibir el mensaje: {msg.error()}")
            break

        # Procesamiento del mensaje recibido
        data = msg.value().decode('utf-8')
        print(f"Mensaje recibido: {data}")

except KeyboardInterrupt:
    pass

finally:
    # Cierra el consumidor
    consumer.close()