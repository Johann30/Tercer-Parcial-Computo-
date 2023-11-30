from confluent_kafka import Producer
import pandas as pd
import json

# Configuración del productor de Kafka
producer_conf = {
    'bootstrap.servers': 'localhost:9092',  
}

# Crea una instancia del productor
producer = Producer(producer_conf)

# Nombre del topic en Kafka
topic_name = 'cacao_topic'

# Ruta del archivo CSV
csv_file = 'cacao.csv'

# Función de manejo de entrega de mensajes
def delivery_report(err, msg):
    if err is not None:
        print(f'Error al entregar el mensaje: {err}')
    else:
        print(f'Mensaje entregado: {msg.value()}')

# Función para enviar datos desde el archivo CSV
def send_data_from_csv():
    data = pd.read_csv(csv_file)
    for _, row in data.iterrows():
        row_dict = row.to_dict()
        json_row = json.dumps(row_dict)
        producer.produce(topic_name, json_row.encode('utf-8'), callback=delivery_report)

# Función para enviar datos ingresados manualmente
def send_manual_data():
    while True:
        print("Ingrese los datos ('Company', 'Specific Bean Origin', 'REF', 'Review Date', 'Cocoa Percent', 'Company Location', 'Rating', 'Bean Type', 'Broad Bean Origin'), o escriba 'exit' para salir:")
        user_input = input()
        
        if user_input.lower() == 'exit':
            break
        
        producer.produce(topic_name, user_input.encode('utf-8'), callback=delivery_report)

# Enviar datos desde el archivo CSV
send_data_from_csv()

# Permitir ingreso manual de datos adicionales
send_manual_data()

# Esperar a que todos los mensajes sean entregados o haya un error
producer.flush()