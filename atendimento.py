from kafka import KafkaConsumer, TopicPartition
from time import sleep
import json

def atender_pacientes():
    consumidor = KafkaConsumer(
        'pacientes',
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 1),
        consumer_timeout_ms=5000,
        auto_offset_reset='earliest')
    while True:
        contador = 0
        for atendimento in consumidor:
            if atendimento:
                paciente = json.loads(atendimento.value.decode('utf-8'))
                print(f"Paciente sendo atendido: " + paciente['nome'] + " - " + paciente['especialidade'])
                contador += 1
        print(f"Total de pacientes atendidos: {contador}")
        sleep(5)

if __name__ == '__main__':
    atender_pacientes()