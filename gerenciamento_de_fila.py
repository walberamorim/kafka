from kafka import KafkaClient, KafkaProducer, KafkaConsumer
from faker import Faker
import random
import json

ESPECIALIDADES = ['ortopedista', 'cardiologiasta', 'ginecologista', 'pediatra', 'neurologista']

fake = Faker('pt_BR')

def gerar_paciente_fake():
    return {
        'cpf': fake.cpf(),
        'nome': fake.name(),
        'endereco': fake.address(),
        'idade': random.randint(14, 95),
        'especialidade': random.choice(ESPECIALIDADES),
    }

def iniciar_fila_pacientes():
    iniciada = False
    try:
        cliente = KafkaClient(
            bootstrap_servers='0.0.0.0:9092',
            api_version=(0, 11, 5))
        cliente.add_topic('pacientes'),
        cliente.close(),
        iniciada = True
    except Exception as e:
        print(f"Erro ao conectar ao Kafka: {e}")
    return iniciada

if __name__ == '__main__':
    paciente = gerar_paciente_fake()
    print(paciente)