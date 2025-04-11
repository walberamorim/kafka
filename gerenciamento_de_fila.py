from kafka import KafkaClient, KafkaProducer, KafkaConsumer
from faker import Faker
import random
import json

ESPECIALIDADES = ['ortopedista', 'cardiologiasta', 'ginecologista', 'pediatra', 'neurologista']
TOTAL_PACIENTES = 20

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
            bootstrap_servers=['localhost:9092'],
            api_version=(0, 10, 1))
        cliente.add_topic('pacientes'),
        cliente.close(),
        iniciada = True
    except Exception as e:
        print(f"Erro ao conectar ao Kafka: {e}")
    return iniciada

def on_sucesso(mensagem):
    print(f'Sucesso ao enviar paciente: {mensagem}')

def on_erro(mensagem):
    print(f'Erro ao enviar paciente: {mensagem}')

def gerar_fila_atendimento():
        produtor = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            api_version=(0, 10, 1))
        for i in range(TOTAL_PACIENTES):
            paciente = gerar_paciente_fake()
            print(f"Paciente gerado: " + paciente['nome'] + " - " + paciente['especialidade'])
            envio = produtor.send(topic='pacientes', value=json.dumps(paciente).encode('utf-8'))
            envio.add_callback(on_sucesso).add_errback(on_erro)
            produtor.flush()
        produtor.close()

if __name__ == '__main__':
    iniciada = iniciar_fila_pacientes()
    if iniciada:
        gerar_fila_atendimento()