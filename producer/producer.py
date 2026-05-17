
import uuid
import json
import time
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import sys

# Конфігурація
BOOTSTRAP_SERVERS = 'kafka:29092'  # для Docker, локально: 'localhost:9092'
REQUEST_TOPIC = 'demo-requests'
RESPONSE_TOPIC = 'demo-responses'
GROUP_ID = 'demo-producer-group'
TIMEOUT_SEC = 3000  # таймаут очікування відповіді

def create_topics():
    """Створення топіків через адмін-клієнт"""
    from confluent_kafka.admin import AdminClient, NewTopic
    
    conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}
    admin_client = AdminClient(conf)
    
    topics = [REQUEST_TOPIC, RESPONSE_TOPIC]
    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics]
    
    # Створюємо топіки (ігноруємо помилку, якщо вже існують)
    fs = admin_client.create_topics(new_topics, operation_timeout=10)
    for topic, f in fs.items():
        try:
            f.result()
            print(f" Топік '{topic}' створено")
        except Exception as e:
            if "already exists" in str(e):
                print(f"ℹ Топік '{topic}' вже існує")
            else:
                print(f" Помилка створення топіку '{topic}': {e}")

def collatz_steps(n):
    """Обчислює кількість кроків для числа n в послідовності Коллатца"""
    steps = 0
    while n != 1:
        if n % 2 == 0:
            n = n // 2
        else:
            n = 3 * n + 1
        steps += 1
    return steps

def send_request_and_wait(start, finish):
    """Надсилає запит і чекає відповіді"""
    
    # Створюємо топіки
    create_topics()
    
    # Генеруємо унікальний correlation ID
    correlation_id = str(uuid.uuid4())
    
    # Підписуємося на топік відповідей ДО надсилання запиту
    consumer_conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([RESPONSE_TOPIC])
    
    # Конфігурація продюсера
    producer_conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}
    producer = Producer(producer_conf)
    
    # Формуємо повідомлення
    message_value = f"{start},{finish}"
    
    # Надсилаємо запит з correlation-id в headers
    headers = [('correlation-id', correlation_id.encode('utf-8'))]
    
    def delivery_callback(err, msg):
        if err:
            print(f" Помилка доставки: {err}")
        else:
            print(f" Запит доставлено до {msg.topic()}/{msg.partition()}")
    
    # Асинхронне надсилання
    producer.produce(
        REQUEST_TOPIC,
        value=message_value.encode('utf-8'),
        headers=headers,
        callback=delivery_callback
    )
    producer.flush()
    
    print(f" Запит надіслано: start={start} finish={finish} (id={correlation_id})")
    
    # Чекаємо на відповідь
    start_time = time.time()
    while time.time() - start_time < TIMEOUT_SEC:
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Помилка consumer: {msg.error()}")
                break
        
        # Отримуємо correlation-id з headers відповіді
        response_headers = dict(msg.headers()) if msg.headers() else {}
        response_corr_id = response_headers.get('correlation-id', b'').decode('utf-8')
        
        if response_corr_id == correlation_id:
            # Знайшли свою відповідь
            avg_steps = msg.value().decode('utf-8')
            print(f"📥 Отримано відповідь: avgSteps={avg_steps}")
            consumer.close()
            return avg_steps
    
    consumer.close()
    print(f" Таймаут {TIMEOUT_SEC} секунд минув, відповідь не отримано")
    return None

def main():
    print("=" * 60)
    print("Kafka Request-Reply Producer")
    print("=" * 60)
    
    # Приклад: range від 10 до 100
    start = 10
    finish = 100
    
    # Можна додати цикл для відправки кількох запитів
    while True:
        try:
            result = send_request_and_wait(start, finish)
            if result:
                print(f" Результат: {result}")
            else:
                print(" Не вдалося отримати відповідь")
            
            print("\n" + "=" * 60)
            print("Готово. Контейнер живе. Чекаю наступного запиту...")
            print("=" * 60 + "\n")
            
            # Пауза перед наступним запитом
            time.sleep(10)
            
        except KeyboardInterrupt:
            print("\n Завершення роботи...")
            sys.exit(0)
        except Exception as e:
            print(f" Помилка: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()