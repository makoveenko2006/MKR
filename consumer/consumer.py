import json
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import sys
import signal

# Конфігурація
BOOTSTRAP_SERVERS = 'kafka:29092'  # для Docker, локально: 'localhost:9092'
REQUEST_TOPIC = 'demo-requests'
RESPONSE_TOPIC = 'demo-responses'
GROUP_ID = 'demo-responder-group'

def create_topics():
    """Створення топіків через адмін-клієнт"""
    from confluent_kafka.admin import AdminClient, NewTopic
    
    conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}
    admin_client = AdminClient(conf)
    
    topics = [REQUEST_TOPIC, RESPONSE_TOPIC]
    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics]
    
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
    original_n = n
    while n != 1:
        if n % 2 == 0:
            n = n // 2
        else:
            n = 3 * n + 1
        steps += 1
        # Захист від нескінченного циклу (на випадок помилок)
        if steps > 1000000:
            print(f" Перевищено ліміт кроків для {original_n}")
            return 0
    return steps

def calculate_avg_steps(start, finish):
    """Обчислює середню кількість кроків Коллатца для діапазону чисел"""
    total_steps = 0
    count = 0
    
    print(f" Обчислення для діапазону: [{start}, {finish}]")
    
    for num in range(start, finish + 1):
        steps = collatz_steps(num)
        total_steps += steps
        count += 1
        
        # Прогрес (кожні 10%)
        if count % ((finish - start + 1) // 10 + 1) == 0:
            progress = (count / (finish - start + 1)) * 100
            print(f"   Прогрес: {progress:.1f}% (оброблено {count} чисел)")
    
    avg_steps = total_steps / count if count > 0 else 0
    print(f" Результат: total_steps={total_steps}, count={count}, avg={avg_steps:.2f}")
    
    return round(avg_steps, 2)

def main():
    print("=" * 60)
    print("Kafka Request-Reply Consumer (Responder)")
    print("=" * 60)
    
    # Створюємо топіки
    create_topics()
    
    # Конфігурація consumer
    consumer_conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    }
    
    # Конфігурація producer для відповідей
    producer_conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}
    
    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    
    # Підписуємося на топік запитів
    consumer.subscribe([REQUEST_TOPIC])
    
    print(f"👂 Чекаю запитів у '{REQUEST_TOPIC}'...")
    print("=" * 60)
    
    def delivery_callback(err, msg):
        if err:
            print(f" Помилка доставки відповіді: {err}")
        else:
            print(f" Відповідь доставлено до {msg.topic()}/{msg.partition()}")
    
    # Обробка сигналів для graceful shutdown
    def signal_handler(sig, frame):
        print("\n Завершення роботи...")
        consumer.close()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Помилка consumer: {msg.error()}")
                    break
            
            # Отримуємо дані запиту
            try:
                request_data = msg.value().decode('utf-8')
                start_str, finish_str = request_data.split(',')
                start = int(start_str.strip())
                finish = int(finish_str.strip())
                
                print(f"\n Отримано запит: start={start} finish={finish}")
                
                # Отримуємо correlation-id з headers
                headers = msg.headers()
                correlation_id = None
                if headers:
                    for key, value in headers:
                        if key == 'correlation-id':
                            correlation_id = value.decode('utf-8')
                            break
                
                if not correlation_id:
                    print(" Запит без correlation-id, пропускаємо")
                    continue
                
                print(f" Correlation ID: {correlation_id}")
                
                # Обчислюємо середню кількість кроків
                avg_steps = calculate_avg_steps(start, finish)
                
                # Надсилаємо відповідь
                response_headers = [('correlation-id', correlation_id.encode('utf-8'))]
                
                producer.produce(
                    RESPONSE_TOPIC,
                    value=str(avg_steps).encode('utf-8'),
                    headers=response_headers,
                    callback=delivery_callback
                )
                producer.flush()
                
                print(f" Надіслано відповідь: avgSteps={avg_steps}")
                print("-" * 60)
                
            except ValueError as e:
                print(f" Помилка парсингу даних: {e}")
                continue
            except Exception as e:
                print(f" Помилка обробки: {e}")
                continue
                
    except KeyboardInterrupt:
        print("\n Завершення роботи...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()