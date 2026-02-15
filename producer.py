from kafka import KafkaProducer
import pandas as pd
import json
import time
import random
from datetime import datetime


class SentimentProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            acks='all',
            retries=3
        )
        self.topic = 'raw_reviews'

    def send_review(self, review):
        """Отправка одного отзыва"""
        try:
            self.producer.send(self.topic, value=review)
            print(f"📨 Отправлен: {review['review_id']} - {review['text'][:50]}...")
            return True
        except Exception as e:
            print(f"❌ Ошибка отправки: {e}")
            return False

    def stream_from_csv(self, csv_path='data/reviews_processed.csv', delay_range=(0.1, 1.0)):
        """Потоковая отправка отзывов из CSV"""
        try:
            df = pd.read_csv(csv_path)
            print(f"📂 Загружено {len(df)} отзывов из {csv_path}")
        except Exception as e:
            print(f"❌ Ошибка загрузки CSV: {e}")
            return

        print(f"🚀 Начинаю потоковую отправку {len(df)} отзывов...")

        for idx, row in df.iterrows():
            review = {
                'review_id': row['review_id'],
                'original_text': row['original_text'],
                'text': row['text'],
                'true_sentiment': int(row['true_sentiment']),
                'source': row['source'],
                'location': row['location'],
                'confidence_score': float(row['confidence_score']),
                'timestamp': row['timestamp'],
                'rating': int(row['rating']),
                'producer_time': datetime.now().isoformat()
            }

            self.send_review(review)

            # Случайная задержка для имитации реального времени
            delay = random.uniform(*delay_range)
            time.sleep(delay)

            # Прогресс
            if (idx + 1) % 10 == 0:
                print(f"📊 Прогресс: {idx + 1}/{len(df)} ({((idx + 1) / len(df) * 100):.1f}%)")

        print("✅ Все отзывы отправлены")
        self.producer.flush()
        self.producer.close()


if __name__ == "__main__":
    producer = SentimentProducer()
    producer.stream_from_csv(delay_range=(0.2, 0.5))