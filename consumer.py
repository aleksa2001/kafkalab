from kafka import KafkaConsumer, KafkaProducer
import json
from model import SentimentAnalyzer
import pandas as pd
from datetime import datetime
import time
import threading


class SentimentConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', window_size=100):
        self.consumer = KafkaConsumer(
            'raw_reviews',
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='sentiment-analysis-group',
            enable_auto_commit=True
        )

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )

        self.model = SentimentAnalyzer()
        self.window_size = window_size
        self.processed_reviews = []
        self.statistics = {
            'total_processed': 0,
            'positive_count': 0,
            'negative_count': 0,
            'by_source': {},
            'by_location': {},
            'by_hour': {},
            'vader_accuracy': 0,
            'textblob_accuracy': 0,
            'ml_accuracy': 0,
            'avg_confidence': 0,
            'last_update': datetime.now().isoformat()
        }

        # Загрузка и обучение модели
        self.initialize_model()

    def initialize_model(self):
        """Инициализация и обучение модели"""
        try:
            df = pd.read_csv('data/reviews_processed.csv')
            accuracy = self.model.train_ml_model(df)
            self.statistics['ml_accuracy'] = float(accuracy)
            print("✅ Модель инициализирована и обучена")
        except Exception as e:
            print(f"⚠️ Ошибка обучения модели: {e}")
            print("✅ Использую предобученные анализаторы (VADER, TextBlob)")

    def analyze_sentiment(self, text):
        """Анализ тональности всеми методами"""
        # Используем ансамблевый метод
        sentiment, confidence = self.model.predict_ensemble(text)

        # Для сравнения получаем предсказания всех методов
        vader_sentiment, vader_conf = self.model.predict_vader(text)
        blob_sentiment, blob_conf = self.model.predict_textblob(text)
        ml_sentiment, ml_conf = self.model.predict_ml(text)

        return {
            'ensemble': {
                'sentiment': 'positive' if sentiment == 1 else 'negative',
                'confidence': confidence,
                'numeric': sentiment
            },
            'vader': {
                'sentiment': 'positive' if vader_sentiment == 1 else 'negative',
                'confidence': vader_conf,
                'numeric': vader_sentiment
            },
            'textblob': {
                'sentiment': 'positive' if blob_sentiment == 1 else 'negative',
                'confidence': blob_conf,
                'numeric': blob_sentiment
            },
            'ml': {
                'sentiment': 'positive' if ml_sentiment == 1 else 'negative',
                'confidence': ml_conf,
                'numeric': ml_sentiment
            }
        }

    def process_review(self, review):
        """Обработка одного отзыва"""
        # Анализ тональности
        sentiment_results = self.analyze_sentiment(review['text'])

        # Создание обогащенного сообщения
        processed_review = {
            **review,
            'sentiment_analysis': sentiment_results,
            'processing_time': datetime.now().isoformat(),
            'processing_duration': (datetime.now() - datetime.fromisoformat(
                review.get('producer_time', datetime.now().isoformat()))).total_seconds()
        }

        # Обновление статистики
        self.update_statistics(processed_review)

        # Отправка в processed_reviews
        try:
            self.producer.send('processed_reviews', value=processed_review)
        except Exception as e:
            print(f"❌ Ошибка отправки в processed_reviews: {e}")

        print(f"✅ Обработан: {review['review_id']}")
        print(f"   Текст: {review['text'][:60]}...")
        print(
            f"   Ансамбль: {sentiment_results['ensemble']['sentiment']} ({sentiment_results['ensemble']['confidence']:.2f})")
        print(f"   Истинная: {'positive' if review['true_sentiment'] == 1 else 'negative'}")

        return processed_review

    def update_statistics(self, review):
        """Обновление статистики"""
        self.statistics['total_processed'] += 1

        # Статистика по тональности
        sentiment = review['sentiment_analysis']['ensemble']['numeric']
        if sentiment == 1:
            self.statistics['positive_count'] += 1
        else:
            self.statistics['negative_count'] += 1

        # Статистика по источнику
        source = review['source']
        if source not in self.statistics['by_source']:
            self.statistics['by_source'][source] = {'positive': 0, 'negative': 0, 'total': 0}
        self.statistics['by_source'][source]['total'] += 1
        if sentiment == 1:
            self.statistics['by_source'][source]['positive'] += 1
        else:
            self.statistics['by_source'][source]['negative'] += 1

        # Статистика по локации
        location = review['location']
        if location not in self.statistics['by_location']:
            self.statistics['by_location'][location] = {'positive': 0, 'negative': 0, 'total': 0}
        self.statistics['by_location'][location]['total'] += 1
        if sentiment == 1:
            self.statistics['by_location'][location]['positive'] += 1
        else:
            self.statistics['by_location'][location]['negative'] += 1

        # Статистика по часам
        try:
            hour = datetime.fromisoformat(str(review['timestamp'])).hour
            hour_key = f"{hour:02d}:00"
            if hour_key not in self.statistics['by_hour']:
                self.statistics['by_hour'][hour_key] = {'count': 0, 'positive': 0}
            self.statistics['by_hour'][hour_key]['count'] += 1
            if sentiment == 1:
                self.statistics['by_hour'][hour_key]['positive'] += 1
        except:
            pass

        # Расчет точности моделей (если есть истинная тональность)
        true_sentiment = review.get('true_sentiment')
        if true_sentiment is not None:
            # VADER accuracy
            vader_correct = (review['sentiment_analysis']['vader']['numeric'] == true_sentiment)
            self.statistics['vader_accuracy'] = (
                    self.statistics.get('vader_accuracy', 0) * 0.9 + vader_correct * 0.1
            )

            # TextBlob accuracy
            blob_correct = (review['sentiment_analysis']['textblob']['numeric'] == true_sentiment)
            self.statistics['textblob_accuracy'] = (
                    self.statistics.get('textblob_accuracy', 0) * 0.9 + blob_correct * 0.1
            )

            # ML accuracy
            ml_correct = (review['sentiment_analysis']['ml']['numeric'] == true_sentiment)
            self.statistics['ml_accuracy'] = (
                    self.statistics.get('ml_accuracy', 0) * 0.9 + ml_correct * 0.1
            )

        # Средняя уверенность
        confidence = review['sentiment_analysis']['ensemble']['confidence']
        self.statistics['avg_confidence'] = (
                self.statistics.get('avg_confidence', 0) * 0.9 + confidence * 0.1
        )

        self.statistics['last_update'] = datetime.now().isoformat()

        # Сохранение в файл
        self.save_statistics()

    def save_statistics(self):
        """Сохранение статистики в JSON"""
        try:
            with open('data/statistics.json', 'w') as f:
                json.dump(self.statistics, f, indent=2, default=str)
        except Exception as e:
            print(f"⚠️ Ошибка сохранения статистики: {e}")

    def consume(self):
        """Основной цикл consumption"""
        print("🚀 Consumer запущен и ожидает сообщения...")
        print("   Топик: raw_reviews")
        print("   Группа: sentiment-analysis-group")
        print("-" * 50)

        try:
            for message in self.consumer:
                try:
                    review = message.value
                    self.process_review(review)

                    # Сохраняем в список последних отзывов
                    self.processed_reviews.append(review)
                    if len(self.processed_reviews) > self.window_size:
                        self.processed_reviews.pop(0)

                except Exception as e:
                    print(f"❌ Ошибка обработки сообщения: {e}")
                    continue

        except KeyboardInterrupt:
            print("\n🛑 Consumer остановлен пользователем")
        finally:
            self.consumer.close()
            self.producer.close()


if __name__ == "__main__":
    consumer = SentimentConsumer()
    consumer.consume()