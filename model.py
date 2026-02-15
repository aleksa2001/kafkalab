import pickle
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import accuracy_score
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from textblob import TextBlob
import nltk
import re
from nltk.corpus import stopwords


class SentimentAnalyzer:
    """Класс для анализа тональности с несколькими методами"""

    def __init__(self):
        self.vectorizer = TfidfVectorizer(max_features=1000)
        self.ml_model = GaussianNB()  # По умолчанию используем Naive Bayes
        self.vader_analyzer = SentimentIntensityAnalyzer()
        self.stopwords = set(stopwords.words("english"))
        self.is_trained = False

    def clean_text(self, text):
        """Очистка текста"""
        if pd.isna(text):
            return ""

        text = str(text).lower()
        text = re.sub(r"[^\w\s]", " ", text)
        text = ' '.join([w for w in text.split() if w not in self.stopwords])
        return text.strip()

    def train_ml_model(self, df):
        """Обучение ML модели на данных"""
        # Очищаем текст
        df['cleaned'] = df['text'].apply(self.clean_text)

        # Преобразуем текст в векторы
        X = self.vectorizer.fit_transform(df['cleaned']).toarray()
        y = df['true_sentiment'].values

        # Обучаем модель
        self.ml_model.fit(X, y)
        self.is_trained = True

        # Оцениваем точность
        y_pred = self.ml_model.predict(X)
        accuracy = accuracy_score(y, y_pred)
        print(f"✅ ML модель обучена. Точность: {accuracy:.2%}")

        return accuracy

    def predict_vader(self, text):
        """Предсказание с помощью VADER"""
        cleaned = self.clean_text(text)
        scores = self.vader_analyzer.polarity_scores(cleaned)

        if scores['pos'] >= scores['neg']:
            sentiment = 1  # Positive
            confidence = scores['pos']
        else:
            sentiment = 0  # Negative
            confidence = scores['neg']

        return sentiment, float(confidence)

    def predict_textblob(self, text):
        """Предсказание с помощью TextBlob"""
        cleaned = self.clean_text(text)
        sentiment_score = TextBlob(cleaned).sentiment.polarity

        if sentiment_score >= 0:
            sentiment = 1  # Positive
        else:
            sentiment = 0  # Negative

        confidence = abs(sentiment_score)
        return sentiment, float(confidence)

    def predict_ml(self, text):
        """Предсказание с помощью ML модели"""
        if not self.is_trained:
            return 1, 0.5  # По умолчанию positive с низкой уверенностью

        cleaned = self.clean_text(text)
        X = self.vectorizer.transform([cleaned]).toarray()

        try:
            sentiment = int(self.ml_model.predict(X)[0])
            proba = self.ml_model.predict_proba(X)[0]
            confidence = float(max(proba))
        except:
            sentiment = 1
            confidence = 0.5

        return sentiment, confidence

    def predict_ensemble(self, text):
        """Ансамблевое предсказание (взвешенное голосование)"""
        # Получаем предсказания от всех методов
        vader_sentiment, vader_conf = self.predict_vader(text)
        blob_sentiment, blob_conf = self.predict_textblob(text)
        ml_sentiment, ml_conf = self.predict_ml(text)

        # Взвешенное голосование
        predictions = [vader_sentiment, blob_sentiment, ml_sentiment]
        weights = [vader_conf, blob_conf, ml_conf]

        # Взвешенное среднее
        weighted_sum = sum(p * w for p, w in zip(predictions, weights))
        total_weight = sum(weights)

        if total_weight > 0:
            sentiment = 1 if weighted_sum / total_weight >= 0.5 else 0
            confidence = total_weight / 3  # Нормализованная уверенность
        else:
            sentiment = 1
            confidence = 0.5

        return sentiment, confidence

    def save(self, path='sentiment_model.pkl'):
        """Сохранение модели"""
        with open(path, 'wb') as f:
            pickle.dump({
                'vectorizer': self.vectorizer,
                'model': self.ml_model,
                'is_trained': self.is_trained
            }, f)

    def load(self, path='sentiment_model.pkl'):
        """Загрузка модели"""
        with open(path, 'rb') as f:
            data = pickle.load(f)
            self.vectorizer = data['vectorizer']
            self.model = data['model']
            self.is_trained = data['is_trained']