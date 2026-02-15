import pandas as pd
import numpy as np
import re
from nltk.corpus import stopwords
import nltk
from datetime import datetime

# Скачиваем необходимые ресурсы NLTK
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

nltk.download('stopwords', quiet=True)
nltk.download('vader_lexicon', quiet=True)


def clean_text(text):
    """Очистка текста (адаптировано из вашего notebook)"""
    if pd.isna(text):
        return ""

    text = str(text).lower()
    # Убираем кавычки если они есть
    text = text.replace('"', '').replace("'", "")
    text = re.sub(r"[^\w\s]", " ", text)

    # Убираем стоп-слова
    STOPWORDS = set(stopwords.words("english"))
    text = ' '.join([w for w in text.split() if w not in STOPWORDS])

    return text.strip()


def prepare_dataset():
    """Подготовка датасета для использования в Kafka pipeline"""

    # Читаем CSV с правильной обработкой кавычек
    df = pd.read_csv(
        'data/sentiment-analysis.csv',
        quotechar='"',
        sep=","
    )

    # Если данные в одной колонке, разделяем их
    if df.shape[1] == 1:
        df = df.iloc[:, 0].str.split(',', expand=True)
        df.columns = ['Text', 'Sentiment', 'Source', 'Date/Time', 'User ID', 'Location', 'Confidence Score']

    # Очистка данных
    df = df.dropna(subset=['Text', 'Sentiment'])

    # Очищаем текст
    df['cleaned_text'] = df['Text'].apply(clean_text)

    # Преобразуем Sentiment в числовой формат
    sentiment_mapping = {
        'Positive': 1,
        'Negative': 0,
        ' Positive': 1,
        ' Negative': 0
    }

    df['sentiment_numeric'] = df['Sentiment'].map(sentiment_mapping)

    # Создаем review_id для уникальности
    df['review_id'] = [f'rev_{i:06d}' for i in range(len(df))]

    # Создаем timestamp если его нет
    if 'Date/Time' in df.columns:
        df['timestamp'] = pd.to_datetime(df['Date/Time'], errors='coerce')
    else:
        # Генерируем случайные даты в последний месяц
        end_date = datetime.now()
        start_date = end_date - pd.Timedelta(days=30)
        random_dates = pd.to_datetime(np.random.uniform(
            start_date.timestamp(),
            end_date.timestamp(),
            len(df)
        ), unit='s')
        df['timestamp'] = random_dates

    # Заполняем пропущенные значения
    if 'Source' not in df.columns:
        df['Source'] = 'Unknown'

    if 'Location' not in df.columns:
        df['Location'] = 'Unknown'

    if 'Confidence Score' not in df.columns:
        df['Confidence Score'] = np.random.uniform(0.6, 0.95, len(df))

    # Создаем рейтинг на основе тональности
    df['rating'] = df['sentiment_numeric'].apply(
        lambda x: np.random.randint(4, 6) if x == 1 else np.random.randint(1, 3)
    )

    # Фильтруем слишком короткие тексты
    df = df[df['cleaned_text'].str.len() > 10]

    # Сохраняем обработанные данные
    output_df = df[[
        'review_id',
        'Text',
        'cleaned_text',
        'sentiment_numeric',
        'Source',
        'Location',
        'Confidence Score',
        'timestamp',
        'rating'
    ]].copy()

    output_df.columns = [
        'review_id',
        'original_text',
        'text',
        'true_sentiment',
        'source',
        'location',
        'confidence_score',
        'timestamp',
        'rating'
    ]

    output_df.to_csv('data/reviews_processed.csv', index=False)
    print(f"✅ Подготовлено {len(output_df)} отзывов")
    print(f"📊 Распределение тональности:")
    print(f"   Положительные: {(output_df['true_sentiment'] == 1).sum()}")
    print(f"   Отрицательные: {(output_df['true_sentiment'] == 0).sum()}")

    return output_df


if __name__ == "__main__":
    df = prepare_dataset()
    print("\nПервые 5 записей:")
    print(df[['review_id', 'text', 'true_sentiment', 'source']].head())