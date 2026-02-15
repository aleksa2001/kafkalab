import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import time
from datetime import datetime
import threading
import queue
import sys
import os

# Настройка страницы
st.set_page_config(
    page_title="Real-time Sentiment Analysis Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Инициализация состояния
if 'statistics' not in st.session_state:
    st.session_state.statistics = {}
if 'recent_reviews' not in st.session_state:
    st.session_state.recent_reviews = []
if 'kafka_messages' not in st.session_state:
    st.session_state.kafka_messages = []
if 'last_update' not in st.session_state:
    st.session_state.last_update = datetime.now()


class KafkaDashboardStream:
    def __init__(self):
        self.queue = queue.Queue()
        self.running = True
        self.consumer = None

    def start_consumer(self):
        """Запуск Kafka consumer в отдельном потоке"""

        def consume():
            try:
                from kafka import KafkaConsumer
                self.consumer = KafkaConsumer(
                    'processed_reviews',
                    bootstrap_servers='localhost:9092',
                    auto_offset_reset='latest',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    group_id='dashboard-group',
                    consumer_timeout_ms=1000
                )

                for message in self.consumer:
                    if not self.running:
                        break
                    self.queue.put(message.value)
            except Exception as e:
                print(f"Kafka connection error: {e}")
                time.sleep(2)

        thread = threading.Thread(target=consume, daemon=True)
        thread.start()
        return thread

    def stop(self):
        self.running = False
        if self.consumer:
            self.consumer.close()


# Инициализация стрима
try:
    kafka_stream = KafkaDashboardStream()
    kafka_thread = kafka_stream.start_consumer()
    kafka_available = True
except:
    kafka_available = False

# Заголовок
st.title(" Real-time Sentiment Analysis Dashboard")
st.markdown("Анализ тональности отзывов в реальном времени с использованием Kafka и ML")

# Сайдбар
with st.sidebar:
    st.header("⚙️ Настройки")

    auto_refresh = st.checkbox("Автообновление", value=True)
    refresh_interval = st.slider("Интервал обновления (сек)", 1, 30, 5)

    st.header("📈 Метрики моделей")

    # Загрузка статистики из файла
    try:
        with open('data/statistics.json', 'r') as f:
            stats = json.load(f)

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("VADER", f"{stats.get('vader_accuracy', 0) * 100:.1f}%")
            with col2:
                st.metric("TextBlob", f"{stats.get('textblob_accuracy', 0) * 100:.1f}%")
            with col3:
                st.metric("ML Model", f"{stats.get('ml_accuracy', 0) * 100:.1f}%")

            st.metric("Ансамбль", f"{stats.get('avg_confidence', 0) * 100:.1f}%")
    except:
        st.info("Ожидаю данные...")

    if st.button("🔄 Обновить вручную"):
        st.rerun()

    st.markdown("---")
    st.info(f"Kafka: {' Доступен' if kafka_available else ' Не доступен'}")

# Основные метрики
st.header(" Основные метрики")

col1, col2, col3, col4 = st.columns(4)

try:
    with open('data/statistics.json', 'r') as f:
        stats = json.load(f)

        with col1:
            st.metric("Всего обработано", stats.get('total_processed', 0))
        with col2:
            positive = stats.get('positive_count', 0)
            total = max(stats.get('total_processed', 1), 1)
            positive_rate = (positive / total) * 100
            st.metric("Положительных", f"{positive_rate:.1f}%")
        with col3:
            negative = stats.get('negative_count', 0)
            negative_rate = (negative / total) * 100
            st.metric("Отрицательных", f"{negative_rate:.1f}%")
        with col4:
            avg_conf = stats.get('avg_confidence', 0) * 100
            st.metric("Уверенность", f"{avg_conf:.1f}%")
except:
    col1.metric("Всего обработано", "0")
    col2.metric("Положительных", "0%")
    col3.metric("Отрицательных", "0%")
    col4.metric("Confidence", "0%")

st.markdown("---")

# Основная часть - два столбца
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader(" Распределение тональности")

    try:
        with open('data/statistics.json', 'r') as f:
            stats = json.load(f)

            # Круговая диаграмма тональности
            positive = stats.get('positive_count', 0)
            negative = stats.get('negative_count', 0)

            if positive + negative > 0:
                sentiment_data = pd.DataFrame({
                    'Sentiment': ['Positive', 'Negative'],
                    'Count': [positive, negative],
                    'Color': ['#2E8B57', '#DC143C']
                })

                fig1 = px.pie(sentiment_data, values='Count', names='Sentiment',
                              color='Sentiment',
                              color_discrete_map={'Positive': '#2E8B57', 'Negative': '#DC143C'},
                              hole=0.3)
                fig1.update_layout(showlegend=True, height=400)
                st.plotly_chart(fig1, use_container_width=True)
            else:
                st.info("Нет данных для отображения")
    except Exception as e:
        st.info("Ожидаю данные...")

    # График точности моделей
    st.subheader(" Точность моделей")

    try:
        with open('data/statistics.json', 'r') as f:
            stats = json.load(f)

            models_data = pd.DataFrame({
                'Model': ['VADER', 'TextBlob', 'ML Model'],
                'Accuracy': [
                    stats.get('vader_accuracy', 0) * 100,
                    stats.get('textblob_accuracy', 0) * 100,
                    stats.get('ml_accuracy', 0) * 100
                ]
            })

            fig2 = px.bar(models_data, x='Model', y='Accuracy',
                          color='Model',
                          color_discrete_sequence=['#FF6B6B', '#4ECDC4', '#45B7D1'])
            fig2.update_layout(
                yaxis_range=[0, 100],
                yaxis_title="Accuracy (%)",
                height=300
            )
            st.plotly_chart(fig2, use_container_width=True)
    except:
        st.info("Нет данных о точности моделей")

with col_right:
    st.subheader("🔄 Последние отзывы")

    # Обработка новых сообщений из Kafka
    new_messages = []
    if kafka_available:
        while True:
            try:
                message = kafka_stream.queue.get_nowait()
                new_messages.append(message)
                st.session_state.kafka_messages.append(message)

                # Ограничиваем историю
                if len(st.session_state.kafka_messages) > 50:
                    st.session_state.kafka_messages = st.session_state.kafka_messages[-50:]

            except queue.Empty:
                break

    # Отображение последних сообщений
    if st.session_state.kafka_messages:
        for i, msg in enumerate(st.session_state.kafka_messages[-5:][::-1]):
            with st.container():
                sentiment = msg.get('sentiment_analysis', {}).get('ensemble', {}).get('sentiment', 'unknown')
                confidence = msg.get('sentiment_analysis', {}).get('ensemble', {}).get('confidence', 0)
                source = msg.get('source', 'Unknown')
                text = msg.get('text', '')[:80] + '...' if len(msg.get('text', '')) > 80 else msg.get('text', '')

                # Цвет в зависимости от тональности
                if sentiment == 'positive':
                    color = "🟢"
                    border_color = "#2E8B57"
                else:
                    color = "🔴"
                    border_color = "#DC143C"

                st.markdown(f"""
                <div style='border-left: 4px solid {border_color}; padding-left: 10px; margin: 5px 0; padding: 10px; background-color: #f8f9fa; border-radius: 5px;'>
                <b>{color} {source}</b><br>
                {text}<br>
                <small>Тональность: <b>{sentiment}</b> ({confidence:.2f})</small>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("Нет новых отзывов")

        # Покажем пример из статистики если есть
        try:
            with open('data/statistics.json', 'r') as f:
                stats = json.load(f)
                if stats.get('total_processed', 0) > 0:
                    st.success(f" Обработано {stats.get('total_processed', 0)} отзывов")
        except:
            pass

st.markdown("---")

# Дополнительные графики
st.subheader(" Детальная аналитика")

tab1, tab2, tab3 = st.tabs([" По источникам", " По локациям", " По времени"])

with tab1:
    try:
        with open('data/statistics.json', 'r') as f:
            stats = json.load(f)

            if 'by_source' in stats and stats['by_source']:
                sources_data = []
                for source, data in stats['by_source'].items():
                    total = data.get('total', 0)
                    if total > 0:
                        positive_rate = (data.get('positive', 0) / total) * 100
                        sources_data.append({
                            'Source': source,
                            'Total': total,
                            'Positive Rate': positive_rate
                        })

                if sources_data:
                    df_sources = pd.DataFrame(sources_data)
                    fig3 = px.bar(df_sources, x='Source', y='Positive Rate',
                                  color='Total',
                                  color_continuous_scale='Viridis',
                                  title="Процент положительных отзывов по источникам")
                    fig3.update_layout(
                        yaxis_title="Positive Rate (%)",
                        yaxis_range=[0, 100],
                        height=400
                    )
                    st.plotly_chart(fig3, use_container_width=True)
                else:
                    st.info("Нет данных по источникам")
    except:
        st.info("Нет данных по источникам")

with tab2:
    try:
        with open('data/statistics.json', 'r') as f:
            stats = json.load(f)

            if 'by_location' in stats and stats['by_location']:
                locations_data = []
                for location, data in stats['by_location'].items():
                    total = data.get('total', 0)
                    if total > 0:
                        positive = data.get('positive', 0)
                        locations_data.append({
                            'Location': location,
                            'Positive': positive,
                            'Total': total
                        })

                if locations_data:
                    df_locations = pd.DataFrame(locations_data)
                    df_locations['Negative'] = df_locations['Total'] - df_locations['Positive']

                    fig4 = go.Figure(data=[
                        go.Bar(name='Positive', x=df_locations['Location'],
                               y=df_locations['Positive'], marker_color='#2E8B57'),
                        go.Bar(name='Negative', x=df_locations['Location'],
                               y=df_locations['Negative'], marker_color='#DC143C')
                    ])
                    fig4.update_layout(
                        barmode='stack',
                        title="Распределение тональности по локациям",
                        height=400
                    )
                    st.plotly_chart(fig4, use_container_width=True)
                else:
                    st.info("Нет данных по локациям")
    except:
        st.info("Нет данных по локациям")

with tab3:
    try:
        with open('data/statistics.json', 'r') as f:
            stats = json.load(f)

            if 'by_hour' in stats and stats['by_hour']:
                hours = sorted(stats['by_hour'].keys())
                counts = [stats['by_hour'][h]['count'] for h in hours]
                positive_counts = [stats['by_hour'][h]['positive'] for h in hours]

                if sum(counts) > 0:
                    fig5 = make_subplots(specs=[[{"secondary_y": True}]])
                    fig5.add_trace(
                        go.Bar(name='Всего отзывов', x=hours, y=counts, marker_color='#4682B4'),
                        secondary_y=False
                    )

                    # Расчет процента положительных
                    positive_rates = []
                    for h in hours:
                        total = stats['by_hour'][h]['count']
                        positive = stats['by_hour'][h]['positive']
                        rate = (positive / total * 100) if total > 0 else 0
                        positive_rates.append(rate)

                    fig5.add_trace(
                        go.Scatter(name='% Положительных', x=hours, y=positive_rates,
                                   mode='lines+markers', line=dict(color='#2E8B57', width=3)),
                        secondary_y=True
                    )

                    fig5.update_layout(
                        title="Активность по часам",
                        height=400
                    )
                    fig5.update_yaxes(title_text="Количество отзывов", secondary_y=False)
                    fig5.update_yaxes(title_text="% Положительных", secondary_y=True, range=[0, 100])
                    st.plotly_chart(fig5, use_container_width=True)
                else:
                    st.info("Нет данных по времени")
    except:
        st.info("Нет данных по времени")

# Статус
st.markdown("---")
status_col1, status_col2 = st.columns([3, 1])

with status_col1:
    if new_messages:
        st.success(f" Получено {len(new_messages)} новых сообщений из Kafka")
    else:
        st.info(" Ожидаю новые сообщения из Kafka...")

with status_col2:
    last_update = st.session_state.last_update
    st.caption(f"Последнее обновление: {last_update.strftime('%H:%M:%S')}")

# Автообновление
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()


# Завершение работы при закрытии
def cleanup():
    if kafka_available:
        kafka_stream.stop()


import atexit

atexit.register(cleanup)