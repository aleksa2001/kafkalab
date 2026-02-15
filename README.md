## Real-time Sentiment Analysis with Kafka

### Цель проекта:
Разработка системы реального времени для анализа тональности пользовательских отзывов с использованием Apache Kafka в качестве брокера сообщений и машинного обучения для классификации эмоциональной окраски текста.
### Описание работы:
Приложение имитирует потоковую обработку данных отзывов с различных платформ (Twitter, Yelp, IMDb, TripAdvisor и др.) через распределенную систему на основе Kafka. Каждый компонент системы выполняет специализированную задачу, обеспечивая сквозную обработку данных от получения до визуализации.
Архитектура потока данных: CSV Dataset → Kafka Producer → Kafka Broker → Consumer + ML Model → Dashboard
 ### Основные компоненты:
 #### 1. Data Preparation (prepare_data.py)
Загрузка и очистка CSV с отзывами

Преобразование текста (удаление стоп-слов, нормализация)

Создание уникальных идентификаторов и временных меток


Подготовка данных для потоковой отправки
#### 2. Kafka Producer (producer.py)
Чтение подготовленных данных

Отправка сообщений в топик Kafka raw_reviews

Имитация реального времени с рандомными задержками

Сериализация данных в JSON формат

#### 3. Kafka Consumer + ML Model (consumer.py, model.py)
Подписка на топик raw_reviews

Анализ тональности тремя методами:

VADER (правила для социальных медиа)

TextBlob (лексический анализ)

ML Model (Scikit-learn, ансамблевый метод)

Отправка обогащенных данных в топик processed_reviews

Сохранение статистики в реальном времени

#### 4. Real-time Dashboard (dashboard/app.py)
Streamlit веб-приложение

Визуализация в реальном времени:

Распределение тональности (положительная/отрицательная)

Точность моделей ML

Статистика по источникам и локациям

Графики активности по времени

Последние обработанные отзывы

 ## Локальное развертывание Kafka. 
 ### Шаг 1: Установка Kafka
  Скачайте Kafka с официального сайта


 Распакуйте архив
tar -xzf kafka_2.13-3.6.0.tgz
cd kafka_2.13-3.6.0
### Шаг 2: Запуск сервисов

#### Терминал 1: Запустите Zookeeper: <img width="974" height="561" alt="image" src="https://github.com/user-attachments/assets/0f0a5275-ac70-43ae-8a60-d25937d5cccf" />

#### Терминал 2: Запустите Kafka: <img width="974" height="524" alt="image" src="https://github.com/user-attachments/assets/7bcecb3c-74ea-459a-bdf6-b8d702d1fab5" />

#### Шаг 3: Создание топиков: <img width="974" height="303" alt="image" src="https://github.com/user-attachments/assets/db6ad2d9-55d8-4931-8f0a-6b73125cd7be" />

#### Шаг 4: Запуск приложения: 


 1. Подготовка данных <img width="1236" height="308" alt="image" src="https://github.com/user-attachments/assets/b4e40afc-e1b9-4069-892f-aec8f1aafce9" />


 2. Запустите Producer (Терминал 4) <img width="722" height="495" alt="image" src="https://github.com/user-attachments/assets/f88b14fc-b987-4b2e-a7bc-3c1889d90a02" />

 3. Запустите Consumer (Терминал 5) <img width="1131" height="996" alt="image" src="https://github.com/user-attachments/assets/3506dbc1-8693-4464-ad5a-06b453be3449" />

 4. Запустите Dashboard (Терминал 6): streamlit run src/dashboard/app.py

<img width="1088" height="605" alt="image" src="https://github.com/user-attachments/assets/c70902a7-44d0-4c58-a99d-fdae49f81585" />

### Доступ к приложению
Kafka Broker: localhost:9092

Streamlit Dashboard: http://localhost:8501

Zookeeper: localhost:2181

### Итоговый дашборд
<img width="1880" height="977" alt="image" src="https://github.com/user-attachments/assets/81d48eef-4a6d-4b6e-adf1-335e4e2e5581" />

 <img width="1117" height="404" alt="image" src="https://github.com/user-attachments/assets/88707069-fa9a-4808-bd86-4025ae41167a" />

 <img width="1646" height="736" alt="image" src="https://github.com/user-attachments/assets/87eac2fc-814f-44a7-bab3-b5fdd04a0fec" />

<img width="1654" height="571" alt="image" src="https://github.com/user-attachments/assets/c5c04b4f-4039-4b6d-9117-d2c581efb41d" />

<img width="1681" height="599" alt="image" src="https://github.com/user-attachments/assets/7f5fbec2-203e-4c70-abfa-a2b8c99b331e" />
