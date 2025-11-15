import pymongo # type: ignore
from faker import Faker # type: ignore
import random
from datetime import datetime, timedelta

# --- 0. Настройка подключения к MongoDB ---
# Замените строку подключения, если ваш сервер находится в другом месте
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "lab_work_db"

client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
fake = Faker('ru_RU') # Используем русский язык для генерации данных

# --------------------------------------------------
# 1. Создание базы данных и коллекций (происходит автоматически при первом использовании)
# --------------------------------------------------
def setup_collections():
    print(f"--- 1. & 2. Создание коллекций и определение схемы ---")
    # Коллекции: Товары, Категории, Заказы
    # MongoDB создает коллекции при первой вставке документа
    collections = ['products', 'categories', 'orders']
    for coll in collections:
        if coll not in db.list_collection_names():
            db.create_collection(coll)
            print(f"Коллекция '{coll}' создана.")
        else:
            print(f"Коллекция '{coll}' уже существует.")

# --------------------------------------------------
# 4. Создание индексов
# --------------------------------------------------
def create_indexes():
    print(f"\n--- 4. Создание индексов ---")
    # Индекс для поиска по имени продукта (текстовый поиск или обычный)
    db.products.create_index([("name", pymongo.ASCENDING)], unique=False)
    # Индекс для быстрого поиска заказов конкретного пользователя (связь)
    db.orders.create_index([("user_email", pymongo.ASCENDING)])
    # Индекс для фильтрации по дате заказа
    db.orders.create_index([("order_date", pymongo.DESCENDING)])
    print("Индексы созданы.")

# --------------------------------------------------
# 3. & 5. Определение схемы данных, связей и наполнение данными
# --------------------------------------------------
def seed_data():
    print(f"\n--- 5. Наполнение базы данных данными ---")
    if db.categories.count_documents({}) == 0:
        categories = ["Электроника", "Одежда", "Продукты питания", "Книги", "Бытовая техника"]
        category_docs = [{"name": cat} for cat in categories]
        db.categories.insert_many(category_docs)
        print(f"Добавлено {len(categories)} категорий.")
    
    if db.products.count_documents({}) == 0:
        # Получаем ObjectID категорий для связывания
        category_ids = [doc['_id'] for doc in db.categories.find({}, {'_id': 1})]
        products = []
        for _ in range(50):
            product = {
                "name": fake.word().capitalize() + " " + fake.word(),
                "description": fake.sentence(),
                "price": round(random.uniform(100.0, 50000.0), 2),
                # Связь с категорией (референс по ObjectID)
                "category_id": random.choice(category_ids), 
                "stock": random.randint(0, 500)
            }
            products.append(product)
        db.products.insert_many(products)
        print(f"Добавлено {len(products)} продуктов.")

    if db.orders.count_documents({}) == 0:
        orders = []
        for _ in range(30):
            order = {
                # Связь с пользователем (хоть и нет коллекции пользователей, используем email как ключ)
                "user_email": fake.email(), 
                "order_date": fake.date_time_between(start_date="-1y", end_date="now"),
                "total_amount": round(random.uniform(500.0, 150000.0), 2),
                "status": random.choice(["pending", "shipped", "delivered"]),
                # Встраиваем детали заказа/продуктов в заказ (предпочтительно для NoSQL)
                "items": [
                    {"product_name": fake.word(), "quantity": random.randint(1, 3), "price": round(random.uniform(100, 1000), 2)}
                    for _ in range(random.randint(1, 4))
                ]
            }
            orders.append(order)
        db.orders.insert_many(orders)
        print(f"Добавлено {len(orders)} заказов.")


# --------------------------------------------------
# 6. & 7. Написание и тестирование запросов (CRUD операции)
# --------------------------------------------------
def perform_queries():
    print(f"\n--- 6. & 7. Выполнение и тестирование запросов (CRUD) ---")

    # --- READ: Чтение данных (Поиск продуктов дороже 1000 руб.) ---
    print("\n[READ] Поиск 3 продуктов дороже 1000 руб.:")
    query = {"price": {"$gt": 1000.0}}
    results = db.products.find(query).limit(3)
    for prod in results:
        print(f"  - {prod['name']}: {prod['price']} руб.")

    # --- CREATE: Создание нового продукта ---
    print("\n[CREATE] Создание нового продукта 'Test Product 123':")
    new_product = {
        "name": "Test Product 123",
        "description": "A test product description.",
        "price": 99.99,
        "category_id": db.categories.find_one()['_id'],
        "stock": 10
    }
    insert_result = db.products.insert_one(new_product)
    print(f"  - Продукт создан с ID: {insert_result.inserted_id}")

    # --- UPDATE: Обновление созданного продукта ---
    print(f"\n[UPDATE] Обновление цены 'Test Product 123' на 129.99 руб.:")
    update_query = {"_id": insert_result.inserted_id}
    new_values = {"$set": {"price": 129.99}}
    db.products.update_one(update_query, new_values)
    
    # Проверка обновления
    updated_product = db.products.find_one(update_query)
    print(f"  - Новая цена продукта: {updated_product['price']} руб.")

    # --- DELETE: Удаление созданного продукта ---
    print(f"\n[DELETE] Удаление 'Test Product 123':")
    delete_query = {"_id": insert_result.inserted_id}
    delete_result = db.products.delete_one(delete_query)
    print(f"  - Продукт удален. Количество удаленных: {delete_result.deleted_count}")


    # --- Агрегация: Пример более сложного запроса ---
    print("\n[READ - Агрегация] Средняя цена продукта по категориям:")
    pipeline = [
        {"$group": {"_id": "$category_id", "avgPrice": {"$avg": "$price"}}}
    ]
    # Выполняем агрегацию и объединяем с именем категории через $lookup (хотя здесь мы просто выводим ID)
    avg_prices = db.products.aggregate(pipeline)
    for item in avg_prices:
        # Для отображения имени категории потребуется еще один $lookup или отдельный запрос
        print(f"  - Категория ID {item['_id']}: Средняя цена {item['avgPrice']:.2f}")


# --------------------------------------------------
# 8. Оптимизация запросов (Уже сделано)
# --------------------------------------------------
# Мы создали индексы в функции create_indexes(), что оптимизирует поиск по name, user_email и order_date.
def optimization_note():
    print("\n--- 8. Оптимизация запросов ---")
    print("Индексы были созданы для полей 'name', 'user_email', 'order_date'. Это ускоряет запросы фильтрации и поиска по этим полям.")


# --------------------------------------------------
# Главная функция запуска
# --------------------------------------------------
if __name__ == "__main__":
    try:
        setup_collections()
        create_indexes()
        seed_data()
        perform_queries()
        optimization_note()
        print("\nВсе шаги задания успешно выполнены.")
        client.close()
    except pymongo.errors.ConnectionFailure as e:
        print(f"\nОшибка подключения к MongoDB: {e}")
        print("Пожалуйста, убедитесь, что ваш сервер MongoDB запущен и строка подключения (MONGO_URI) верна.")

