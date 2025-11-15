import pymongo
from faker import Faker
import random
from datetime import datetime, timedelta

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "lab_work_db"

client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
fake = Faker('ru_RU')

def setup_collections():
    print(f"Создание коллекций и определение схемы")
    collections = ['products', 'categories', 'orders']
    for coll in collections:
        if coll not in db.list_collection_names():
            db.create_collection(coll)
            print(f"Коллекция '{coll}' создана.")
        else:
            print(f"Коллекция '{coll}' уже существует.")

def create_indexes():
    print(f"Создание индексов")
    db.products.create_index([("name", pymongo.ASCENDING)], unique=False)
    db.orders.create_index([("user_email", pymongo.ASCENDING)])
    db.orders.create_index([("order_date", pymongo.DESCENDING)])
    print("Индексы созданы.")

def seed_data():
    print(f"Наполнение базы данных данными")
    if db.categories.count_documents({}) == 0:
        categories = ["Электроника", "Одежда", "Продукты питания", "Книги", "Бытовая техника"]
        category_docs = [{"name": cat} for cat in categories]
        db.categories.insert_many(category_docs)
        print(f"Добавлено {len(categories)} категорий.")
    
    if db.products.count_documents({}) == 0:
        category_ids = [doc['_id'] for doc in db.categories.find({}, {'_id': 1})]
        products = []
        for _ in range(50):
            product = {
                "name": fake.word().capitalize() + " " + fake.word(),
                "description": fake.sentence(),
                "price": round(random.uniform(100.0, 50000.0), 2),
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
                "user_email": fake.email(), 
                "order_date": fake.date_time_between(start_date="-1y", end_date="now"),
                "total_amount": round(random.uniform(500.0, 150000.0), 2),
                "status": random.choice(["pending", "shipped", "delivered"]),
                "items": [
                    {"product_name": fake.word(), "quantity": random.randint(1, 3), "price": round(random.uniform(100, 1000), 2)}
                    for _ in range(random.randint(1, 4))
                ]
            }
            orders.append(order)
        db.orders.insert_many(orders)
        print(f"Добавлено {len(orders)} заказов.")

def perform_queries():
    print(f"Выполнение и тестирование запросов (CRUD)")

    print("\n[READ] Поиск 3 продуктов дороже 1000 руб.:")
    query = {"price": {"$gt": 1000.0}}
    results = db.products.find(query).limit(3)
    for prod in results:
        print(f"  - {prod['name']}: {prod['price']} руб.")

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

    print(f"\n[UPDATE] Обновление цены 'Test Product 123' на 129.99 руб.:")
    update_query = {"_id": insert_result.inserted_id}
    new_values = {"$set": {"price": 129.99}}
    db.products.update_one(update_query, new_values)
    
    updated_product = db.products.find_one(update_query)
    print(f"  - Новая цена продукта: {updated_product['price']} сум.")

    print(f"\n[DELETE] Удаление 'Test Product 123':")
    delete_query = {"_id": insert_result.inserted_id}
    delete_result = db.products.delete_one(delete_query)
    print(f"  - Продукт удален. Количество удаленных: {delete_result.deleted_count}")

    print("\n[READ - Агрегация] Средняя цена продукта по категориям:")
    pipeline = [
        {"$group": {"_id": "$category_id", "avgPrice": {"$avg": "$price"}}}
    ]
    avg_prices = db.products.aggregate(pipeline)
    for item in avg_prices:
        print(f"  - Категория ID {item['_id']}: Средняя цена {item['avgPrice']:.2f}")


def optimization_note():
    print("Оптимизация запросов")
    print("Индексы были созданы для полей 'name', 'user_email', 'order_date'. Это ускоряет запросы фильтрации и поиска по этим полям.")

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

