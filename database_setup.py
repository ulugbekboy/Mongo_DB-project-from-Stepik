from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from datetime import datetime, timedelta
from bson.objectid import ObjectId
from pprint import pprint


def connect_to_mongodb():
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client['onlineShop']
        print("✅ Успешное подключение к MongoDB!")
        return db
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return None


def create_collections(db):
    
    db.users.drop()
    db.products.drop()
    db.orders.drop()
    
    db.create_collection("users", validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['email', 'name', 'createdAt'],
            'properties': {
                'email': {'bsonType': 'string'},
                'name': {'bsonType': 'string', 'minLength': 2},
                'phone': {'bsonType': 'string'},
                'address': {'bsonType': 'object'},
                'createdAt': {'bsonType': 'date'},
                'lastLogin': {'bsonType': 'date'}
            }
        }
    })
    
    db.create_collection("products", validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['name', 'price', 'category', 'stock'],
            'properties': {
                'name': {'bsonType': 'string'},
                'description': {'bsonType': 'string'},
                'price': {'bsonType': ['double', 'int'], 'minimum': 0},
                'category': {'enum': ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']},
                'stock': {'bsonType': 'int', 'minimum': 0},
                'rating': {'bsonType': ['double', 'int'], 'minimum': 0, 'maximum': 5},
                'tags': {'bsonType': 'array'},
                'createdAt': {'bsonType': 'date'}
            }
        }
    })
    
    db.create_collection("orders", validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['userId', 'items', 'totalAmount', 'status', 'orderDate'],
            'properties': {
                'userId': {'bsonType': 'objectId'},
                'items': {'bsonType': 'array', 'minItems': 1},
                'totalAmount': {'bsonType': ['double', 'int'], 'minimum': 0},
                'status': {'enum': ['pending', 'processing', 'shipped', 'delivered', 'cancelled']},
                'orderDate': {'bsonType': 'date'},
                'shippingAddress': {'bsonType': 'object'},
                'deliveryDate': {'bsonType': 'date'}
            }
        }
    })
    
    print("✅ Коллекции созданы!")

def create_indexes(db):
    
    db.users.create_index([('email', ASCENDING)], unique=True)
    db.users.create_index([('createdAt', DESCENDING)])
    db.users.create_index([('address.city', ASCENDING)])
    
    db.products.create_index([('name', TEXT), ('description', TEXT)])
    db.products.create_index([('category', ASCENDING), ('price', ASCENDING)])
    db.products.create_index([('price', ASCENDING)])
    db.products.create_index([('rating', DESCENDING)])
    db.products.create_index([('tags', ASCENDING)])
    
    db.orders.create_index([('userId', ASCENDING), ('orderDate', DESCENDING)])
    db.orders.create_index([('status', ASCENDING)])
    db.orders.create_index([('orderDate', DESCENDING)])
    db.orders.create_index([('items.productId', ASCENDING)])
    
    print("✅ Индексы созданы!")


def populate_database(db):
    
    users_data = [
        {'email': 'ivan.petrov@example.com', 'name': 'Иван Петров', 
         'phone': '+7 900 123-45-67', 'address': {'city': 'Москва'}, 
         'createdAt': datetime(2024, 1, 15)},
        {'email': 'maria.ivanova@example.com', 'name': 'Мария Иванова', 
         'phone': '+7 900 234-56-78', 'address': {'city': 'Санкт-Петербург'}, 
         'createdAt': datetime(2024, 2, 20)},
        {'email': 'alex.smirnov@example.com', 'name': 'Александр Смирнов', 
         'phone': '+7 900 345-67-89', 'address': {'city': 'Екатеринбург'}, 
         'createdAt': datetime(2024, 3, 10)}
    ]
    
    result = db.users.insert_many(users_data)
    user_ids = result.inserted_ids
    print(f"✅ Добавлено {len(user_ids)} пользователей")
    
    products_data = [
        {'name': 'Ноутбук ASUS ROG', 'description': 'Игровой ноутбук', 
         'price': 89999.0, 'category': 'Electronics', 'stock': 15, 
         'rating': 4.7, 'tags': ['gaming', 'laptop'], 'createdAt': datetime(2024, 1, 1)},
        {'name': 'Смартфон Samsung S24', 'description': 'Флагман 2024', 
         'price': 79999.0, 'category': 'Electronics', 'stock': 30, 
         'rating': 4.8, 'tags': ['smartphone'], 'createdAt': datetime(2024, 2, 1)},
        {'name': "Джинсы Levi's 501", 'description': 'Классика', 
         'price': 5999.0, 'category': 'Clothing', 'stock': 50, 
         'rating': 4.5, 'tags': ['jeans'], 'createdAt': datetime(2024, 1, 15)},
        {'name': 'Книга Мастер и Маргарита', 'description': 'Булгаков', 
         'price': 499.0, 'category': 'Books', 'stock': 100, 
         'rating': 4.9, 'tags': ['book'], 'createdAt': datetime(2024, 1, 10)},
        {'name': 'Кофеварка DeLonghi', 'description': 'Автомат', 
         'price': 35999.0, 'category': 'Home', 'stock': 20, 
         'rating': 4.6, 'tags': ['coffee'], 'createdAt': datetime(2024, 2, 15)},
        {'name': 'Кроссовки Nike Air', 'description': 'Для бега', 
         'price': 8999.0, 'category': 'Sports', 'stock': 40, 
         'rating': 4.7, 'tags': ['shoes'], 'createdAt': datetime(2024, 3, 1)}
    ]
    
    result = db.products.insert_many(products_data)
    product_ids = result.inserted_ids
    print(f"✅ Добавлено {len(product_ids)} товаров")
    
    orders_data = [
        {'userId': user_ids[0], 
         'items': [{'productId': product_ids[0], 'quantity': 1, 'price': 89999.0}],
         'totalAmount': 89999.0, 'status': 'delivered', 
         'orderDate': datetime(2024, 10, 15)},
        {'userId': user_ids[1], 
         'items': [{'productId': product_ids[1], 'quantity': 1, 'price': 79999.0}],
         'totalAmount': 79999.0, 'status': 'shipped', 
         'orderDate': datetime(2024, 11, 10)},
        {'userId': user_ids[2], 
         'items': [{'productId': product_ids[4], 'quantity': 1, 'price': 35999.0}],
         'totalAmount': 35999.0, 'status': 'processing', 
         'orderDate': datetime(2024, 11, 13)},
        {'userId': user_ids[0], 
         'items': [{'productId': product_ids[3], 'quantity': 5, 'price': 499.0}],
         'totalAmount': 2495.0, 'status': 'pending', 
         'orderDate': datetime(2024, 11, 14)}
    ]
    
    result = db.orders.insert_many(orders_data)
    order_ids = result.inserted_ids
    print(f"✅ Добавлено {len(order_ids)} заказов")
    
    return user_ids, product_ids, order_ids

def crud_operations(db):
    
    print("\n" + "="*60)
    print("CREATE (Создание)")
    print("="*60)
    
    new_user = {
        'email': 'new.user@example.com',
        'name': 'Новый Пользователь',
        'phone': '+7 900 456-78-90',
        'address': {'city': 'Казань'},
        'createdAt': datetime.now()
    }
    result = db.users.insert_one(new_user)
    print(f"✅ Создан пользователь с ID: {result.inserted_id}")
    
    print("\n" + "="*60)
    print("READ (Чтение)")
    print("="*60)
    
    print("\n📍 Пользователи из Москвы:")
    for user in db.users.find({'address.city': 'Москва'}):
        print(f"  - {user['name']} ({user['email']})")
    
    print("\n💰 Товары дешевле 10000 руб:")
    for product in db.products.find({'price': {'$lt': 10000}}).sort('price', ASCENDING):
        print(f"  - {product['name']}: {product['price']} ₽")
    
    print("\n⭐ Электроника с рейтингом > 4.5:")
    for product in db.products.find({'category': 'Electronics', 'rating': {'$gt': 4.5}}):
        print(f"  - {product['name']}: {product['rating']}")
    
    print("\n" + "="*60)
    print("UPDATE (Обновление)")
    print("="*60)
    
    result = db.users.update_one(
        {'email': 'ivan.petrov@example.com'},
        {'$set': {'lastLogin': datetime.now()}}
    )
    print(f"✅ Обновлено {result.modified_count} пользователей")
    
    result = db.products.update_many(
        {'category': 'Electronics'},
        {'$mul': {'price': 1.1}}
    )
    print(f"✅ Обновлено {result.modified_count} товаров (цена +10%)")
    
    result = db.orders.update_one(
        {'status': 'pending'},
        {'$set': {'status': 'processing'}}
    )
    print(f"✅ Обновлен статус заказа")
    
    print("\n" + "="*60)
    print("DELETE (Удаление)")
    print("="*60)
    
    result = db.users.delete_one({'email': 'new.user@example.com'})
    print(f"✅ Удалено {result.deleted_count} пользователей")

def complex_queries(db):
    
    print("\n" + "="*60)
    print("СЛОЖНЫЕ ЗАПРОСЫ")
    print("="*60)
    
    print("\n💎 Топ-3 самых дорогих товара:")
    pipeline = [
        {'$sort': {'price': -1}},
        {'$limit': 3},
        {'$project': {'name': 1, 'price': 1}}
    ]
    for product in db.products.aggregate(pipeline):
        print(f"  - {product['name']}: {product['price']} ₽")
    
    print("\n💰 Сумма заказов по пользователям:")
    pipeline = [
        {'$group': {
            '_id': '$userId',
            'totalSpent': {'$sum': '$totalAmount'},
            'orderCount': {'$sum': 1}
        }},
        {'$sort': {'totalSpent': -1}}
    ]
    for result in db.orders.aggregate(pipeline):
        print(f"  - Пользователь {result['_id']}: {result['totalSpent']} ₽ ({result['orderCount']} заказов)")
    
    print("\n👤 Заказы с информацией о пользователях (JOIN):")
    pipeline = [
        {'$lookup': {
            'from': 'users',
            'localField': 'userId',
            'foreignField': '_id',
            'as': 'userInfo'
        }},
        {'$unwind': '$userInfo'},
        {'$project': {
            'totalAmount': 1,
            'status': 1,
            'userName': '$userInfo.name'
        }},
        {'$limit': 3}
    ]
    for order in db.orders.aggregate(pipeline):
        print(f"  - {order['userName']}: {order['totalAmount']} ₽ ({order['status']})")

def show_stats(db):
    
    print("\n" + "="*60)
    print("СТАТИСТИКА")
    print("="*60)
    
    for collection_name in ['users', 'products', 'orders']:
        stats = db.command('collStats', collection_name)
        print(f"\n{collection_name}:")
        print(f"  Документов: {stats['count']}")
        print(f"  Размер: {stats['size']} байт")
        print(f"  Индексов: {stats['nindexes']}")

def show_menu():
    print("\n" + "="*60)
    print("МЕНЮ")
    print("="*60)
    print("1. Показать всех пользователей")
    print("2. Показать все товары")
    print("3. Показать все заказы")
    print("4. Добавить пользователя")
    print("5. Выполнить сложные запросы")
    print("6. Показать статистику")
    print("0. Выход")
    print("="*60)

def interactive_menu(db):
    
    while True:
        show_menu()
        choice = input("\nВыберите действие: ")
        
        if choice == '1':
            print("\n👥 Список пользователей:")
            for user in db.users.find():
                print(f"  - {user['name']} ({user['email']}) - {user['address'].get('city', 'N/A')}")
        
        elif choice == '2':
            print("\n📦 Список товаров:")
            for product in db.products.find():
                print(f"  - {product['name']}: {product['price']} ₽ (склад: {product['stock']} шт)")
        
        elif choice == '3':
            print("\n🛒 Список заказов:")
            for order in db.orders.find():
                print(f"  - {order['totalAmount']} ₽ ({order['status']})")
        
        elif choice == '4':
            print("\n➕ Добавление пользователя")
            email = input("Email: ")
            name = input("Имя: ")
            city = input("Город: ")
            
            user = {
                'email': email,
                'name': name,
                'address': {'city': city},
                'createdAt': datetime.now()
            }
            
            try:
                result = db.users.insert_one(user)
                print(f"✅ Пользователь создан с ID: {result.inserted_id}")
            except Exception as e:
                print(f"❌ Ошибка: {e}")
        
        elif choice == '5':
            complex_queries(db)
        
        elif choice == '6':
            show_stats(db)
        
        elif choice == '0':
            print("\n👋 До свидания!")
            break
        
        else:
            print("❌ Неверный выбор")
        
        input("\nНажмите Enter для продолжения...")

def main():
    
    db = connect_to_mongodb()
    if db is None:
        return
    
    print("\n" + "="*60)
    print("ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ")
    print("="*60)
    
    create_collections(db)
    create_indexes(db)
    user_ids, product_ids, order_ids = populate_database(db)
    
    crud_operations(db)
    complex_queries(db)
    show_stats(db)
    interactive_menu(db)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Программа прервана")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()