# MongoDB Online Shop - Database Implementation

A MongoDB database project demonstrating schema design, indexing, CRUD operations, and aggregation pipelines using Python & PyMongo.
The Project was made for the NoSQL in STEPIK online learning platform - https://stepik.org/lesson/1237975/step/1?unit=1251713
## Tech Stack

- **Database**: MongoDB 6.0+
- **Language**: Python 3.8+
- **Driver**: PyMongo 4.6.1

## Architecture

### Collections & Schema

**users** (3 documents)
```python
{_id, email*, name*, phone, address{city}, createdAt*, lastLogin}
Indexes: email (unique), createdAt, address.city
```

**products** (6 documents)
```python
{_id, name*, price*, category*, stock*, rating, tags[], description, createdAt}
Indexes: (name, description) text, (category, price), rating, tags
```

**orders** (4 documents)
```python
{_id, userId*, items[{productId*, quantity*, price*}], totalAmount*, status*, orderDate*, shippingAddress}
Indexes: (userId, orderDate), status, items.productId
```

### Relationships
- `orders.userId` → `users._id` (1:N)
- `orders.items.productId` → `products._id` (N:M)

## Installation

```bash
# Install MongoDB
brew install mongodb-community  # macOS
sudo apt-get install mongodb    # Linux

# Start MongoDB
mongod

# Install dependencies
pip install pymongo

# Run project
python database_setup.py
```

## Features Implemented

### 1. Schema Validation
```python
validator = {'$jsonSchema': {'bsonType': 'object', 'required': [...]}}
db.create_collection("users", validator=validator)
```

### 2. Indexing (12 total)
- Unique: `users.email`
- Compound: `(category, price)`, `(userId, orderDate)`
- Text: `(name, description)`
- Single: `rating`, `status`, `tags`

### 3. CRUD Operations

**Create:**
```python
db.users.insert_one({...})
db.products.insert_many([...])
```

**Read:**
```python
db.users.find({'address.city': 'Moscow'})
db.products.find({'$text': {'$search': 'gaming'}})
db.orders.find({'status': {'$in': ['shipped', 'delivered']}})
```

**Update:**
```python
db.users.update_one({'email': 'user@email'}, {'$set': {'lastLogin': now}})
db.products.update_many({'category': 'Electronics'}, {'$mul': {'price': 1.1}})
db.orders.update_one({'status': 'pending'}, {'$set': {'status': 'processing'}})
```

**Delete:**
```python
db.users.delete_one({'email': 'user@email'})
db.products.delete_many({'stock': 0})
```

### 4. Aggregation Pipelines

**Top 3 Most Expensive Products:**
```python
pipeline = [
    {'$sort': {'price': -1}},
    {'$limit': 3}
]
```

**Total Spent per User:**
```python
pipeline = [
    {'$group': {
        '_id': '$userId',
        'totalSpent': {'$sum': '$totalAmount'},
        'orderCount': {'$sum': 1}
    }},
    {'$sort': {'totalSpent': -1}}
]
```

**Orders with User Info (JOIN):**
```python
pipeline = [
    {'$lookup': {
        'from': 'users',
        'localField': 'userId',
        'foreignField': '_id',
        'as': 'userInfo'
    }},
    {'$unwind': '$userInfo'}
]
```

**Popular Products:**
```python
pipeline = [
    {'$unwind': '$items'},
    {'$group': {
        '_id': '$items.productId',
        'totalSold': {'$sum': '$items.quantity'},
        'revenue': {'$sum': {'$multiply': ['$items.quantity', '$items.price']}}
    }},
    {'$sort': {'totalSold': -1}}
]
```

## Execution Results

### Database Statistics
```
users: 3 docs, 3 indexes, ~500 bytes
products: 6 docs, 5 indexes, ~1.2 KB
orders: 4 docs, 4 indexes, ~800 bytes
Total: 13 documents, 12 indexes
```

### Sample Data Summary
```
Users: 3 (Moscow, St. Petersburg, Yekaterinburg)
Products: 6 (Electronics: 2, Clothing: 1, Books: 1, Home: 1, Sports: 1)
Orders: 4 (delivered: 1, shipped: 1, processing: 1, pending: 1)
Total Revenue: ₽224,488
```

### Query Performance (with indexes)
```
Find by email: <1ms (unique index)
Text search: ~2ms (text index)
Find by userId: <1ms (compound index)
Category + price filter: ~1ms (compound index)
```

### Aggregation Results

**Top 3 Products by Price:**
1. ASUS ROG Laptop - ₽98,998.90 (after 10% increase)
2. Samsung Galaxy S24 - ₽87,998.90
3. DeLonghi Coffee Maker - ₽35,999

**User Spending:**
1. User 1: ₽93,492 (2 orders)
2. User 2: ₽85,998 (1 order)
3. User 3: ₽44,998 (1 order)

**Most Sold Products:**
1. Master and Margarita Book: 7 units sold
2. ASUS ROG Laptop: 1 unit sold
3. Samsung Galaxy S24: 1 unit sold

## File Structure
```
.
├── database_setup.py      # Main implementation
├── requirements.txt       # Dependencies
└── README.md             # Documentation
```

## Key Functions

```python
connect_to_mongodb()      # Connection handling
create_collections(db)    # Schema validation setup
create_indexes(db)        # Index creation
populate_database(db)     # Data insertion
crud_operations(db)       # CRUD demonstrations
complex_queries(db)       # Aggregation pipelines
show_stats(db)           # Performance metrics
interactive_menu(db)     # CLI interface
```

## Usage

```bash
python database_setup.py
```

Program automatically:
1. Creates collections with validation
2. Creates 12 indexes
3. Populates with sample data
4. Executes CRUD operations
5. Runs aggregation queries
6. Shows statistics
7. Launches interactive menu

## License

MIT
