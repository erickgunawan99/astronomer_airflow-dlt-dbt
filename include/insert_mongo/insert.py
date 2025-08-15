from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime, timezone
# Load environment variables from .env file


# Connect to MongoDB (update with your connection string if needed)
client = MongoClient("mongodb+srv://komper:Nama200010@chillcluster.veaiqbd.mongodb.net")
db = client['sample_mflix']  # Database name
collection = db['comments']  # Collection name

# # Insert One Document
# def insert_one_document():
#     # Create a document with an auto-generated ObjectId
#     document = {
#         '_id': ObjectId(),  # Explicitly generate ObjectId
#         'name': 'Alice',
#         'age': 25,
#         'city': 'New York'
#     }
    
#     # Insert the document
#     result = collection.insert_one(document)
#     print(f"Inserted document with ID: {result.inserted_id}")

# Insert Many Documents
def insert_many_documents():
    # Create a list of documents, each with an auto-generated ObjectId
    documents = [
        {
            '_id': ObjectId(),
            'name': 'jim chaow',
            'email': 'jimc@gmail.com',
            'movie_id': ObjectId('573a139ff29313caabcff466'),
            'text': 'Very Good movie!',
            'date': datetime.now(timezone.utc)
        },
        {
            '_id': ObjectId(),
            'name': 'jim chaow',
            'email': 'jimc@gmail.com',
            'movie_id': ObjectId('573a1395f29313caabce1126'),
            'text': 'This is a Not Good movie',
            'date': datetime.now(timezone.utc)
        }
    ]
    
    # Insert multiple documents
    result = collection.insert_many(documents)
    print(f"Inserted {len(result.inserted_ids)} documents with IDs: {result.inserted_ids}")

# Run the functions
if __name__ == "__main__":
    
    
    # Insert multiple documents
    insert_many_documents()