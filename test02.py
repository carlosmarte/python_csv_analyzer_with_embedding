import chromadb
from chromadb.config import Settings

# We'll use scikit-learn for TF-IDF
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np


data = [
    {"name": "Alice2",   "age": 25, "city": "New York", "education": "YES"},
    {"name": "Bob",     "age": 30, "city": "Los Angeles", "education": "NO"},
    {"name": "Charlie", "age": 35, "city": "Chicago", "education": "NO"},
]

# Convert each row to a text snippet
def row_to_text(row):
    return f"{row['name']} is {row['age']} years old and lives in {row['city']}."

texts = [row_to_text(r) for r in data]

# 2) Create TF-IDF vectors
vectorizer = TfidfVectorizer()
tfidf_matrix = vectorizer.fit_transform(texts)  
# tfidf_matrix is a sparse matrix of shape (3 documents, N features)

# Convert sparse to dense or array form, if needed
tfidf_embeddings = tfidf_matrix.toarray()  # shape = (3, N_features)

# 3) Setup Chroma
client = chromadb.PersistentClient(
    path="./chroma_db"  # Directory where you want to store the database
)

collection = client.get_or_create_collection(name="people_collection_2")

# 4) Add documents & embeddings to Chroma
for i, (row, emb) in enumerate(zip(data, tfidf_embeddings)):
    doc_id = f"person_{i}"
    collection.add(
        documents=[texts[i]],
        embeddings=[emb.tolist()],  # must be a list of floats
        ids=[doc_id],
        metadatas=[row]
    )

# 5) Query
#    *We need to convert the query text into a TF-IDF vector using the same vectorizer*
query_text = "xxx"
query_vector = vectorizer.transform([query_text]).toarray()[0].tolist()

results = collection.query(
    query_embeddings=[query_vector],
    n_results=3
)

print("Results:", results)