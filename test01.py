import chromadb
chroma_client = chromadb.Client()

# switch `create_collection` to `get_or_create_collection` to avoid creating a new collection every time
collection = chroma_client.get_or_create_collection(name="my_collection")

# switch `add` to `upsert` to avoid adding the same documents every time
# collection.upsert(
#     documents=[
#         "name=Alice, age=25, city=New York",
#         "name=Bob, age=30, city=Los Angeles",
#         "name=Charlie, age=35, city=Chicago"
#     ],
#     ids=["id1", "id2", "id3"]
# )


# print(collection.query(
#     query_texts=["This is a query document about new york"], # Chroma will embed this for you
#     n_results=2 # how many results to return
# ))

# print(collection.query(
#     query_texts=["This is a query document about florida"], # Chroma will embed this for you
#     n_results=2 # how many results to return
# ))

# print(collection.get(
# 	ids=["id1"],

# ))

# print(collection.get(
#     include=["documents"]
# ))

print(collection.query(
    query_texts=["New York"],
    n_results=10,
    # where_document={"$contains":"New York"}
))