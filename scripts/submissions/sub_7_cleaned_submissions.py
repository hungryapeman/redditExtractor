from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['reddit_data']
source_collection = db['[6]top_500_submissions_per_domain']
target_collection = db['[7]cleaned_submissions']

# List of fields to remove
fields_to_remove = [
    "link_flair_css_class", "edited", "is_self", "media_embed", "domain", "thumbnail",
    "hidden", "saved", "over_18", "gilded", "retrieved_on", "stickied",
    "secure_media_embed", "archived", "quarantine", "hide_score", "locked"
]

# Fetch all documents from the source collection
documents = source_collection.find()

# Remove specified fields and store in a new collection
cleaned_documents = []
for doc in documents:
    for field in fields_to_remove:
        doc.pop(field, None)
    cleaned_documents.append(doc)

# Insert cleaned documents into the target collection
if cleaned_documents:
    target_collection.insert_many(cleaned_documents)

print("Fields removed and cleaned documents stored in a new collection successfully.")
