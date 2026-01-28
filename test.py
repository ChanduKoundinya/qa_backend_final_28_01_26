from pymongo import MongoClient
import gridfs
from bson import ObjectId

# 1. Connect to MongoDB (Adjust URL if needed)
client = MongoClient("mongodb://localhost:27017/")
db = client["your_database_name"]  # ⚠️ REPLACE with your actual DB name
fs = gridfs.GridFS(db)

# 2. The ID from your screenshot
file_id = ObjectId("695f698d9edcfc88eda2f48b")

try:
    # 3. Get the file
    grid_file = fs.get(file_id)
    print(f"✅ Found file: {grid_file.filename}")
    
    # 4. Save to disk
    with open(grid_file.filename, "wb") as output_file:
        output_file.write(grid_file.read())
        
    print(f"🎉 Successfully downloaded to: {grid_file.filename}")

except Exception as e:
    print(f"❌ Error: {e}")