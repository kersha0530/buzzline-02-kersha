from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Print environment variables to verify
print("KAFKA_TOPIC:", os.getenv("KAFKA_TOPIC"))
print("KAFKA_CONSUMER_GROUP_ID:", os.getenv("KAFKA_CONSUMER_GROUP_ID"))
