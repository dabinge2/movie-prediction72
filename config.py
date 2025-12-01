import os
from pathlib import Path


class Config:
    # Flask config
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-key-123'

    # Database config
    MYSQL_HOST = 'localhost'
    MYSQL_USER = 'movielens'
    MYSQL_PASSWORD = 'movielens123'
    MYSQL_DB = 'movielens'

    # Hadoop config
    HDFS_URI = 'hdfs://localhost:9000'
    HDFS_DATA_DIR = '/movielens'

    # Spark config
    SPARK_MASTER = 'local[*]'

    # Paths
    BASE_DIR = Path(__file__).parent
    UPLOAD_FOLDER = BASE_DIR / 'uploads'
    DATA_FOLDER = BASE_DIR / 'data'
    STREAMING_FOLDER = BASE_DIR / 'streaming_data'

    # Ensure directories exist
    UPLOAD_FOLDER.mkdir(exist_ok=True)
    DATA_FOLDER.mkdir(exist_ok=True)
    STREAMING_FOLDER.mkdir(exist_ok=True)