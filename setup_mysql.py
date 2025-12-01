import mysql.connector
from config import Config


def setup_database():
    try:
        # Connect to MySQL server
        conn = mysql.connector.connect(
            host=Config.MYSQL_HOST,
            user='root',
            password='your_root_password'
        )
        cursor = conn.cursor()

        # Create database
        cursor.execute("CREATE DATABASE IF NOT EXISTS movielens")
        cursor.execute("USE movielens")

        # Create user
        cursor.execute(
            f"CREATE USER IF NOT EXISTS '{Config.MYSQL_USER}'@'localhost' IDENTIFIED BY '{Config.MYSQL_PASSWORD}'")
        cursor.execute(f"GRANT ALL PRIVILEGES ON movielens.* TO '{Config.MYSQL_USER}'@'localhost'")

        # Create tables
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            movieId INT PRIMARY KEY,
            title VARCHAR(255),
            genres VARCHAR(255)
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ratings (
            userId INT,
            movieId INT,
            rating FLOAT,
            timestamp BIGINT,
            FOREIGN KEY (movieId) REFERENCES movies(movieId)
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tags (
            userId INT,
            movieId INT,
            tag VARCHAR(255),
            timestamp BIGINT,
            FOREIGN KEY (movieId) REFERENCES movies(movieId)
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            userId INT PRIMARY KEY,
            gender CHAR(1),
            age INT,
            occupation INT,
            zipcode VARCHAR(10)
        )
        """)

        conn.commit()
        print("Database setup completed successfully.")

    except Exception as e:
        print(f"Error setting up database: {str(e)}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


if __name__ == '__main__':
    setup_database()