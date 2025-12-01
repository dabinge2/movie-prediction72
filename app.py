import os
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from flask_socketio import SocketIO
from pyspark.sql import SparkSession
from config import Config
from spark_jobs.task1_processing import process_task1
from spark_jobs.task2_processing import process_task2
from spark_jobs.task3_processing import StreamingProcessor
import threading
import time

# MAIN BRANCH: Base version
app = Flask(__name__)
app.config.from_object(Config)
socketio = SocketIO(app)

# Initialize Spark
spark = SparkSession.builder \
    .appName("MovieAnalysis") \
    .master(app.config['SPARK_MASTER']) \
    .config("spark.jars", "mysql-connector-java-8.0.23.jar") \
    .getOrCreate()

# Global streaming processor
streaming_processor = None


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/task1', methods=['GET', 'POST'])
def task1():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file uploaded')
            return redirect(request.url)

        file = request.files['file']
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)

        if file and file.filename.endswith('.csv'):
            filename = 'movies.csv'
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)

            try:
                result = process_task1(spark, filepath)
                return render_template('task1.html', movies=result)
            except Exception as e:
                flash(f'Error processing file: {str(e)}')
                return redirect(request.url)

    return render_template('task1.html')


@app.route('/task2')
def task2():
    try:
        result = process_task2(spark)
        return render_template('task2.html', data=result)
    except Exception as e:
        flash(f'Error processing task 2: {str(e)}')
        return redirect(url_for('index'))


@app.route('/task3')
def task3():
    global streaming_processor

    if streaming_processor is None or not streaming_processor.is_active():
        streaming_processor = StreamingProcessor(spark, socketio)
        streaming_thread = threading.Thread(target=streaming_processor.start)
        streaming_thread.daemon = True
        streaming_thread.start()

    return render_template('task3.html')


@app.route('/api/stream_rating', methods=['POST'])
def stream_rating():
    data = request.json
    if not data or 'userId' not in data or 'movieId' not in data or 'rating' not in data:
        return jsonify({"status": "error", "message": "Invalid data format"})

    try:
        filepath = os.path.join(app.config['STREAMING_FOLDER'], 'ratings_stream.json')
        with open(filepath, 'a') as f:
            f.write(f"{data}\n")
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


@socketio.on('connect')
def handle_connect():
    print('Client connected')


@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')


if __name__ == '__main__':
    socketio.run(app, debug=True)