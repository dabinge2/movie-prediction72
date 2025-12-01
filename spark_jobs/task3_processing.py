from pyspark.streaming import StreamingContext
import json
from datetime import datetime


class StreamingProcessor:
    def __init__(self, spark, socketio):
        self.spark = spark
        self.socketio = socketio
        self.ssc = StreamingContext(spark.sparkContext, 10)  # 10-second batches
        self.active = False

    def is_active(self):
        return self.active

    def start(self):
        self.active = True

        # Create a stream from the streaming directory
        lines = self.ssc.textFileStream(str('streaming_data/'))

        # Process each RDD in the stream
        def process_rdd(time, rdd):
            if not rdd.isEmpty():
                try:
                    # Parse JSON data
                    ratings = rdd.map(lambda x: json.loads(x))

                    # Calculate statistics
                    rating_values = ratings.map(lambda x: float(x['rating']))
                    stats = rating_values.stats()

                    # Get timestamp
                    timestamp = datetime.fromtimestamp(time.milliseconds / 1000).strftime('%H:%M:%S')

                    # Prepare data for frontend
                    result = {
                        "time": timestamp,
                        "count": stats.count(),
                        "avg": stats.mean(),
                        "min": stats.min(),
                        "max": stats.max()
                    }

                    # Send to all clients via SocketIO
                    self.socketio.emit('stream_update', result)

                except Exception as e:
                    print(f"Error processing RDD: {str(e)}")

        lines.foreachRDD(process_rdd)

        # Start the streaming context
        self.ssc.start()
        self.ssc.awaitTermination()

    def stop(self):
        self.active = False
        self.ssc.stop(stopSparkContext=False, stopGraceFully=True)