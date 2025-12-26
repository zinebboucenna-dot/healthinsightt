import json
import uuid
from datetime import datetime
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

class KafkaToCassandraConsumer:
    def __init__(self, kafka_broker="localhost:9092", cassandra_host="127.0.0.1"):
        # Connect to Kafka
        self.consumer = KafkaConsumer(
            'patient.monitoring.raw',
            bootstrap_servers=kafka_broker,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='cassandra-writer'
        )
        
        # Connect to Cassandra
        self.cluster = Cluster([cassandra_host])
        self.session = self.cluster.connect('healthinsight')
        
        # Prepare insert statement
        self.insert_stmt = self.session.prepare("""
            INSERT INTO patient_monitoring 
            (patient_id, reading_timestamp, metric_type, value, unit, 
             device_id, is_abnormal, threshold_min, threshold_max)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        print("✓ Connected to Kafka and Cassandra")
    
    def process_messages(self, max_messages=None):
        """Consume messages from Kafka and write to Cassandra"""
        count = 0
        
        try:
            print(f"Starting to consume messages from Kafka...")
            print("Press Ctrl+C to stop\n")
            
            for message in self.consumer:
                reading = message.value
                
                # Parse timestamp
                timestamp = datetime.fromisoformat(reading['reading_timestamp'].replace('Z', '+00:00'))
                
                # Insert into Cassandra
                self.session.execute(self.insert_stmt, (
                    uuid.UUID(reading['patient_id']) if '-' in reading['patient_id'] else uuid.uuid5(uuid.NAMESPACE_DNS, reading['patient_id']),
                    timestamp,
                    reading['metric_type'],
                    reading['value'],
                    reading['unit'],
                    reading['device_id'],
                    reading['is_abnormal'],
                    reading['threshold_min'],
                    reading['threshold_max']
                ))
                
                count += 1
                
                if count % 100 == 0:
                    print(f"✓ Processed {count} readings", end='\r')
                
                if max_messages and count >= max_messages:
                    break
                    
        except KeyboardInterrupt:
            print(f"\n\n✓ Stopped by user")
        finally:
            print(f"\n✓ Total readings written to Cassandra: {count}")
            self.consumer.close()
            self.cluster.shutdown()

if __name__ == "__main__":
    print("=" * 60)
    print("Kafka to Cassandra Consumer")
    print("=" * 60)
    
    consumer = KafkaToCassandraConsumer()
    
    # Process all messages in Kafka (about 14,880)
    consumer.process_messages()