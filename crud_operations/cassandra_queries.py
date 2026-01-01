from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, dict_factory
from datetime import datetime, timedelta, timezone
import uuid

class CassandraOperations:
    def __init__(self, contact_points=['127.0.0.1'], keyspace='healthinsight'):
        self.cluster = Cluster(contact_points)
        self.session = self.cluster.connect(keyspace)
        self.session.row_factory = dict_factory
    
    def insert_reading(self, patient_id, metric_type, value, unit, device_id, 
                       is_abnormal, threshold_min, threshold_max):
        """Q1: Insert patient monitoring event (<100ms latency)"""
        query = """
        INSERT INTO patient_monitoring 
        (patient_id, reading_timestamp, metric_type, value, unit, device_id, 
         is_abnormal, threshold_min, threshold_max)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        USING TTL 7776000
        """
        
        self.session.execute(
            query,
            (uuid.UUID(patient_id), datetime.now(timezone.utc), metric_type, value, unit,
             device_id, is_abnormal, threshold_min, threshold_max),
            timeout=0.1  # 100ms timeout
        )
    
    def get_last_n_readings(self, patient_id, metric_type=None, limit=20):
        """Q2: Retrieve last N readings for a patient"""
        if metric_type:
            query = """
            SELECT * FROM patient_monitoring
            WHERE patient_id = %s AND metric_type = %s
            ORDER BY reading_timestamp DESC
            LIMIT %s
            """
            rows = self.session.execute(query, (uuid.UUID(patient_id), metric_type, limit))
        else:
            query = """
            SELECT * FROM patient_monitoring
            WHERE patient_id = %s
            ORDER BY reading_timestamp DESC
            LIMIT %s
            """
            rows = self.session.execute(query, (uuid.UUID(patient_id), limit))
        
        return list(rows)
    
    def get_abnormal_readings(self, hours=24):
        """Q4: Detect abnormal readings across patients"""
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        query = """
        SELECT patient_id, metric_type, value, reading_timestamp
        FROM patient_monitoring
        WHERE is_abnormal = true AND reading_timestamp > %s
        ALLOW FILTERING
        """
        
        rows = self.session.execute(query, (cutoff_time,))
        return list(rows)
    
    def close(self):
        self.cluster.shutdown()

# Example usage
if __name__ == "__main__":
    cassandra = CassandraOperations()
    
    # Q2: Get last 20 readings
    readings = cassandra.get_last_n_readings("P000001", "heart_rate", limit=20)
    print(f"Last 20 heart rate readings: {len(readings)} found")
    
    cassandra.close()