import sys
import uuid
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

# Initialize asyncio for Python 3.12+
try:
    import cassandra
    cassandra.io.asyncioreactor.AsyncioConnection.initialize_reactor()
except:
    pass

def get_last_n_readings(patient_id, metric_type=None, limit=20):
    """Q2: Retrieve last N readings for a patient"""
    
    # Connect to Cassandra
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('healthinsight')
    session.row_factory = dict_factory
    
    # Convert patient_id to UUID
    if '-' not in patient_id:
        patient_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, patient_id)
    else:
        patient_uuid = uuid.UUID(patient_id)
    
    # Query
    if metric_type:
        query = """
        SELECT reading_timestamp, metric_type, value, unit, is_abnormal
        FROM patient_monitoring
        WHERE patient_id = %s AND metric_type = %s
        ORDER BY reading_timestamp DESC
        LIMIT %s
        """
        rows = session.execute(query, (patient_uuid, metric_type, limit))
    else:
        query = """
        SELECT reading_timestamp, metric_type, value, unit, is_abnormal
        FROM patient_monitoring
        WHERE patient_id = %s
        LIMIT %s
        ALLOW FILTERING
        """
        rows = session.execute(query, (patient_uuid, limit))
    
    results = list(rows)
    cluster.shutdown()
    
    return results

if __name__ == "__main__":
    patient_id = sys.argv[1] if len(sys.argv) > 1 else "P123456"
    
    print(f"\n{'='*60}")
    print(f"Q2: Last 20 Readings for Patient {patient_id}")
    print(f"{'='*60}\n")
    
    try:
        readings = get_last_n_readings(patient_id, limit=20)
        
        if readings:
            print(f"Found {len(readings)} readings:\n")
            for i, r in enumerate(readings[:20], 1):
                status = "⚠️ ABNORMAL" if r['is_abnormal'] else "✓ Normal"
                print(f"{i}. {r['reading_timestamp']} | {r['metric_type']}: {r['value']} {r['unit']} {status}")
            
            print(f"\n{'='*60}")
            print(f"✅ Q2 COMPLETE: Retrieved {len(readings)} readings")
            print(f"{'='*60}\n")
        else:
            print(f"❌ No readings found for patient {patient_id}")
            print("   Run Q1 first to insert data.")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nTry installing cassandra-driver with asyncio support:")
        print("pip install cassandra-driver geomet")