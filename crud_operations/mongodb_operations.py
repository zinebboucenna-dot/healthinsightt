from pymongo import MongoClient, WriteConcern
from datetime import datetime
import uuid

class MongoDBOperations:
    def __init__(self, uri="mongodb://localhost:27017/"):
        self.client = MongoClient(uri)
        self.db = self.client.healthinsight
        self.patients = self.db.patients.with_options(
            write_concern=WriteConcern(w="majority", j=True)
        )
    
    def update_patient_profile(self, patient_id, updates):
        """Q3: Update patient profile (contact info, medical notes)"""
        result = self.patients.update_one(
            {"patient_id": patient_id},
            {
                "$set": {
                    **updates,
                    "last_updated": datetime.utcnow()
                }
            }
        )
        return result.modified_count > 0
    
    def add_medical_note(self, patient_id, doctor, note):
        """Q3: Add medical note to patient record"""
        result = self.patients.update_one(
            {"patient_id": patient_id},
            {
                "$push": {
                    "medical_notes": {
                        "date": datetime.utcnow(),
                        "doctor": doctor,
                        "note": note
                    }
                },
                "$set": {"last_updated": datetime.utcnow()}
            }
        )
        return result.modified_count > 0
    
    def get_patient_by_id(self, patient_id):
        """Retrieve patient profile"""
        return self.patients.find_one({"patient_id": patient_id})
    
    def search_patients_by_name(self, name):
        """Text search for patients"""
        return list(self.patients.find(
            {"$text": {"$search": name}},
            {"score": {"$meta": "textScore"}}
        ).sort([("score", {"$meta": "textScore"})]))
    
    def get_risk_score(self, patient_id):
        """Q6: Get current risk score (fast indexed query)"""
        result = self.patients.find_one(
            {"patient_id": patient_id},
            {"patient_id": 1, "risk_score": 1, "last_updated": 1}
        ).hint({"patient_id": 1})  # Force index usage for speed
        
        if result:
            return {
                "patient_id": result["patient_id"],
                "risk_score": result["risk_score"],
                "last_updated": result["last_updated"]
            }
        return None
    
    def update_risk_score(self, patient_id, new_score, factors=None):
        """Q6: Update current risk score with history tracking"""
        update_doc = {
            "$set": {
                "risk_score": new_score,
                "last_updated": datetime.utcnow()
            }
        }
        
        # Optionally track risk score history
        if factors:
            update_doc["$push"] = {
                "risk_score_history": {
                    "score": new_score,
                    "calculated_at": datetime.utcnow(),
                    "factors": factors
                }
            }
        
        result = self.patients.update_one(
            {"patient_id": patient_id},
            update_doc
        )
        return result.modified_count > 0
    
    def get_high_risk_patients(self, threshold=70, limit=10):
        """Q6: Query patients by risk score"""
        return list(self.patients.find(
            {"risk_score": {"$gte": threshold}}
        ).sort("risk_score", -1).limit(limit))

# Example usage
if __name__ == "__main__":
    mongo = MongoDBOperations()
    
    # Q3: Update patient contact info
    success = mongo.update_patient_profile(
        "P000001",
        {"personal_info.contact.phone": "+213555999888"}
    )
    print(f"Profile updated: {success}")
    
    # Q3: Add medical note
    mongo.add_medical_note(
        "P000001",
        "Dr. Salim Kaddour",
        "Patient responding well to treatment. Continue current medication."
    )
    
    # Q6: Get risk score (fast read)
    risk_data = mongo.get_risk_score("P000001")
    print(f"Risk score: {risk_data}")
    
    # Q6: Update risk score
    mongo.update_risk_score(
        "P000001",
        new_score=70,
        factors=["recent_abnormal_readings", "hypertension"]
    )
    print("Risk score updated successfully")
    
    # Q6: Get high-risk patients
    high_risk = mongo.get_high_risk_patients(threshold=70)
    print(f"Found {len(high_risk)} high-risk patients")