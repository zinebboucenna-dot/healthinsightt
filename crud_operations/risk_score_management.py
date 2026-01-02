from pymongo import MongoClient
from datetime import datetime
import sys
import random

class RiskScoreManager:
    def __init__(self, mongo_uri="mongodb://localhost:27018/?directConnection=true"):
        try:
            self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            self.db = self.client.healthinsight
            self.patients = self.db.patients
            # Test connection
            self.client.admin.command('ping')
            print("✓ Connected to MongoDB\n")
        except Exception as e:
            print(f"❌ Could not connect to MongoDB: {e}")
            sys.exit(1)
    
    def update_risk_score(self, patient_id, new_score, factors=None):
        """Q6: Update current risk score with history tracking"""
        update_doc = {
            "$set": {
                "risk_score": new_score,
                "last_updated": datetime.utcnow()
            }
        }
        
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
    
    def get_risk_score(self, patient_id):
        """Q6: Get current risk score (fast indexed query)"""
        result = self.patients.find_one(
            {"patient_id": patient_id},
            {"patient_id": 1, "risk_score": 1, "last_updated": 1, "_id": 0}
        )
        return result
    
    def get_high_risk_patients(self, threshold=70):
        """Q6: Query patients by risk score"""
        return list(self.patients.find(
            {"risk_score": {"$gte": threshold}},
            {"patient_id": 1, "personal_info.name": 1, "risk_score": 1, "_id": 0}
        ).sort("risk_score", -1).limit(10))
    
    def bulk_update_risk_scores(self):
        """Simulate ML model updating risk scores for all patients"""
        patients = list(self.patients.find({}, {"patient_id": 1, "_id": 0}))
        
        print(f"Updating risk scores for {len(patients)} patients...")
        
        updated = 0
        for patient in patients:
            # Simulate ML model calculation
            new_score = random.randint(20, 95)
            factors = random.sample([
                "recent_abnormal_readings",
                "hypertension",
                "age_factor",
                "multiple_conditions",
                "medication_compliance"
            ], k=random.randint(1, 3))
            
            if self.update_risk_score(patient['patient_id'], new_score, factors):
                updated += 1
        
        return updated

if __name__ == "__main__":
    print("\n" + "="*80)
    print("Q6: Risk Score Maintenance")
    print("="*80 + "\n")
    
    manager = RiskScoreManager()
    
    # Test 1: Get a sample patient
    sample_patient = manager.patients.find_one()
    if not sample_patient:
        print("❌ No patients found in database")
        sys.exit(1)
    
    patient_id = sample_patient['patient_id']
    patient_name = sample_patient['personal_info']['name']
    
    print(f"📋 Testing with Patient: {patient_name}")
    print(f"   ID: {patient_id}\n")
    print("-" * 80)
    
    # Test 2: Get current risk score
    print("\n🔍 TEST 1: Retrieve Current Risk Score (Fast Indexed Query)")
    print("-" * 80)
    
    risk_data = manager.get_risk_score(patient_id)
    if risk_data:
        print(f"✅ Retrieved risk score:")
        print(f"   Patient ID: {risk_data['patient_id']}")
        print(f"   Current Risk Score: {risk_data['risk_score']}/100")
        print(f"   Last Updated: {risk_data.get('last_updated', 'N/A')}")
        current_score = risk_data['risk_score']
    else:
        print("❌ Failed to retrieve risk score")
        sys.exit(1)
    
    # Test 3: Update risk score
    print("\n📊 TEST 2: Update Risk Score with ML Factors")
    print("-" * 80)
    
    new_score = min(current_score + 10, 95)  # Increase score slightly
    risk_factors = ["recent_abnormal_readings", "hypertension", "age_factor"]
    
    print(f"Updating risk score from {current_score} to {new_score}")
    print(f"Risk factors: {', '.join(risk_factors)}")
    
    success = manager.update_risk_score(patient_id, new_score, factors=risk_factors)
    
    if success:
        updated_risk = manager.get_risk_score(patient_id)
        print(f"✅ Risk score updated successfully:")
        print(f"   New Score: {updated_risk['risk_score']}/100")
        
        # Check history was tracked
        patient = manager.patients.find_one({"patient_id": patient_id})
        history_count = len(patient.get('risk_score_history', []))
        print(f"   History Entries: {history_count}")
    else:
        print("❌ Failed to update risk score")
    
    # Test 4: Bulk update (simulating ML model)
    print("\n🤖 TEST 3: Bulk Risk Score Update (Simulating ML Model)")
    print("-" * 80)
    print("Running ML model to recalculate risk scores for all patients...")
    
    updated_count = manager.bulk_update_risk_scores()
    print(f"✅ Updated risk scores for {updated_count} patients")
    
    # Test 5: Query high-risk patients
    print("\n🚨 TEST 4: Query High-Risk Patients (Risk Score ≥ 70)")
    print("-" * 80)
    
    high_risk = manager.get_high_risk_patients(threshold=70)
    
    print(f"Found {len(high_risk)} high-risk patients:\n")
    
    if high_risk:
        print(f"{'Rank':<6} {'Patient ID':<15} {'Name':<25} {'Risk Score':<12}")
        print("-" * 60)
        for i, patient in enumerate(high_risk, 1):
            name = patient.get('personal_info', {}).get('name', 'N/A')
            print(f"{i:<6} {patient['patient_id']:<15} {name:<25} {patient['risk_score']:<12}")
    else:
        print("No high-risk patients found")
    
    # Final Summary
    print("\n" + "="*80)
    print("📊 SUMMARY")
    print("="*80)
    
    all_patients = list(manager.patients.find({}, {"risk_score": 1, "_id": 0}))
    avg_risk = sum(p['risk_score'] for p in all_patients) / len(all_patients)
    high_risk_count = sum(1 for p in all_patients if p['risk_score'] >= 70)
    
    print(f"Total patients: {len(all_patients)}")
    print(f"Average risk score: {avg_risk:.2f}")
    print(f"High-risk patients (≥70): {high_risk_count}")
    print(f"High-risk percentage: {(high_risk_count/len(all_patients)*100):.2f}%")
    
    print("\n" + "="*80)
    print("✅ Q6 COMPLETE: Risk score maintenance demonstrated successfully")
    print("="*80 + "\n")