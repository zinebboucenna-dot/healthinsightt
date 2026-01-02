from pymongo import MongoClient
from datetime import datetime
import sys

class MongoDBOperations:
    def __init__(self, mongo_uri="mongodb://localhost:27018/?directConnection=true"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client.healthinsight
        self.patients = self.db.patients
    
    def update_patient_profile(self, patient_id, updates):
        """Q3: Update patient profile (contact info)"""
        result = self.patients.update_one(
            {"patient_id": patient_id},
            {
                "$set": {
                    **updates,
                    "last_updated": datetime.utcnow()
                }
            }
        )
        return result.matched_count > 0
    
    def add_medical_note(self, patient_id, doctor, note):
        """Q3: Add medical note"""
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
        return result.matched_count > 0
    
    def get_patient(self, patient_id):
        """Retrieve patient"""
        return self.patients.find_one({"patient_id": patient_id})


if __name__ == "__main__":

    print("\n" + "=" * 60)
    print("Q3: Update Patient Profile")
    print("=" * 60 + "\n")

    # 🔹 Require patient ID from command line
    if len(sys.argv) > 1:
        patient_id = sys.argv[1]
    else:
        print("❌ Please provide patient ID")
        print("Usage: python crud_operations/mongodb_operations.py P000001")
        sys.exit(1)

    mongo = MongoDBOperations()

    patient = mongo.get_patient(patient_id)
    if not patient:
        print(f"❌ Patient {patient_id} not found.")
        sys.exit(1)

    print(f"Testing with Patient ID: {patient_id}")
    print(
        f"Current phone: "
        f"{patient.get('personal_info', {}).get('contact', {}).get('phone', 'N/A')}\n"
    )

    # ===============================
    # Test 1: Update phone number
    # ===============================
    print("Test 1: Updating phone number...")
    success = mongo.update_patient_profile(
        patient_id,
        {"personal_info.contact.phone": "+213555999555"}
    )

    if success:
        updated = mongo.get_patient(patient_id)
        new_phone = updated["personal_info"]["contact"]["phone"]
        print(f"✅ Phone updated to: {new_phone}\n")
    else:
        print("❌ Phone update failed\n")

    # ===============================
    # Test 2: Add medical note
    # ===============================
    print("Test 2: Adding medical note...")
    success = mongo.add_medical_note(
        patient_id,
        "Dr. Salim Kaddour",
        "Patient profile updated. Contact information verified."
    )

    if success:
        updated = mongo.get_patient(patient_id)
        notes_count = len(updated.get("medical_notes", []))
        print(f"✅ Medical note added. Total notes: {notes_count}\n")
    else:
        print("❌ Failed to add medical note\n")

    print("=" * 60)
    print("✅ Q3 COMPLETE: Patient profile updated successfully")
    print("=" * 60 + "\n")
