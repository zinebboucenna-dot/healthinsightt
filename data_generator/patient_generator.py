from pymongo import MongoClient
from faker import Faker
from datetime import datetime, timezone, time
import random
import sys

class PatientGenerator:
    def __init__(self, mongo_uri="mongodb://localhost:27018/?directConnection=true", db_name="healthinsight"):
        try:
            self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            self.db = self.client[db_name]
            self.patients = self.db["patients"]

            # Test connection
            self.client.admin.command("ping")

        except Exception as e:
            print(f"❌ Could not connect to MongoDB: {e}")
            sys.exit(1)

        self.fake = Faker("fr_FR")
        self.start_index = self.get_next_patient_index()

    def get_next_patient_index(self):
        last_patient = self.patients.find_one(sort=[("patient_id", -1)])
        if last_patient:
            return int(last_patient["patient_id"][1:]) + 1  # remove 'P' and increment
        else:
            return 100000  # start from P100000 if collection is empty

    def generate_patient(self, index: int) -> dict:
        # Generate date of birth as datetime (MongoDB compatible)
        dob_date = self.fake.date_of_birth(minimum_age=1, maximum_age=95)
        dob_datetime = datetime.combine(dob_date, time.min)

        patient = {
            "patient_id": f"P{index}",
            "personal_info": {
                "name": self.fake.name(),
                "date_of_birth": dob_datetime,
                "gender": random.choice(["M", "F"]),
                "contact": {
                    "phone": self.fake.phone_number(),
                    "email": self.fake.email(),
                    "address": {
                        "street": self.fake.street_address(),
                        "city": self.fake.city(),
                        "postal_code": self.fake.postcode()
                    }
                }
            },
            "medical_info": {
                "blood_type": random.choice(["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]),
                "allergies": random.sample(
                    ["penicillin", "pollen", "nuts", "latex", "none"],
                    k=random.randint(0, 2)
                ),
                "chronic_conditions": random.sample(
                    ["diabetes", "hypertension", "asthma", "none"],
                    k=random.randint(0, 2)
                ),
                "current_medications": []
            },
            "risk_score": random.randint(0, 100),
            "risk_score_history": [],
            "medical_notes": [],
            "last_updated": datetime.now(timezone.utc)
        }

        return patient

    def generate_batch(self, count: int):
        patients = [self.generate_patient(self.start_index + i) for i in range(count)]
        result = self.patients.insert_many(patients)
        return result.inserted_ids

if __name__ == "__main__":
    print("Generating patients...")

    generator = PatientGenerator(
        mongo_uri="mongodb://localhost:27018/?directConnection=true",
        db_name="healthinsight"
    )

    inserted_ids = generator.generate_batch(100)
    print(f"✅ Inserted {len(inserted_ids)} patients")
