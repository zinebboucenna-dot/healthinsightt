// Get reference to the healthinsight database
const db = db.getSiblingDB("healthinsight");

// Create patients collection with schema validation
db.createCollection("patients", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["patient_id", "personal_info", "medical_info", "risk_score"],
      properties: {
        patient_id: {
          bsonType: "string",
          description: "Unique patient identifier - required"
        },
        personal_info: {
          bsonType: "object",
          required: ["name", "date_of_birth", "gender"],
          properties: {
            name: { bsonType: "string" },
            date_of_birth: { bsonType: "date" },
            gender: { enum: ["M", "F", "Other"] },
            contact: {
              bsonType: "object",
              properties: {
                phone: { bsonType: "string" },
                email: { bsonType: "string" },
                address: { bsonType: "object" }
              }
            }
          }
        },
        medical_info: {
          bsonType: "object",
          properties: {
            blood_type: {
              enum: ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
            },
            allergies: { bsonType: "array" },
            chronic_conditions: { bsonType: "array" },
            current_medications: { bsonType: "array" }
          }
        },
        risk_score: {
          bsonType: "int",
          minimum: 0,
          maximum: 100,
          description: "Current health risk score (0-100) - Q6"
        },
        risk_score_history: {
          bsonType: "array",
          description: "Historical risk score changes"
        },
        last_updated: { bsonType: "date" }
      }
    }
  }
});

// Create indexes
db.patients.createIndex({ patient_id: 1 }, { unique: true, name: "idx_patient_id" });
db.patients.createIndex({ "personal_info.name": "text" }, { name: "idx_patient_name_text" });
db.patients.createIndex({ risk_score: -1 }, { name: "idx_risk_score" });
db.patients.createIndex({ "medical_info.chronic_conditions": 1 }, { name: "idx_chronic_conditions" });

// Analytics collection
db.createCollection("daily_statistics");
db.daily_statistics.createIndex({ date: -1 }, { name: "idx_date" });

// Insert sample patient
db.patients.insertOne({
  patient_id: "P000001",
  personal_info: {
    name: "Ronaldo Ronaldo",
    date_of_birth: new Date("1985-03-15"),
    gender: "M",
    contact: {
      phone: "+213555123456",
      email: "Ronaldo@example.dz",
      address: {
        street: "Rue des Frères Bouadou",
        city: "Constantine",
        wilaya: "Constantine",
        postal_code: "25000"
      }
    }
  },
  medical_info: {
    blood_type: "O+",
    allergies: ["penicillin"],
    chronic_conditions: ["hypertension"],
    current_medications: [
      { name: "Lisinopril", dosage: "10mg", frequency: "daily", started: new Date("2024-01-15") }
    ]
  },
  risk_score: 65,
  risk_score_history: [
    { score: 60, calculated_at: new Date("2025-12-01"), factors: ["age", "gender"] },
    { score: 65, calculated_at: new Date("2025-12-20"), factors: ["age", "hypertension"] }
  ],
  last_updated: new Date(),
  medical_notes: []
});

print("✓ MongoDB schema created with risk score support (Q6)");
