#!/usr/bin/env python3
"""
Generate 100 users with realistic data for the fraud detection application.
"""

import json
import random
from datetime import datetime, timedelta

# Sample data for generating realistic users
FIRST_NAMES = [
    "Alice", "Bob", "Carol", "David", "Eva", "Frank", "Grace", "Henry", "Iris", "Jack",
    "Kate", "Liam", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Rachel", "Sam", "Tina",
    "Uma", "Victor", "Wendy", "Xavier", "Yara", "Zoe", "Adam", "Beth", "Chris", "Diana",
    "Eric", "Fiona", "George", "Hannah", "Ian", "Julia", "Kevin", "Lisa", "Mark", "Nina",
    "Oscar", "Paula", "Ryan", "Sarah", "Tom", "Ursula", "Vincent", "Wendy", "Xander", "Yvonne"
]

LAST_NAMES = [
    "Johnson", "Smith", "Davis", "Wilson", "Martinez", "Brown", "Lee", "Garcia", "Rodriguez", "Taylor",
    "Anderson", "Thompson", "White", "Clark", "Lewis", "Hall", "Young", "King", "Wright", "Lopez",
    "Hill", "Scott", "Green", "Adams", "Baker", "Gonzalez", "Nelson", "Carter", "Mitchell", "Perez",
    "Roberts", "Turner", "Phillips", "Campbell", "Parker", "Evans", "Edwards", "Collins", "Stewart", "Sanchez",
    "Morris", "Rogers", "Reed", "Cook", "Morgan", "Bell", "Murphy", "Bailey", "Rivera", "Cooper"
]

LOCATIONS = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", 
    "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "Fort Worth", "Columbus", 
    "Charlotte", "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington", "Boston",
    "El Paso", "Nashville", "Detroit", "Oklahoma City", "Portland", "Las Vegas", "Memphis",
    "Louisville", "Baltimore", "Milwaukee", "Albuquerque", "Tucson", "Fresno", "Sacramento",
    "Atlanta", "Kansas City", "Long Beach", "Colorado Springs", "Raleigh", "Miami", "Virginia Beach"
]

OCCUPATIONS = [
    "Software Engineer", "Marketing Manager", "Financial Analyst", "Sales Representative", "Teacher",
    "Accountant", "Nurse", "Police Officer", "Graphic Designer", "Project Manager", "Data Scientist",
    "Construction Manager", "HR Specialist", "Web Developer", "Real Estate Agent", "Doctor",
    "Lawyer", "Chef", "Electrician", "Plumber", "Mechanic", "Dentist", "Architect", "Engineer",
    "Consultant", "Administrator", "Coordinator", "Specialist", "Assistant", "Director",
    "Supervisor", "Manager", "Analyst", "Coordinator", "Technician", "Operator", "Clerk",
    "Receptionist", "Secretary", "Cashier", "Driver", "Waiter", "Bartender", "Janitor",
    "Security Guard", "Librarian", "Translator", "Interpreter", "Journalist", "Photographer"
]

ACCOUNT_TYPES = ["checking", "savings", "credit"]

def generate_user(user_id):
    """Generate a single user with realistic data."""
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    name = f"{first_name} {last_name}"
    email = f"{first_name.lower()}.{last_name.lower()}@email.com"
    
    age = random.randint(18, 65)
    location = random.choice(LOCATIONS)
    occupation = random.choice(OCCUPATIONS)
    
    # Generate realistic risk score (0-100)
    risk_score = round(random.uniform(0, 100), 1)
    
    # Generate signup date within the last 2 years
    days_ago = random.randint(0, 730)
    signup_date = datetime.now() - timedelta(days=days_ago)
    
    phone = f"+1-555-{random.randint(1000, 9999):04d}"
    
    # Generate 1-3 accounts per user
    num_accounts = random.randint(1, 3)
    accounts = []
    
    for i in range(num_accounts):
        account_type = random.choice(ACCOUNT_TYPES)
        if account_type == "credit":
            balance = round(random.uniform(-5000, 0), 2)  # Negative for credit
        else:
            balance = round(random.uniform(100, 50000), 2)
        
        account = {
            "id": f"A{user_id:03d}_{i+1}",
            "type": account_type,
            "balance": balance,
            "created_date": signup_date.isoformat() + "Z"
        }
        accounts.append(account)
    
    user = {
        "id": f"U{user_id:03d}",
        "name": name,
        "email": email,
        "age": age,
        "location": location,
        "occupation": occupation,
        "risk_score": risk_score,
        "signup_date": signup_date.isoformat() + "Z",
        "phone": phone,
        "accounts": accounts
    }
    
    return user

def main():
    """Generate 100 users and save to JSON file."""
    print("Generating 100 users with realistic data...")
    
    users = []
    for i in range(1, 101):
        user = generate_user(i)
        users.append(user)
        if i % 10 == 0:
            print(f"Generated {i} users...")
    
    data = {"users": users}
    
    # Save to JSON file
    with open("data/users.json", "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"✅ Generated {len(users)} users and saved to data/users.json")
    print(f"📊 Total accounts: {sum(len(user['accounts']) for user in users)}")

if __name__ == "__main__":
    main() 