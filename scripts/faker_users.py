import json
import random
import argparse
from faker import Faker
from datetime import datetime, timedelta

# Setup
fake = Faker("en_IN")
Faker.seed(42)
random.seed(42)

# Command-line argument
parser = argparse.ArgumentParser(description="Generate Indian user data with optional fraud flag")
parser.add_argument("-f", "--fraud", action="store_true", help="Mark all accounts as fraudFlag: true")
args = parser.parse_args()

# Config
NUM_USERS = 200
START_DATE = datetime(2023, 1, 1)
occupations = [
    "Software Engineer", "Teacher", "Accountant", "Sales Representative",
    "Marketing Manager", "Nurse", "Police Officer", "Data Scientist",
    "HR Specialist", "Web Developer", "Graphic Designer", "Financial Analyst",
    "Project Manager", "Real Estate Agent", "Construction Manager"
]
cities = [
    "Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Ahmedabad", "Chennai", "Kolkata",
    "Pune", "Jaipur", "Lucknow", "Kanpur", "Nagpur", "Indore", "Thane", "Bhopal"
]

# Device configurations
device_types = ["mobile", "desktop", "tablet"]
operating_systems = {
    "mobile": ["Android 13", "Android 12", "iOS 16", "iOS 15", "Android 11"],
    "desktop": ["Windows 11", "Windows 10", "macOS Ventura", "macOS Monterey", "Ubuntu 22.04"],
    "tablet": ["iPadOS 16", "Android 12", "iPadOS 15", "Android 11"]
}
browsers = {
    "mobile": ["Chrome Mobile", "Safari Mobile", "Firefox Mobile", "Samsung Internet"],
    "desktop": ["Chrome", "Firefox", "Safari", "Edge", "Opera"],
    "tablet": ["Safari", "Chrome", "Firefox"]
}

# Create a pool of devices that will be shared
device_pool = []
for i in range(150):  # Create more devices than users for variety
    device_type = random.choice(device_types)
    device = {
        "id": f"DEV{str(i+1).zfill(4)}",
        "type": device_type,
        "os": random.choice(operating_systems[device_type]),
        "browser": random.choice(browsers[device_type]),
        "fingerprint": fake.sha256(),
        "first_seen": (START_DATE + timedelta(days=random.randint(0, 500))).isoformat() + "Z"
    }
    device_pool.append(device)

users = []
used_devices = set()  # Track which devices have been assigned

# Create some shared device groups for suspicious behavior
shared_device_groups = []
# Group 1: 3-4 users sharing same devices (highly suspicious)
shared_group_1 = random.sample(device_pool, 2)  # 2 shared devices
shared_device_groups.append({"users": [5, 12, 18, 25], "devices": shared_group_1})

# Group 2: 2 users sharing some devices (moderately suspicious)  
shared_group_2 = random.sample(device_pool, 3)  # 3 shared devices
shared_device_groups.append({"users": [45, 67], "devices": shared_group_2})

# Group 3: Family/household sharing (2-3 users, 1-2 shared devices)
shared_group_3 = random.sample(device_pool, 2)  # 2 shared devices  
shared_device_groups.append({"users": [89, 134, 156], "devices": shared_group_3})

# Mark shared devices as used
for group in shared_device_groups:
    for device in group["devices"]:
        used_devices.add(device["id"])

# Generate users
for i in range(NUM_USERS):
    user_id = f"U{str(i+1).zfill(4)}"
    name = fake.name()
    email = fake.email()
    age = random.randint(22, 60)
    location = random.choice(cities)
    occupation = random.choice(occupations)
    risk_score = round(random.uniform(5.0, 35.0), 1)
    signup_date = START_DATE + timedelta(days=random.randint(0, 600), minutes=random.randint(0, 1440))
    phone = fake.phone_number()

    accounts = []

    # Default account: savings
    account_savings = {
        "id": f"A{str(i*2+1).zfill(5)}",
        "type": "savings",
        "balance": round(random.uniform(3000.0, 500000.0), 2),
        "created_date": signup_date.isoformat() + "Z"
    }
    if args.fraud:
        account_savings["fraudFlag"] = True
    accounts.append(account_savings)

    # Optional credit account
    if random.random() < 0.5:
        account_credit = {
            "id": f"A{str(i*2+2).zfill(5)}",
            "type": "credit",
            "balance": round(random.uniform(5000.0, 100000.0), 2) * -1,
            "created_date": signup_date.isoformat() + "Z"
        }
        if args.fraud:
            account_credit["fraudFlag"] = True
        accounts.append(account_credit)

    # Assign devices to user
    user_devices = []
    
    # Check if user is in any shared device group
    user_in_shared_group = None
    for group in shared_device_groups:
        if i in group["users"]:
            user_in_shared_group = group
            break
    
    if user_in_shared_group:
        # User shares devices with others
        user_devices.extend(user_in_shared_group["devices"])
        
        # 30% chance to also have 1-2 personal devices
        if random.random() < 0.3:
            personal_device_count = random.randint(1, 2)
            available_devices = [d for d in device_pool if d["id"] not in used_devices]
            if available_devices:
                personal_devices = random.sample(available_devices, min(personal_device_count, len(available_devices)))
                user_devices.extend(personal_devices)
                for device in personal_devices:
                    used_devices.add(device["id"])
    else:
        # Regular user gets 1-3 unique devices
        device_count = random.choices([1, 2, 3], weights=[0.4, 0.4, 0.2])[0]  # Most have 1-2 devices
        available_devices = [d for d in device_pool if d["id"] not in used_devices]
        
        if len(available_devices) >= device_count:
            user_devices = random.sample(available_devices, device_count)
            for device in user_devices:
                used_devices.add(device["id"])
    
    # Add last_login timestamp to each device for this user
    for device in user_devices:
        device["last_login"] = (signup_date + timedelta(days=random.randint(1, 30), hours=random.randint(0, 23))).isoformat() + "Z"
        device["login_count"] = random.randint(5, 150)

    users.append({
        "id": user_id,
        "name": name,
        "email": email,
        "age": age,
        "location": location,
        "occupation": occupation,
        "risk_score": risk_score,
        "signup_date": signup_date.isoformat() + "Z",
        "phone": phone,
        "accounts": accounts,
        "devices": user_devices
    })

# Save to JSON
filename = f"users_{NUM_USERS}_india_fraud.json" if args.fraud else  f"users_{NUM_USERS}_india.json"
with open(filename, "w") as f:
    json.dump({"users": users}, f, indent=2)

print(f"✅ Generated {NUM_USERS} Indian users with accounts and devices in {filename}")
print(f"📱 Device sharing patterns:")
print(f"   - {len(shared_device_groups[0]['users'])} users sharing {len(shared_device_groups[0]['devices'])} devices (highly suspicious)")
print(f"   - {len(shared_device_groups[1]['users'])} users sharing {len(shared_device_groups[1]['devices'])} devices (moderately suspicious)")  
print(f"   - {len(shared_device_groups[2]['users'])} users sharing {len(shared_device_groups[2]['devices'])} devices (family/household)")
print(f"   - {NUM_USERS - sum(len(group['users']) for group in shared_device_groups)} users with unique devices")

# Show sample of shared devices for verification
print(f"\n🔍 Sample shared device IDs:")
for i, group in enumerate(shared_device_groups):
    device_ids = [d["id"] for d in group["devices"]]
    print(f"   Group {i+1}: {device_ids} (shared by users {group['users']})")

# Show device count distribution
device_counts = {}
for user in users:
    count = len(user["devices"])
    device_counts[count] = device_counts.get(count, 0) + 1

print(f"\n📊 Device count per user:")
for count, num_users in sorted(device_counts.items()):
    print(f"   {num_users} users have {count} device(s)")
