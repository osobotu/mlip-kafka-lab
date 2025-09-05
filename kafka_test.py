from kafka import KafkaAdminClient

try:
    admin = KafkaAdminClient(bootstrap_servers="localhost:9092")
    print("Connected to Kafka!")
    print("Brokers:", admin.list_topics())
except Exception as e:
    print("Connection failed:", e)


"""
assumptions:
1. the device wouldn't break when you fall
2. the user will always wear the device and wear it appropriately
3. assume constant internet connection
4. the user the may have a physical disability and cannot wear the device
5. the user may not charge the device
6. the user is not 


what can go wrong:
1. false negative when the user hasn't fallen
2. the emergency contact is unavailable
3. Device breaks when the user falls and no detection and no reporting.
4. true negative - the user falls and it is not detected
"""