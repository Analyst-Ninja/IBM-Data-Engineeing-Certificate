from kafka import KafkaProducer
import json

producer = KafkaProducer(value_serializer = lambda v : json.dumps(v).encode('utf-8'))
txnId = 101
while True:
    user_input = input("Do you want to use ATM service (press 'q' to stop)")
    if user_input.lower() == 'q':
        producer.flush()
        producer.close()
        break
    else:
        atm_choice = int(input("Which ATM you want to transact in (1 or 2) : "))
        if atm_choice in [1,2]:
            atmId = int(input('Enter AtmId : '))
            producer.send("bankbranch", {'atmid':atmId, 'transid':txnId})
            producer.flush()
            txnId += 1
        else:
            print("Invalid ATM")
            continue
