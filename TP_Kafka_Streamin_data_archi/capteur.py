import uuid
import json
import time
import pandas as pd
from datetime import datetime
from confluent_kafka import Producer

class Transaction:
    def __init__(self, id_transaction, type_transaction, montant, devise, date, lieu, moyen_paiement, details, utilisateur):
        self.id_transaction = id_transaction
        self.type_transaction = type_transaction
        self.montant = montant
        self.devise = devise
        self.date = date
        self.lieu = lieu
        self.moyen_paiement = moyen_paiement
        self.details = details
        self.utilisateur = utilisateur

    def to_json(self):
        return json.dumps(self.__dict__)

def read_transactions_from_csv(filepath):
    df = pd.read_csv(filepath)
    transactions = []

    for _, row in df.iterrows():
        transaction = Transaction(
            id_transaction=str(row["id_transaction"]),
            type_transaction=row["type_transaction"],
            montant=float(row["montant"]),
            devise=row["devise"],
            date=row["date"],
            lieu=row["lieu"],
            moyen_paiement=row["moyen_paiement"],
            details=json.loads(row["details"]),
            utilisateur=json.loads(row["utilisateur"])
        )
        transactions.append(transaction)
    return transactions

def main():
    bootstrap_servers = 'localhost:9092'
    topic = 'transaction'
    csv_file_path = 'transactions.csv' 

    producer = Producer({'bootstrap.servers': bootstrap_servers})

    while True:
        transactions = read_transactions_from_csv(csv_file_path)

        try:
            for transaction in transactions:
                print(f"Message envoyé : {transaction.to_json()}")
                producer.produce(topic, key=transaction.id_transaction, value=transaction.to_json())
                producer.flush()
                time.sleep(1)
        except KeyboardInterrupt:
            break  
        time.sleep(3)  

    producer.flush()

if __name__ == "__main__":
    main()
