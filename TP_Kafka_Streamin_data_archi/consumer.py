from confluent_kafka import Consumer, KafkaError
import json
import os
import csv

def write_to_csv(data, filename):
    # Liste des colonnes attendues
    fieldnames = [
        "id_transaction", "type_transaction", "montant", "devise",
        "date", "lieu", "moyen_paiement", "details", "utilisateur"
    ]

    # Vérifie si le fichier existe pour savoir si on écrit l'en-tête
    file_exists = os.path.isfile(filename)

    with open(filename, mode='a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Écrire l’en-tête si le fichier est nouveau
        if not file_exists:
            writer.writeheader()

        # Convertir les champs complexes (dictionnaires) en chaînes JSON
        data["details"] = json.dumps(data["details"], ensure_ascii=False)
        data["utilisateur"] = json.dumps(data["utilisateur"], ensure_ascii=False)

        writer.writerow(data)

def main():
    bootstrap_servers = 'localhost:9092'
    group_id = 'transaction_group'
    topic = 'transaction'
    output_file = 'transactions_reçues.csv'

    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([topic])
    print("🟢 Consumer actif... En attente de messages.")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Erreur : {msg.error()}")
                    continue

            # Désérialisation et affichage
            transaction = json.loads(msg.value().decode('utf-8'))
            print(f"✅ Transaction reçue : {transaction['id_transaction']}")

            # Écriture dans le fichier CSV
            write_to_csv(transaction, output_file)

    except KeyboardInterrupt:
        print("🛑 Arrêt du consumer.")

    finally:
        consumer.close()

if __name__ == "__main__":
    main()
