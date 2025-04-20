Les principales fonctionnalités du projet Flink dans le cadre du projet stream_data_archi_project incluent :

1. Analyse des Données en Temps Réel
Traitement des flux de données en temps réel pour des analyses instantanées.
2. Traitement de Flux Événementiels
Capacité à gérer des événements en continu, permettant des réponses rapides aux changements de données.
3. Intégration avec Kafka
Utilisation d'Apache Kafka pour la gestion des messages, facilitant la communication entre le producteur et le consommateur.
4. Fonctionnalités de Fenêtrage
Possibilité de définir des fenêtres temporelles pour agréger les données sur des périodes spécifiques.
5. État et Résilience
Gestion de l'état des applications, assurant une récupération après des pannes et une continuité des opérations.

--------------------------------------------------------------------------------------------------------------
Principales fonctionnalités du projet Kafka
Producteur Kafka :

Envoie des messages à un sujet(topics (transaction)).
Peut être configuré pour produire des messages en temps réel. donnees de transactions
Consommateur Kafka :

Lit les messages d'un sujets ( transaction).
Peut être configuré pour traiter les messages de manière synchrone.
Gestion des flux de données :
Permet la gestion et l'analyse des données en temps réel.
Supporte le traitement de grandes quantités de données de manière efficace.
Scalabilité :
Conçu pour être hautement scalable, permettant d'ajouter facilement des producteurs et des consommateurs.
Durabilité :

Les messages sont stockés de manière persistante sur un fichier csv, garantissant la durabilité des données.
Système de publication/abonnement :

Utilise un modèle de publication/abonnement, permettant aux producteurs de publier des messages et aux consommateurs de s'abonner à des sujets comme transaction.

Conclusion
Ces fonctionnalités font de Kafka un choix idéal pour les architectures de données en flux, permettant une communication efficace et une gestion des données en temps réel.

