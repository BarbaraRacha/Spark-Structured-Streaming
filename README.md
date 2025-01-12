# Application Spark pour la gestion des incidents hospitaliers en streaming

Ce projet implémente une application Apache Spark qui reçoit les incidents de l'hôpital en temps réel grâce à **Structured Streaming**. Les incidents arrivent en continu sous forme de fichiers CSV, chaque fichier contenant des données sur les événements hospitaliers.

Le format des données dans ces fichiers CSV est le suivant :

![Format des données CSV](images/img_8.png)

## Code de la classe "IncidentAppSpark"

Voici le code utilisé pour implémenter l'application Spark :

### Partie 1 - Initialisation et configuration

![Code initialisation](images/img_9.png)

### Partie 2 - Traitement des données et Streaming

![Code traitement streaming](images/img_10.png)

## Sortie de l'application

L'application affiche les résultats en temps réel. Voici à quoi ressemble la sortie de l'application :

![Sortie 1](images/img_6.png)
![Sortie 2](images/img_7.png)

