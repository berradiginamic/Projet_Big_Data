"""
Ce script analyse un ensemble de données HBase pour les ventes de produits.

Il calcule :

- La commande avec la plus grande somme de quantités vendues à Nantes entre le 1er janvier 2020 et le 31 décembre 2020.
- Le nombre total de commandes par année pour toutes les villes.

## Fonctionnalités :

- Se connecte à une instance HBase spécifiée.
- Scanne une table HBase spécifiée.
- Extrait les données des colonnes 'codecli', 'ville', 'codecde', 'date' et 'qte'.
- Calcule la somme des quantités pour chaque commande.
- Calcule le nombre total de commandes pour chaque année.
- Affiche la commande avec la plus grande somme de quantités vendues à Nantes.
- Exporte les résultats dans un fichier CSV.
- Ferme la connexion à HBase.

## Prérequis :

- Avoir installé les modules `happybase`, `datetime` et `csv`.
- Avoir une instance HBase accessible.

## Exemple d'utilisation :

```python
python hbase_lot3_Q1.py


## Remarques :

- Ce script peut être facilement modifié pour analyser d'autres types de données HBase.
- La plage de dates et le critère de filtrage sur la ville peuvent être modifiés en fonction des besoins.
"""

import csv
from datetime import datetime
import happybase

# Configurer la connexion à HBase
connection = happybase.Connection('node175910-env-1839015-etudiant18.sh1.hidora.com', 11560)  # Mettre à jour avec votre hôte et port HBase
connection.open()

# Sélectionner la table HBase
table_name = 'dataFromagerie'  # Mettre à jour avec le nom de votre table HBase
table = connection.table(table_name)

# Initialiser un dictionnaire pour stocker la somme des quantités par commande
quantities_by_order = {}
# Initialiser un dictionnaire pour stocker le nombre total de commandes par année
total_orders_by_year = {}

# Scanner les lignes de la table HBase
for key, data in table.scan():
    # Extraire les données de chaque ligne
    codecli = data[b'cf:codecli'].decode()
    ville = data[b'cf:ville'].decode()
    codecde = data[b'cf:codecde'].decode()
    date_str = data[b'cf:date'].decode()
    qte = data[b'cf:qte'].decode()

    # Extraction de la date de la colonne et définition des dates de début et de fin
    start_date = datetime.strptime('2020-01-01', "%Y-%m-%d")
    end_date = datetime.strptime('2020-12-31', "%Y-%m-%d")

    try:
        # Vérifie si la date est valide et dans la plage spécifiée
        date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

        # Vérifie si la date est dans la plage spécifiée et que le département correspond à certains critères
        if start_date <= date <= end_date and ville == 'NANTES':
            # Mise à jour de la somme des quantités pour la commande actuelle
            quantities_by_order[codecde] = quantities_by_order.get(codecde, 0) + int(qte)

            # Mise à jour du nombre total de commandes pour l'année actuelle
            total_orders_by_year[date.year] = total_orders_by_year.get(date.year, 0) + 1

    except ValueError:
        pass  # Passe à la prochaine itération en cas d'erreur de valeur lors de la conversion de la date

# Imprimer la commande avec la plus grande somme des quantités
if quantities_by_order:
    max_order = max(quantities_by_order, key=quantities_by_order.get)
    print(f'Meilleure vente à Nantes : Commande {max_order} avec une somme de quantités de {quantities_by_order[max_order]}')

# Fermer la connexion à HBase
connection.close()

# Export des résultats dans un fichier CSV
#csv_filename = '/datavolume1/resultats_lot3_1.csv' # Répertoire Hbase
csv_filename = '../Output/Lot_3/resultats_lot3_1.csv'

with open(csv_filename, mode='w', newline='') as file:
    writer = csv.writer(file)

    # Écrire l'en-tête du fichier CSV
    writer.writerow(['Commande', 'Somme des quantites'])

    # Écrire chaque ligne de données dans le fichier CSV
    for order, quantity in quantities_by_order.items():
        writer.writerow([order, quantity])

print(f"Les resultats ont ete exportes dans le fichier CSV : {csv_filename}")
