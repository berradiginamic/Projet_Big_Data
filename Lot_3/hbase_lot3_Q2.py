"""
Ce script analyse un ensemble de données HBase pour compter le nombre total de commandes par année entre 2010 et 2015.

## Fonctionnalités :

- Se connecte à une instance HBase spécifiée.
- Scanne une table HBase spécifiée.
- Extrait les données des colonnes 'codecli' et 'date'.
- Convertit les dates en objets datetime.
- Compte le nombre de commandes pour chaque année entre 2010 et 2015.
- Affiche le nombre total de commandes par année.
- Exporte un graphique à barres représentant le nombre total de commandes par année au format PDF.
- Ferme la connexion à HBase.

## Prérequis :

- Avoir installé les modules `happybase`, `datetime` et `matplotlib`.
- Avoir une instance HBase accessible.

## Exemple d'utilisation :

```python
python hbase_lot3_Q2.py


## Remarques :

- Le script peut être facilement modifié pour analyser d'autres plages de dates ou d'autres colonnes de la table HBase.
- Il est possible d'ajouter des fonctionnalités pour enregistrer les résultats dans un fichier ou une base de données.
"""

from datetime import datetime
import happybase
from matplotlib import pyplot as plt

# Initialiser un dictionnaire pour stocker le nombre total de commandes par année
total_orders_by_year = {}

# Configurer la connexion à HBase
connection = happybase.Connection('node175910-env-1839015-etudiant18.sh1.hidora.com', 11560)  # Mettre à jour avec votre hôte et port HBase
connection.open()

# Sélectionner la table HBase
table_name = 'dataFromagerie'  # Mettre à jour avec le nom de votre table HBase
table = connection.table(table_name)

# Scanner les lignes de la table HBase
for key, data in table.scan():
    # Extraire les données de chaque ligne
    codecli = data[b'cf:codecli'].decode()
    date_str = data[b'cf:date'].decode()

    try:
        # Convertir la date en objet datetime
        date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

        # Vérifier si la date est dans la plage spécifiée (2010 à 2015)
        if datetime(2010, 1, 1) <= date <= datetime(2015, 12, 31):
            # Mettre à jour le nombre total de commandes pour l'année actuelle
            total_orders_by_year[date.year] = total_orders_by_year.get(date.year, 0) + 1

    except ValueError:
        pass  # Passer à l'itération suivante en cas d'erreur de valeur lors de la conversion de la date

# Imprimer le nombre total de commandes par année entre 2010 et 2015
for year, total_orders in total_orders_by_year.items():
    print(f"Année {year}: Nombre total de commandes = {total_orders}")

# Fermer la connexion à HBase
connection.close()

# Créer le graphique à barres
plt.bar(total_orders_by_year.keys(), total_orders_by_year.values(), color='blue')
plt.xlabel('Année')
plt.ylabel('Nombre total de commandes')
plt.title('Nombre total de commandes par année entre 2010 et 2015')

# Sauvegarder le graphique au format PDF
#pdf_filename = '/datavolume1/Lot_3/total_orders_by_year.pdf' # Répertoire Hbase
pdf_filename = '../Output/Lot_3/resultats_lot3_2.pdf'
plt.savefig(pdf_filename)

# Afficher le graphique
# plt.show()

print(f"Le graphique a ete exporte en format PDF : {pdf_filename}")
