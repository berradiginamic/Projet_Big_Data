"""
Ce script importe des données d'un fichier CSV vers une table HBase.

**Fonctionnalités:**

* Lit le fichier CSV et le convertit en DataFrame Pandas.
* Remplace les valeurs 'NULL' par ''.
* Convertit la colonne 'datcde' en format datetime et filtre les valeurs invalides.
* Insère les données dans HBase avec la clé de ligne générée automatiquement.
* Gère les erreurs de conversion et d'insertion.

**Prérequis:**

* Installer les modules `pandas` et `happybase`.

**Utilisation:**

* Exécutez le script avec Python.
* Assurez-vous que le fichier CSV et la table HBase existent.
"""
from datetime import datetime

import pandas as pd
import happybase

# Configurer la connexion à HBase
hbase_table = 'dataFromagerie'  # Nom de votre table HBase
connection = happybase.Connection('node175910-env-1839015-etudiant18.sh1.hidora.com', 11560)
connection.open()

# Vérifier si la table existe
if hbase_table.encode() in connection.tables():
    # Si la table existe, la désactiver et la supprimer
    connection.disable_table(hbase_table)
    connection.delete_table(hbase_table)

# Créer une table HBase avec des familles de colonnes
column_families = {
    'cf': dict()  # Supposons que vous voulez toutes les colonnes sous une seule famille 'cf'
}
connection.create_table(hbase_table, column_families)

# Obtenir une référence à la table nouvellement créée
table = connection.table(hbase_table)

# Chemin vers votre fichier CSV
csv_file_path = '../Input/dataw_fro03.csv'


# Fonction pour importer des données dans HBase en fonction des critères
def import_data_to_hbase(csv_file_path, hbase_table):
    # Lire le fichier CSV dans un DataFrame pandas
    df = pd.read_csv(csv_file_path)

    # Remplacer les valeurs 'NULL' par ''
    df.fillna('', inplace=True)

    # Convertir la colonne 'datcde' au format datetime avec une spécification de format explicite
    df['datcde'] = pd.to_datetime(df['datcde'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

    # Filtrer les lignes avec des valeurs datetime invalides
    df = df.dropna(subset=['datcde'])

    # Filtrer les lignes avec une date supérieure ou égale à 01/01/2004
    df = df[df['datcde'] >= datetime(2004, 1, 1)]

    # Convertir la colonne 'datcde' au format chaîne de caractères
    df['datcde'] = df['datcde'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Itérer sur le DataFrame filtré et insérer les données dans HBase
    for index, row in df.iterrows():
        try:
            # Insérer les données dans HBase sans spécifier la clé de ligne (HBase la générera)
            table.put(str(index).encode(), {  # Utiliser l'index comme clé de ligne
                # Colonnes et valeurs
                b'cf:codecli': str(row['codcli']).encode(),
                b'cf:genrecli': str(row['genrecli']).encode(),
                b'cf:nomcli': str(row['nomcli']).encode(),
                b'cf:prenomcli': str(row['prenomcli']).encode(),
                b'cf:cpcli': str(row['cpcli']).encode(),
                b'cf:ville': str(row['villecli']).encode(),
                b'cf:codecde': str(row['codcde']).encode(),
                b'cf:date': str(row['datcde']).encode(),
                b'cf:timbrecli': str(row['timbrecli']).encode(),
                b'cf:timbrecde': str(row['timbrecde']).encode(),
                b'cf:Nbcolis': str(row['Nbcolis']).encode(),
                b'cf:cheqcli': str(row['cheqcli']).encode(),
                b'cf:barchive': str(row['barchive']).encode(),
                b'cf:bstock': str(row['bstock']).encode(),
                b'cf:codobj': str(row['codobj']).encode(),
                b'cf:qte': str(row['qte']).encode(),
                b'cf:Colis': str(row['Colis']).encode(),
                b'cf:libobj': str(row['libobj']).encode(),
                b'cf:Tailleobj': str(row['Tailleobj']).encode(),
                b'cf:Poidsobj': str(row['Poidsobj']).encode(),
                b'cf:points': str(row['points']).encode(),
                b'cf:indispobj': str(row['indispobj']).encode(),
                b'cf:libcondit': str(row['libcondit']).encode(),
                b'cf:prixcond': str(row['prixcond']).encode(),
                b'cf:puobj': str(row['puobj']).encode()
            })
        except ValueError:
            pass  # Passer à l'itération suivante s'il y a une erreur


# Appeler la fonction d'importation
import_data_to_hbase(csv_file_path, hbase_table)

# Fermer la connexion à HBase
connection.close()
print("Insertion Terminé")
