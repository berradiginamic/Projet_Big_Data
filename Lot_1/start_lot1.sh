# Copie du fichier hadoop-streaming-2.7.2.jar depuis le répertoire spécifié vers le répertoire courant
cp /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar .

# Démarrage de Hadoop
./start-hadoop.sh

# Création du répertoire "input" sur HDFS (-p pour créer les répertoires parents si nécessaire)
hdfs dfs -mkdir -p input

# Copie du fichier dataw_fro03.csv vers le répertoire "input" sur HDFS
hdfs dfs -put dataw_fro03.csv input

# Suppression récursive du répertoire "output_lot1_exo1" sur HDFS
hdfs dfs -rm -r output/output_lot1_exo1

# Pause de 5 secondes pour permettre à Hadoop de terminer la suppression avant de démarrer le job
sleep 5

# Exécution d'un job MapReduce avec Hadoop Streaming
hadoop jar hadoop-streaming-2.7.2.jar -file mapper_lot1.py -mapper "python3 mapper_lot1.py" -file reducer_lot1.py -reducer "python3 reducer_lot1.py" -input input/dataw_fro03.csv -output output/output_lot1_exo1
