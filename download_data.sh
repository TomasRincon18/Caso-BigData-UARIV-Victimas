#!/usr/bin/env bash
# =====================================================================
# Descarga del conjunto de datos del caso de estudio (UARIV)
# Cifras de Víctimas por Hechos Municipal — datos.gov.co ID: 9qih-4vkc
#
# Uso (en el nodo maestro del clúster, usuario hadoop):
#   bash download_data.sh
#
# El archivo se guarda como data/victimas_uariv.csv (3,33 GB / 3,10 GiB).
# Desde ahí se sube a HDFS con replicación 2:
#   hdfs dfs -mkdir -p /caso
#   hdfs dfs -put -f data/victimas_uariv.csv /caso/
#   hdfs dfs -ls /caso/victimas_uariv.csv
#   hdfs fsck /caso/victimas_uariv.csv -files -blocks -locations
# =====================================================================
set -euo pipefail

URL="https://www.datos.gov.co/api/views/9qih-4vkc/rows.csv?accessType=DOWNLOAD"
OUT="data/victimas_uariv.csv"

mkdir -p data

echo "Descargando el dataset desde datos.gov.co ..."
echo "URL: $URL"
echo "Destino: $OUT"

# curl con reanudación (-C -) y barra de progreso
curl -L -C - --retry 3 --progress-bar "$URL" -o "$OUT"

echo ""
echo "Descarga completada."
ls -lh "$OUT"
du -sb "$OUT"
