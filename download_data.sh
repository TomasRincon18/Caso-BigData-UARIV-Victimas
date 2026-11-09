#!/usr/bin/env bash
# =====================================================================
# Descarga del conjunto de datos del caso de estudio (UARIV)
# Cifras de Víctimas por Hechos Municipal — datos.gov.co ID: 9qih-4vkc
# Respaldo oficial en Google Drive: 1YxLHcfDSIAhB7-eubG579c5jxEcTrdaB
#
# Uso (en el nodo maestro del clúster, usuario hadoop):
#   bash download_data.sh              # Descarga rápida desde respaldo verificado (Google Drive)
#   bash download_data.sh --oficial    # Descarga directa desde portal datos.gov.co
#
# El archivo se guarda como data/victimas_uariv.csv (3.330.779.150 bytes / 3,33 GB).
# Desde ahí se sube a HDFS con replicación 2:
#   hdfs dfs -mkdir -p /caso
#   hdfs dfs -put -f data/victimas_uariv.csv /caso/
#   hdfs dfs -ls /caso/victimas_uariv.csv
#   hdfs fsck /caso/victimas_uariv.csv -files -blocks -locations
# =====================================================================
set -euo pipefail

OUT="data/victimas_uariv.csv"
mkdir -p data

MODE="${1:-backup}"

if [ "$MODE" = "--oficial" ]; then
    URL="https://www.datos.gov.co/api/views/9qih-4vkc/rows.csv?accessType=DOWNLOAD"
    echo "Descargando dataset desde datos.gov.co (portal oficial)..."
    echo "URL: $URL"
    echo "Destino: $OUT"
    curl -L --retry 3 --progress-bar "$URL" -o "$OUT"
else
    echo "Descargando dataset desde URL directa de respaldo (Google Drive CDN)..."
    echo "Destino: $OUT"
    curl -c /tmp/cookies.txt -s -L "https://drive.google.com/uc?export=download&id=1YxLHcfDSIAhB7-eubG579c5jxEcTrdaB" > /dev/null && \
    curl -b /tmp/cookies.txt -L --retry 3 --progress-bar "https://drive.usercontent.google.com/download?id=1YxLHcfDSIAhB7-eubG579c5jxEcTrdaB&export=download&confirm=t" -o "$OUT"
    rm -f /tmp/cookies.txt
fi

echo ""
echo "=== Descarga completada exitosamente ==="
ls -lh "$OUT"
du -sb "$OUT"

