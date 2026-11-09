# Caso de Estudio Big Data — UARIV (Víctimas del Conflicto Armado)

**Asignatura:** Big Data 2026-II · Ciencia de Datos · Universidad Externado de Colombia
**Equipo (MABD01):** Julián Steven Duarte Gómez · Julián Camilo Jiménez Rodríguez · Juan Tomás Rincón Pinzón

## Descripción

Caracterización y distribución territorial del impacto del conflicto armado en Colombia a partir del
**Registro Único de Víctimas (RUV)**, administrado por la **Unidad para la Atención y Reparación Integral
a las Víctimas (UARIV)**.

El caso de estudio implementa un *pipeline* de procesamiento distribuido por lotes (**batch**) sobre un
clúster físico de **Apache Spark sobre YARN y HDFS** (1 nodo maestro + 2 nodos trabajadores).

## Conjunto de datos

- **Fuente oficial:** [Cifras de Víctimas por Hechos Municipal](https://www.datos.gov.co/Inclusi-n-Social-y-Reconciliaci-n/Cifras-de-V-ctimas-por-Hechos-Municipal/9qih-4vkc) — Portal Nacional de Datos Abiertos (`datos.gov.co`), ID `9qih-4vkc`.
- **Dimensiones:** 11.121.064 filas · 19 columnas · 3.330.779.150 bytes (3,33 GB / 3,10 GiB) · CSV UTF-8.
- **Cobertura temporal:** `FECHA_CORTE` desde 30/06/2024 hasta 30/09/2026 (cortes mensuales).

### Encabezado (19 columnas)

```
FECHA_CORTE,NOM_RPT,COD_PAIS,PAIS,COD_ESTADO_DEPTO,ESTADO_DEPTO,COD_CIUDAD_MUNI,CIUDAD_MUNICIPIO,
PARAM_HECHO,HECHO,ETNIA,SEXO,DISCAPACIDAD,CICLO_VITAL,PER_OCU,PER_DECLA,PER_UBIC,PER_SA,EVENTOS
```

## Estructura del repositorio

```
.
├── CasoBD_Duarte-Jimenez-Rincon_Notebook.ipynb   # Notebook preliminar (10 secciones) para el clúster
├── download_data.sh                              # Descarga el dataset completo desde datos.gov.co
├── data/
│   ├── victimas_uariv.csv                        # Dataset completo (3,33 GB) — NO versionado en Git
│   └── victimas_uariv_sample.csv                 # Muestra compacta (para exploración sin clúster)
└── README.md
```

## Descarga de los datos

```bash
bash download_data.sh
```

El archivo completo (`data/victimas_uariv.csv`) supera el límite de tamaño de GitHub (100 MB por archivo),
por lo que **no se versiona**. El repositorio incluye una muestra compacta
(`data/victimas_uariv_sample.csv`) para inspección rápida. Para el clúster, descargue el archivo completo
con el script y súbalo a HDFS:

```bash
hdfs dfs -mkdir -p /caso
hdfs dfs -put -f data/victimas_uariv.csv /caso/
hdfs dfs -ls /caso/victimas_uariv.csv
hdfs fsck /caso/victimas_uariv.csv -files -blocks -locations
```

## Ejecución del notebook

El notebook **solo se ejecuta en el clúster** (Spark sobre YARN y HDFS); no corre en una máquina personal.

1. Subir el dataset a HDFS en `/caso/victimas_uariv.csv` (replicación 2).
2. Abrir `CasoBD_Duarte-Jimenez-Rincon_Notebook.ipynb` en Jupyter (nodo maestro).
3. Ejecutar las 10 secciones en orden, registrando las capturas C1–C11 en el Anexo A1.

## Objetivos del análisis

1. Caracterizar la concentración espacial e histórica de hechos victimizantes (departamento y municipio).
2. Evaluar la brecha de atención humanitaria (personas declaradas vs. ubicadas).
3. Enriquecer los datos con un *Broadcast Join* sobre la codificación DANE (región natural).
