# Plan paso a paso — Caso de Estudio Big Data 2026-II (UARIV)

> **Documento nuevo.** Complementa y no reemplaza a `PLAN_DE_TRABAJO_CLUSTER.md` (que se conserva intacto).
> **Equipo MABD01:** Julián Steven Duarte Gómez (A) · Julián Camilo Jiménez Rodríguez (B) · Juan Tomás Rincón Pinzón (C) · D: equipo conjunto.
> **Cliente:** UARIV · **Dataset:** *Cifras de Víctimas por Hechos – Municipal* (`datos.gov.co`, ID `9qih-4vkc`).

---

## 0. Resumen del estado actual

| Elemento | Estado |
|---|---|
| Repositorio GitHub | `TomasRincon18/Caso-BigData-UARIV-Victimas` (privado) |
| Dataset completo | ✅ descargado, **3.330.779.150 bytes exactos**, 11.121.064 filas |
| Muestra para inspección | ✅ `data/victimas_uariv_sample.csv` (100.000 filas) |
| Notebook preliminar | ✅ 38 celdas · 10 secciones + `S4d` + gráfico de evolución (`S10e`) |
| Plantillas Word (Anexos y Bitácora) | ✅ en la carpeta del repo |
| Datos en HDFS | ⏳ pendiente (Sesión 1) |
| Bonificación | ✅ **SMJ vs. Broadcast Hash Join (+0,3)** elegida (no se usa MLlib) |

## 1. Valores verificados sobre el archivo completo (usar como control de calidad)

| Métrica | Valor |
|---|---|
| Filas totales | **11.121.064** |
| Suma total `EVENTOS` | **346.571.518** |
| Suma `EVENTOS` solo 2026 | **127.922.758** |
| Cortes distintos `FECHA_CORTE` | **28** (2024: 7 · 2025: 12 · 2026: 9) |
| Filas de 2026 | **3.879.564** |
| Departamentos (COD) | **34** (incluye `0` = Sin Definir) |
| Municipios / Hechos | **1.126 / 22** |
| Nulos categóricas (`ESTADO_DEPTO`, `CIUDAD_MUNICIPIO`, `HECHO`, `ETNIA`, `SEXO`) | **0 (0 %)** |
| Nulos `PER_DECLA` / `PER_UBIC` | **41,86 % / 32,40 %** |
| Codificación | UTF-8 válido (0 líneas con bytes inválidos) |

**Formatos de `FECHA_CORTE`** (el notebook ya los normaliza):
`dd/MM/yyyy` · `dd/MM/yy` (`30/09/24`) · `yyyy/MM/dd` (`2024/08/31`) · `dd/MM/yyyy 00:00`.

**Cortes 2026 (filas / EVENTOS):**

| Corte | Filas | EVENTOS |
|---|---:|---:|
| 31/01/2026 | 430.792 | 14.030.894 |
| 28/02/2026 | 431.495 | 14.058.067 |
| 31/03/2026 | 432.095 | 14.090.618 |
| 30/04/2026 | 432.558 | 14.140.966 |
| 31/05/2026 | 433.433 | 14.202.763 |
| 30/06/2026 | 427.616 | 14.255.857 |
| 31/07/2026 | 428.472 | 14.312.816 |
| 31/08/2026 | 430.339 | 14.371.373 |
| 30/09/2026 | 432.764 | 14.459.404 |

## 2. Ajustes ya aplicados al notebook (changelog)

- **Imports:** se añadieron `month` y `coalesce`.
- **S4b:** ahora reporta nulos de *todas* las columnas relevantes (categóricas + `PER_*`) con conteo y porcentaje. Las categóricas dan 0; el hallazgo está en `PER_DECLA`/`PER_UBIC`.
- **S4c:** normaliza `FECHA_CORTE` (4 formatos) a `FECHA_CORTE_DT` y deriva `ANIO`/`MES`; verifica cuántos registros quedan sin parsear (debe ser 0).
- **S4d:** detalle de los 9 cortes de 2026 con control de calidad (esperado: 9 cortes · 3.879.564 filas · 127.922.758 EVENTOS).
- **S7 (markdown):** se confirma por escrito la bonificación **SMJ vs. BHJ (+0,3)** y se descarta MLlib.
- **S10e (nueva):** **gráfico 3** de evolución mensual 2026 (9 puntos, patrón agregar → `.toPandas()` → matplotlib).
- **S10f:** renombrado el cierre (`unpersist()`).
- **Portada/Siguientes pasos:** se dejó constancia de la bonificación elegida.

> ⚠️ Recordatorio: el notebook **solo corre en el clúster** (Spark sobre YARN + HDFS). En local no se ejecuta.

---

## FASE S0 — Preparación previa (ya cubierta / cerrar)

- [ ] Confirmar que el respaldo en Google Drive está accesible por URL directa.
- [ ] Verificar `df -h` en el equipo anfitrión y anotar el valor para la bitácora.
- [ ] Registro **S0** en la bitácora (fecha, integrante, actividad, evidencia, uso de IA).

---

## FASE S1 — Lunes 9 de noviembre (Almacenamiento + SparkSession + EDA)

**Objetivo:** datos en HDFS con replicación 2, `application_id` de referencia y capturas C1–C5.

1. **Actualizar repositorio en el nodo maestro**
   ```bash
   cd ~ && git clone https://github.com/TomasRincon18/Caso-BigData-UARIV-Victimas.git
   cd Caso-BigData-UARIV-Victimas && git pull origin master
   ```
2. **Descargar el dataset** (3,33 GB)
   ```bash
   bash download_data.sh
   # Verificar: stat -c '%s' data/victimas_uariv.csv  ->  3330779150
   ```
3. **Subir a HDFS con replicación 2**
   ```bash
   hdfs dfs -mkdir -p /caso
   hdfs dfs -Ddfs.replication=2 -put -f data/victimas_uariv.csv /caso/
   hdfs dfs -ls /caso/victimas_uariv.csv        # columna de replicación = 2
   ```
4. **Capturas C1, C2, C3**
   - **C1:** `http://<master>:9870` → *Datanodes* (2 nodos "In service").
   - **C2:** *Utilities › Browse the file system* → `/caso/victimas_uariv.csv` (tamaño + Replication = 2).
   - **C3:** terminal (`usuario hadoop`, con fecha/hora visible):
     ```bash
     hdfs fsck /caso/victimas_uariv.csv -files -blocks -locations
     ```
     Verificar: `Total size: 3330779150 B`, `Average block replication: 2.0`, bloques repartidos en ambos DataNodes.
5. **Abrir Jupyter y ejecutar Secciones 1–2–3**
   - Sección 1 → `Master: yarn` y `Application ID: application_...`.
   - Sección 2 → esquema de 19 campos.
   - Sección 3 → `df.rdd.getNumPartitions()` (anotar; ≈ nº de bloques, 25–27).
6. **Capturas C4 y C5**
   - **C4:** `http://<master>:8088` → aplicación de referencia (app_id, usuario `hadoop`, nombre `CasoBD_UARIV_Victimas`, estado).
   - **C5:** Spark UI → *Environment* → *Spark Properties* (con `application_id` en la URL).
7. **Ejecutar Sección 4 (EDA)** y contrastar:
   - S4a → **11.121.064**; S4b → categóricas 0 %, `PER_DECLA` ≈ 41,86 %; S4c → años 2024/2025/2026; S4d → 9 cortes y totales esperados.
8. **Cerrar:** registro **S1** en bitácora con el `application_id` de referencia.

**Salida esperada de la fase:** C1, C2, C3, C4, C5 + app_id de referencia registrado.

---

## FASE S2 — Miércoles 11 de noviembre (Procesamiento, joins, AQE, Spark UI)

1. **Reanudar SparkSession** (Sección 1). Si cambia el `application_id`, anotarlo como secundario; el de referencia sigue siendo el de S1.
2. **Sección 5 — Agregación multidimensional**
   - `groupBy(COD_ESTADO_DEPTO, ESTADO_DEPTO, CIUDAD_MUNICIPIO, HECHO)`.
   - Métricas: `TOTAL_EVENTOS`, `TOTAL_DECLARANTES`, `TOTAL_UBICADOS`, `TASA_EFECTIVIDAD_UBICACION`.
   - Anotar filas resultantes (~15.000–25.000).
3. **Sección 6 — Broadcast Join con tabla satélite DANE**
   - `S6a` crea 34 filas (incluye `0 = Sin Definir`).
   - `S6b` hace `df_agregado.join(broadcast(df_regiones), on="COD_ESTADO_DEPTO", how="left")`.
4. **Sección 7 — Bonificación SMJ vs. BHJ (captura C11)** ✅ elegida
   - `S7a`: `autoBroadcastJoinThreshold = -1` (Sort Merge Join; plan con `SortMergeJoin` + `Exchange`).
   - `S7b`: umbral `10485760` + `broadcast()` (Broadcast Hash Join; sin `Exchange` de la tabla grande).
   - `S7c`: comparación de tiempos y *speedup*.
   - **C11:** plan físico + métricas de shuffle de ambos escenarios.
5. **Sección 8 — Plan y AQE**
   - `S8a`: `.explain(True)` → **C6** con `AdaptiveSparkPlan isFinalPlan=false`.
   - `S8b`: acción que dispara el plan final → **C7** en Spark UI (*SQL/DataFrame*, `isFinalPlan=true`).
6. **Spark UI → Stages (C8 y C9)**
   - **C8 (Stage relevante 1):** mayor *Shuffle Write* (agregación/join). DAG + *Summary Metrics*.
   - **C9 (Stage relevante 2):** mayor duración. DAG + *Summary Metrics* (Duration, Shuffle Read/Write, Spill).
7. **Sección 9 — Persistencia (C10)**
   - `S9`: `persist(StorageLevel.MEMORY_AND_DISK)` + `count()` (materializa).
   - **C10:** Storage → `Memory and Disk`, `Fraction Cached: 100 %`, `Size in Memory/On Disk`.
   - `S9b`: reporte por región (demuestra reutilización del cache).
8. **Anexo A2:** consignar Stage IDs, tareas, shuffle, duración mediana/máx, spill (o «ninguno»), tiempos y cache.
9. **Registro S2** en bitácora.

**Salida esperada de la fase:** C6, C7, C8, C9, C10, C11 + A2 completo.

---

## FASE S3 — Viernes 13 de noviembre (Visualización, QC y cierre)

1. **Sección 10 — Visualización**
   - `S10a`: top 10 municipios (≤ 35 filas).
   - `S10b`: `.toPandas()`.
   - `S10c`: gráfico 1 (municipios, Okabe-Ito azul, `fmt_co`).
   - `S10d`: gráfico 2 (regiones, naranja, `fmt_co`).
   - `S10e`: **gráfico 3** (evolución mensual 2026, 9 cortes).
   - `S10f`: `unpersist()`.
2. **Auditoría de suficiencia C1–C11**
   - Todas con fecha/hora; C5, C7–C11 con `application_...` en la URL; C6 con la celda completa; C3 con prompt `hadoop`.
3. **Incidentes (A4):** documentar I1, I2… con captura y acción; si no hubo, escribir «Sin incidentes».
4. **Guardar notebook con salidas**, `git add/commit/push` del `.ipynb`.
5. **Registro S3** en bitácora y anotar la franja de sustentación (correo del 13-nov).

---

## FASE TI — 14 al 16 de noviembre (Entregables)

| Entregable | Base | Regla crítica |
|---|---|---|
| `CasoBD_Duarte-Jimenez-Rincon_Informe.pdf` | redacción | **máx. 1 página**, TNR 12, márgenes 3 cm |
| `CasoBD_Duarte-Jimenez-Rincon_Anexos.pdf` | `Plantilla_Anexos` | A1–A7; C1–C11 con análisis de 2–4 líneas |
| `CasoBD_Duarte-Jimenez-Rincon_Notebook.ipynb` | notebook final | salidas completas del clúster |
| `CasoBD_Duarte-Jimenez-Rincon_Presentacion.pdf` | nueva | portada + máx. 3 slides por componente |
| `CasoBD_Duarte-Jimenez-Rincon_Bitacora.pdf` | `Plantilla_Bitacora` | S0–TI + firmas de los 3 |

**A5 (cloud vs on-premise):** usar un tiempo real del clúster + el valor de **bytes procesados** de la práctica de BigQuery (carpeta `23_Oct/`).

**A6 (IA):** tabla por componente (A/B/C/D), contribución intelectual (≤ 150 palabras) y firmas.

**Entrega:** martes 17-nov, 8:00 a. m., en Teams (un solo envío).

---

## Mapa de capturas (resumen)

| Captura | Fase | Requisito | Dónde |
|:--:|:--:|:--:|---|
| C1 | S1 | 1 | HDFS UI → Datanodes |
| C2 | S1 | 1 | HDFS UI → Browse (`Replication = 2`) |
| C3 | S1 | 1 | `hdfs fsck` en terminal |
| C4 | S1 | 2 | YARN UI (app_id de referencia 9-nov) |
| C5 | S1 | 2 | Spark UI → Environment |
| C6 | S2 | 5 | Notebook `.explain(True)` → `isFinalPlan=false` |
| C7 | S2 | 5 | Spark UI → SQL/DataFrame → `isFinalPlan=true` |
| C8 | S2 | 6 | Spark UI → Stages (stage 1) |
| C9 | S2 | 6 | Spark UI → Stages (stage 2) |
| C10 | S2 | 8 | Spark UI → Storage (persist) |
| C11 | S2 | Bonif. | Planes + shuffle SMJ vs. BHJ |

---

## Cómo maximizar la rúbrica

- **Selección del dataset (10 %):** ya cumple (fuente oficial, 3,33 GB, actualización > 1-jul-2026, cobertura 2026). Cita APA 7 del portal.
- **Análisis y resultados (25 %):** dos transformaciones complejas (agregación + broadcast join), hallazgos territoriales y brecha humanitaria, 3 visualizaciones con `fmt_co` y paleta accesible.
- **Uso de Big Data (45 %):** HDFS rep 2 (C1–C3), SparkSession justificada (C4–C5), `.explain()` + AQE (C6–C7), 2 stages con métricas (C8–C9), persistencia justificada (C10).
- **Informe y anexos (20 %):** 1 página + A1–A7 completos + A5 con valores observados.
- **Bonificación:** SMJ vs. BHJ (+0,3) documentada en C11.

## Riesgos y mitigaciones

| Riesgo | Mitigación |
|---|---|
| Formatos de `FECHA_CORTE` | ya normalizados (S4c) y validados en S4d |
| Nulos en `PER_*` | se reportan; se excluyen del denominador de la tasa |
| Memoria (8 GB/nodo) | `executor.memory=2g`, `shuffle.partitions=8`, `unpersist()` |
| Capturas sin `application_id`/hora | checklist C1–C11 antes de cerrar cada sesión |
| URL oficial bloqueada | respaldo en Google Drive (URL directa) |

## Checklist final antes de entregar

- [ ] Notebook con salidas completas y `spark.stop()` **no** ejecutado antes de capturar.
- [ ] C1–C11 legibles, con hora y `application_id` donde aplica.
- [ ] A2 con la fuente de cada valor.
- [ ] Informe en 1 página exacta.
- [ ] Declaración de IA + firmas en Anexos y Bitácora.
- [ ] Nombres de archivo con el patrón `CasoBD_<Apellido1>-<Apellido2>-<Apellido3>_<Entregable>`.
