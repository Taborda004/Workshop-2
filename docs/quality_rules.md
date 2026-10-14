# Catálogo de Reglas de Calidad y Matriz de Trazabilidad de Extremo a Extremo (Workshop 2)

Este documento formaliza el marco de calidad de datos y confiabilidad del pipeline ETL para el proyecto **Spotify & Grammy Awards Data Warehouse**, en estricto cumplimiento de las secciones **4.1, 6.5, 6.6 y 8.2 del PDF oficial del Workshop 2**.

---

## 1. Cadena de Razonamiento de Extremo a Extremo (End-to-End Reasoning Chain)

Cada decisión de ingeniería en este pipeline responde a la siguiente cadena lógica de preguntas:

```mermaid
flowchart LR
    R["1. Requisito Analítico"] --> D["2. Datos Requeridos"]
    D --> P["3. Evidencia Profiling"]
    P --> RK["4. Riesgo de Calidad"]
    RK --> Q["5. Regla de Calidad"]
    Q --> GX["6. Expectativa GX (Severidad)"]
    GX --> T["7. Transformación"]
    T --> DW["8. Elemento DW"]
    DW --> KPI["9. KPI / Visualización"]
```

---

## 2. Catálogo Oficial de Reglas de Calidad (Quality Rules Catalog)

Se definen **18 reglas de calidad explícitas** distribuidas en los dos compuertas (*Quality Gates*), cada una justificada por evidencia empírica de *profiling*, contratos de origen o invariantes de negocio:

### 2.1. Raw Quality Gate: Spotify (`spotify_dataset.csv`)

| Rule ID | Capa / Dataset | Atributo | Dimensión de Calidad | Regla de Calidad | Métrica / Umbral | Severidad | Requisito Relacionado | Justificación de Ingeniería |
|---|---|---|---|---|---|---|---|---|
| **DQ-RAW-01** | Spotify Raw | Tabla completa | Conformidad | La estructura debe contener exactamente 21 columnas | Conteo = 21 | `CRITICAL` | Todos | Contrato de esquema. Faltantes de columnas indican corrupción del archivo fuente. |
| **DQ-RAW-02** | Spotify Raw | Tabla completa | Completitud | El volumen total de filas debe estar en el rango esperado | [100000, 150000] filas | `CRITICAL` | Todos | Detección de lotes incompletos o cortes abruptos en la descarga del CSV. |
| **DQ-RAW-03** | Spotify Raw | `popularity` | Validez | El puntaje de popularidad debe estar en la escala oficial de la API de Spotify | Entre 0 y 100 | `CRITICAL` | REQ-01, REQ-03 | La métrica clave de popularidad no admite valores negativos ni superiores a 100. |
| **DQ-RAW-04** | Spotify Raw | `explicit` | Conformidad | Indicador de contenido explícito debe ser booleano | En {True, False, 0, 1} | `CRITICAL` | Segmentación | Atributo categórico de segmentación analítica. |
| **DQ-RAW-05** | Spotify Raw | `track_name` | Completitud | Nombres de canciones no nulos tolerables | $\ge 99\%$ no nulo | `WARNING` | REQ-01, REQ-02 | El profiling detectó 1 nulo marginal en 114K filas. Se tolera bajo política documentada sin abortar el lote. |

### 2.2. Raw Quality Gate: Grammy Awards (`grammy_source.grammy_awards_raw`)

| Rule ID | Capa / Dataset | Atributo | Dimensión de Calidad | Regla de Calidad | Métrica / Umbral | Severidad | Requisito Relacionado | Justificación de Ingeniería |
|---|---|---|---|---|---|---|---|---|
| **DQ-RAW-06** | Grammy Raw | Tabla completa | Conformidad | La tabla origen debe exponer las 5 columnas maestras | Conteo = 5 | `CRITICAL` | REQ-01, REQ-03 | Contrato de esquema con la base de datos relacional origen (`grammy_source`). |
| **DQ-RAW-07** | Grammy Raw | Tabla completa | Completitud | Volumen total de registros históricos en rango | [4000, 6000] filas | `CRITICAL` | REQ-01, REQ-03 | Asegura que la réplica transaccional histórica esté completamente poblada. |
| **DQ-RAW-08** | Grammy Raw | `year` | Validez | Año de ceremonia debe pertenecer a la historia de la premiación | Entre 1958 y 2030 | `CRITICAL` | REQ-03 | La primera edición fue en 1958; fechas futuras o previas son errores de carga. |
| **DQ-RAW-09** | Grammy Raw | `category` | Completitud | Categoría de premiación obligatoria | 100% no nulo | `CRITICAL` | REQ-02 | Clave para mapear familias sonoras y géneros musicales. |
| **DQ-RAW-10** | Grammy Raw | `winner` | Validez | Indicador de premio obtenido válido | En {True, False, 0, 1} | `CRITICAL` | REQ-01, REQ-03 | Determina la métrica de premios ganados (`total_awards`). |
| **DQ-RAW-11** | Grammy Raw | `artist` | Completitud | Artista no nulo en premios directos | $\ge 60\%$ no nulo | `WARNING` | REQ-01, REQ-03 | El profiling reveló que el 38.2% de filas son premios técnicos de álbum sin artista individual. Tolerable por contrato. |

### 2.3. Prepared Quality Gate: Dataset Integrado (`music_prepared.parquet`)

| Rule ID | Capa / Dataset | Atributo | Dimensión de Calidad | Regla de Calidad | Métrica / Umbral | Severidad | Requisito Relacionado | Justificación de Ingeniería |
|---|---|---|---|---|---|---|---|---|
| **DQ-PREP-01**| Prepared Data | Tabla completa | Conformidad | Estructura preparada para carga dimensional | 24 columnas | `CRITICAL` | Todos | Invariante de esquema previo a la inserción en el Data Warehouse. |
| **DQ-PREP-02**| Prepared Data | Tabla completa | Completitud | Volumen de datos post-limpieza y deduplicación | [100000, 120000] filas | `CRITICAL` | Todos | Verifica retención esperada de 113,549 filas tras deduplicación de grano. |
| **DQ-PREP-03**| Prepared Data | `track_id` | Integridad | Identificador de canción no nulo | 100% no nulo | `CRITICAL` | Todos | Parte de la clave primaria compuesta del grano. |
| **DQ-PREP-04**| Prepared Data | `artist_id` | Integridad | Clave de artista resuelta y no nula | 100% no nulo | `CRITICAL` | Todos | Clave foránea mandatoria hacia `dim_artist`. |
| **DQ-PREP-05**| Prepared Data | `track_genre` | Integridad | Género musical asignado y no nulo | 100% no nulo | `CRITICAL` | REQ-01, REQ-02 | Clave foránea mandatoria hacia `dim_genre` y parte del grano. |
| **DQ-PREP-06**| Prepared Data | `popularity` | Validez | Popularidad final acotada | [0, 100] | `CRITICAL` | REQ-01, REQ-03 | Garantiza que ninguna transformación haya descalibrado las métricas de popularidad. |
| **DQ-PREP-07**| Prepared Data | `danceability` | Validez | Bailable normalizado | [0.0, 1.0] | `CRITICAL` | REQ-02 | Atributo de audio analizado para radar y comparaciones. |
| **DQ-PREP-08**| Prepared Data | `energy` | Validez | Energía sonora normalizada | [0.0, 1.0] | `CRITICAL` | REQ-02 | Atributo acústico para perfiles sonoros. |
| **DQ-PREP-09**| Prepared Data | `(track_id, track_genre)` | Unicidad | Unicidad del grano declarado (Track × Genre) | 0 duplicados | `CRITICAL` | Todos | Invariante estricto de cardinalidad del modelo dimensional. |
| **DQ-PREP-10**| Prepared Data | `has_grammy` | Conformidad | Indicador binario post-cruce | {0, 1} | `CRITICAL` | REQ-01, REQ-03 | Certifica que la integración asignó correctamente el flag de reconocimiento. |

---

## 3. Política Operativa de Severidad de Fallos (Failure Policy)

| Nivel de Severidad | Significado Operativo | Respuesta del Pipeline en Airflow | Justificación y Política Documentada |
|---|---|---|---|
| **CRITICAL** | La anomalía hace que continuar el procesamiento o la carga sea inseguro y contamine el DW. | Lanza excepción fatal (`ValueError` / `AirflowFailException`), detiene la tarea y **bloquea de forma absoluta todas las tareas posteriores**. | Principio de datos defensivo: es preferible detener el pipeline a cargar datos corruptos o fuera de norma. |
| **WARNING** | El problema requiere visibilidad pero no invalida la totalidad del lote analítico. | Registra advertencia detallada en logs y persiste la estadística en el archivo JSON de GX. El pipeline **continúa la ejecución**. | Aplica a nulos no esenciales (e.g. 1 nombre de pista faltante o filas de Grammy para álbumes técnicos que carecen de artista). |
| **INFORMATIONAL** | Información de contexto, tendencias de volumen o telemetría de monitoreo. | Se almacena en los metadatos de auditoría de la ejecución. | No afecta el flujo de tareas ni bloquea operaciones. |

---

## 4. Matriz de Trazabilidad de Extremo a Extremo (PDF §8.2)

Esta matriz demuestra la correspondencia estricta desde el requerimiento de negocio inicial hasta el elemento visual de toma de decisiones en Power BI:

| Requisito Analítico | Datos Requeridos | Riesgo de Calidad | Regla ID | Expectativa Great Expectations | Transformación Aplicada | Elemento del DW | KPI / Visualización en Power BI |
|---|---|---|---|---|---|---|---|
| **REQ-01** (Asociación Grammy vs Popularidad por Género y Volumen) | `artists`, `popularity`, `track_genre`, `grammy_nominations`, `grammy_wins` | Valores de popularidad fuera de norma ([0, 100]) sesgan la media y descalibran el *lift*. | **DQ-RAW-03** & **DQ-PREP-06** | `ExpectColumnValuesToBeBetween(column='popularity', min_value=0, max_value=100)` | Normalización de strings de artistas, cruce left-join defensivo, cálculo de niveles de nominación (0, 1-2, 3-5, 6-10, >10). | `fact_track_performance.popularity`, `dim_artist.total_nominations`, vista `v_kpi_r1_genre_popularity_lift` | Tarjetas KPI de *Popularity Lift* (+12.4 pts) y gráfico de barras por nivel de nominación. |
| **REQ-02** (Características Musicales Predominantes por Género) | `danceability`, `energy`, `acousticness`, `valence`, `track_genre`, `has_grammy` | Características acústicas nulas o con escalas no normalizadas invalidan comparaciones radar. | **DQ-PREP-07** & **DQ-PREP-08** | `ExpectColumnValuesToBeBetween(column='danceability', min_value=0.0, max_value=1.0)` | Casteo a punto flotante de alta precisión, preservación del grano Track × Genre (113,549 filas). | `fact_track_performance.[audio_cols]`, `dim_genre.genre_name`, vista `v_kpi_r2_audio_features_by_grammy_genre` | Gráfico de Radar / Barras agrupadas comparando las 9 dimensiones de audio por género. |
| **REQ-03** (Persistencia Histórica de Popularidad frente a Grupo Control) | `popularity`, `year`, `has_grammy`, `first_grammy_decade` | Años de ceremonia corruptos o nulos impiden categorizar las cohortes temporales por décadas. | **DQ-RAW-08** & **DQ-PREP-10** | `ExpectColumnValuesToBeBetween(column='year', min_value=1958, max_value=2030)` | Agrupación temporal histórica `first_grammy_decade = (min(year)//10)*10`, aplicación del umbral empírico $P_{75}=50.0$. | `dim_artist.first_grammy_decade`, vista `v_kpi_r3_historical_cohorts_performance` | Gráfico de columnas por década de reconocimiento, métrica $HPS_{P75}$ y ratio CPI ($> 1.8\times$). |
| **Integridad del Grano** (Invariante Estructural del Modelo) | `track_id`, `track_genre` | Duplicación accidental infla las métricas de streaming y rompe las claves primarias relacionales. | **DQ-PREP-09** | `ExpectCompoundColumnsToBeUnique(column_list=['track_id', 'track_genre'])` | Deduplicación técnica sobre clave compuesta reteniendo exactamente 113,549 observaciones analíticas. | Clave primaria compuesta en `fact_track_performance(track_id, genre_sk)` | Integridad referencial 1:N sin conteos inflados ni datos huérfanos. |

---

## 5. Contrato de Integración y Resolución de Entidades

| Componente | Definición de Contrato y Evidencia de Implementación |
|---|---|
| **Claves de Integración** | Artista normalizado: `primary_artist_key` derivado de Spotify $\leftrightarrow$ `artist_key` derivado de Grammy. |
| **Cardinalidad** | Muchas canciones : Un perfil histórico de artista ($N:1$). |
| **Tipo de Cruce** | `LEFT JOIN` defensivo donde Spotify es el dataset conductor y Grammy el enriquecedor. |
| **Preprocesamiento** | Conversión a minúsculas, remoción de espacios colapsados, eliminación de signos de puntuación y descarte de acentos. |
| **Registros no Coincidentes** | Canciones sin match en Grammy reciben de forma determinista: `has_grammy = 0`, `grammy_nominations = 0`, `grammy_wins = 0`, `first_grammy_decade = NULL`. |
| **Manejo de Ambigüedad** | Para canciones con múltiples artistas separados por punto y coma (`;`), se toma como titular al primer artista listado para mantener la trazabilidad. |
| **Limitaciones Documentadas** | El 38.2% de los registros de Grammy son premios a la producción o álbum sin artista solista/banda acreditada, excluyéndose legítimamente del cruce de artistas. |

---

## 6. Registro de Decisiones de Transformación (PDF §6.8)

| Regla y Justificación (Rule & Rationale) | Campos Afectados (Affected Fields) | Comportamiento Antes / Después (Before / After Behavior) | Manejo de Excepciones (Exception Handling) | Impacto Analítico (Analytical Impact) |
|---|---|---|---|---|
| **T-SP-01: Deduplicación por Grano**<br/>Preservar el grano dimensional analítico (Canción × Género) aprobado en Fase 0. | `track_id`, `track_genre` | **Antes:** 114,000 filas con 451 colisiones técnicas.<br/>**Después:** 113,549 filas exactas únicas por `(track_id, track_genre)`. | Se eliminan duplicados exactos conservando la primera observación válida. | Asegura que la clave primaria del DW `(track_id, genre_sk)` no falle por colisión de unicidad. |
| **T-SP-02: Extracción de Artista Principal**<br/>Permitir cruce determinista con la fuente de premiaciones. | `artists` $\rightarrow$ `primary_artist` | **Antes:** Strings compuestos (ej. `"Lady Gaga;Bradley Cooper"`).<br/>**Después:** Primer artista normalizado (`"lady gaga"`). | Si `artists` es nulo, se descarta el registro (1 fila detectada en profiling). | Habilita la integración relacional $N:1$ con la dimensión `dim_artist`. |
| **T-SP-03: Estandarización de Tipos de Audio**<br/>Garantizar compatibilidad numérica y rangos de agregación OLAP. | `danceability`, `energy`, `loudness`, `valence`, etc. | **Antes:** Tipos mixtos / texto potencial.<br/>**Después:** `float64` acotado a dominios válidos [0.0, 1.0]. | Valores no numéricos se coaccionan a `NaN` y se validan en el Gate Preparado. | Protege la integridad de las medidas en `fact_track_performance` para REQ-02. |
| **T-GR-01: Filtrado de Categorías Técnicas**<br/>Aislar premiaciones asociadas a artistas intérpretes. | `artist` | **Antes:** 4,810 registros con 1,840 nulos en `artist` (premios de álbum/ingeniería).<br/>**Después:** 2,970 registros con artista acreditado. | Filas sin artista se excluyen del matching pero se conservan en la auditoría origen. | Evita falsos positivos en el cálculo de nominaciones y premios individuales. |
| **T-GR-02: Derivación de Cohortes Históricas**<br/>Permitir segmentación temporal longitudinal para REQ-03. | `year` $\rightarrow$ `first_grammy_decade` | **Antes:** Años discretos de ceremonia (1958 a 2019).<br/>**Después:** Década del primer premio (ej. `'1960s'`, `'1980s'`). | Si no hay premio, el artista recibe `NULL` en década de consagración. | Fundamenta el análisis de persistencia histórica y ratio CPI en `v_kpi_r3_*`. |

---

## 7. Registro de Evidencias de Confiabilidad (Reliability Evidence Register - PDF §4.4)

| Evidence ID | Run / Task | Artifact or Path | What It Proves | Related Policy / Rule |
|---|---|---|---|---|
| **EV-01** | Profiling Task | [`notebooks/data_profiling.ipynb`](file:///c:/Users/mauri/OneDrive/Escritorio/etl/workshop_2/notebooks/data_profiling.ipynb) | Perfilamiento exhaustivo de 7 dimensiones y justificación empírica de $P_{75}=50.0$. | Requisitos REQ-01, REQ-02, REQ-03 |
| **EV-02** | Profiling Charts | [`docs/evidence/chart1_popularity_distribution.png`](file:///c:/Users/mauri/OneDrive/Escritorio/etl/workshop_2/docs/evidence/chart1_popularity_distribution.png) a `chart4_*.png` | Evidencia visual de distribuciones de popularidad, décadas de Grammy y perfiles de audio. | Soporte analítico general |
| **EV-03** | `validate_spotify_raw` | [`docs/evidence/spotify_raw_validation_result.json`](file:///c:/Users/mauri/OneDrive/Escritorio/etl/workshop_2/docs/evidence/spotify_raw_validation_result.json) | Aprobación de 5 expectativas crudas con tolerancia documentada de Warning en nulos. | DQ-RAW-01 a DQ-RAW-05 |
| **EV-04** | `validate_grammys_raw` | [`docs/evidence/grammy_raw_validation_result.json`](file:///c:/Users/mauri/OneDrive/Escritorio/etl/workshop_2/docs/evidence/grammy_raw_validation_result.json) | Aprobación de expectativas de esquema, rango temporal [1958, 2030] y completitud. | DQ-RAW-06 a DQ-RAW-11 |
| **EV-05** | `validate_prepared` | [`docs/evidence/prepared_validation_result.json`](file:///c:/Users/mauri/OneDrive/Escritorio/etl/workshop_2/docs/evidence/prepared_validation_result.json) | 100% de éxito en 10 expectativas: unicidad `(track_id, track_genre)` y claves foráneas no nulas. | DQ-PREP-01 a DQ-PREP-10 |
| **EV-06** | Test A (Happy Path) | [`docs/evidence/test_a_successful_run_evidence.json`](file:///c:/Users/mauri/OneDrive/Escritorio/etl/workshop_2/docs/evidence/test_a_successful_run_evidence.json) | Ejecución exitosa de extremo a extremo: 114K raw $\rightarrow$ 113,549 filas preparadas y cargadas en DW. | Política de Flujo Completo |
| **EV-07** | Test B (Inyección Falla) | [`scripts/create_bad_data.py`](file:///c:/Users/mauri/OneDrive/Escritorio/etl/workshop_2/scripts/create_bad_data.py) | Generación verificable de anomalía crítica (`popularity = 150.0`). | Regla DQ-RAW-03 |
| **EV-08** | Test B (Detección Falla) | [`docs/evidence/test_b_failed_raw_validation.json`](file:///c:/Users/mauri/OneDrive/Escritorio/etl/workshop_2/docs/evidence/test_b_failed_raw_validation.json) | Great Expectations detecta violación fuera de rango y asigna severidad `CRITICAL`. | Política de Severidad Crítica |
| **EV-09** | Test B (Bloqueo Airflow) | [`docs/evidence/test_b_controlled_failure_evidence.json`](file:///c:/Users/mauri/OneDrive/Escritorio/etl/workshop_2/docs/evidence/test_b_controlled_failure_evidence.json) | `validate_spotify_raw` falla con excepción crítica; transformación y carga quedan 100% bloqueadas. | Principio de Pipeline Defensivo |
| **EV-10** | Test C (Safe Rerun) | [`docs/evidence/test_c_safe_rerun_evidence.json`](file:///c:/Users/mauri/OneDrive/Escritorio/etl/workshop_2/docs/evidence/test_c_safe_rerun_evidence.json) | Reejecución produce exactamente 113,549 filas sin duplicación técnica ni registros corruptos. | Principio de Idempotencia |
| **EV-11** | Source Reconciliation | [`docs/evidence/grammy_source_reconciliation.json`](file:///c:/Users/mauri/OneDrive/Escritorio/etl/workshop_2/docs/evidence/grammy_source_reconciliation.json) | Reconciliación 4,810 filas leídas de CSV $\leftrightarrow$ 4,810 filas cargadas en `grammy_source`. | Requisito PDF §4.3 & §6.2 |
| **EV-12** | Load Verification | [`docs/evidence/load_summary.json`](file:///c:/Users/mauri/OneDrive/Escritorio/etl/workshop_2/docs/evidence/load_summary.json) | Carga atómica confirmada: `dim_artist` (17,648), `dim_genre` (114), `fact` (113,549). | Requisito PDF §6.10 & §7.1 |
