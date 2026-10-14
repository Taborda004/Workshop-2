# Arquitectura del Pipeline ETL y Data Warehouse (Workshop 2)

Este documento describe en detalle la arquitectura técnica, metodológica y conceptual del pipeline ETL confiable implementado para el **Workshop 2: Spotify & Grammy Awards Data Warehouse**.

---

## 1. Visión General de la Solución

El pipeline está diseñado bajo los principios de **ingeniería de datos defensiva, resiliencia y confiabilidad** (`reliable_pipeline`), garantizando:
1. **Validación rigurosa de calidad de datos en dos compuertas (Gates)** con Great Expectations 1.x:
   - *Raw Quality Gate*: Validación de esquema, tipos y rangos de las fuentes crudas antes de cualquier transformación.
   - *Prepared Quality Gate*: Validación del dataset integrado antes de proceder a la carga al DW.
2. **Clasificación y manejo de anomalías por severidad**:
   - `CRITICAL`: Aborta la ejecución de forma inmediata para proteger la integridad del Data Warehouse (e.g. popularidad fuera del rango 0-100 o violación de unicidad de clave compuesta).
   - `WARNING`: Registra la advertencia en el log de auditoría y persiste métricas de diagnóstico sin detener el pipeline (e.g. valores nulos tolerables en nombres de canciones).
3. **Persistencia intermedia en formato Parquet**: Desacoplamiento entre fases de extracción, transformación y carga, evitando transferencias masivas de datos por memoria o XCom.
4. **Carga transaccional e idempotente**: Uso de transacciones ACID (`DELETE FROM` dentro de bloques atómicos `engine.begin()`) en MySQL 8.0, asegurando que ante cualquier fallo se produzca un `ROLLBACK` total y que reejecuciones del pipeline no generen duplicados.

---

## 2. Diagrama de Arquitectura de Datos y Componentes

```mermaid
flowchart TD
    subgraph Fuentes ["1. Fuentes de Datos Crudas"]
        S1["Spotify Dataset CSV<br/>(114,000 registros)"]
        S2["The Grammy Awards CSV<br/>(4,810 registros)"]
    end

    subgraph Extraccion ["2. Extracción y Persistencia Cruda"]
        E1["extract_spotify_data()"]
        E2["extract_grammy_data()"]
        P1["data/work/raw_spotify.parquet"]
        P2["data/work/raw_grammy.parquet"]
    end

    subgraph Gate1 ["3. Raw Quality Gate (Great Expectations)"]
        GX1["validate_spotify_raw()<br/>(DQ-RAW-01 a DQ-RAW-05)"]
        GX2["validate_grammy_raw()<br/>(DQ-RAW-06 a DQ-RAW-08)"]
        EV1["docs/evidence/spotify_raw_validation_result.json"]
        EV2["docs/evidence/grammy_raw_validation_result.json"]
    end

    subgraph Transformacion ["4. Transformación e Integración"]
        T1["transform_and_integrate()"]
        T_Sub1["Deduplicación por grano (track_id, track_genre)<br/>(113,549 registros)"]
        T_Sub2["Normalización y Matching de Artistas"]
        T_Sub3["Cálculo de Cohortes Históricas (first_grammy_decade)"]
        P3["data/work/prepared_tracks.parquet"]
    end

    subgraph Gate2 ["5. Prepared Quality Gate"]
        GX3["validate_prepared_dataset()<br/>(DQ-PREP-01 a DQ-PREP-10)"]
        EV3["docs/evidence/prepared_validation_result.json"]
    end

    subgraph Carga ["6. Carga Transaccional Idempotente"]
        L1["load_data_warehouse()"]
        TX["Transacción Atómica (engine.begin())<br/>DELETE FROM -> INSERT"]
    end

    subgraph DW ["7. Data Warehouse (MySQL 8.0)"]
        D1[("dim_artist<br/>(SK, has_grammy, nominations, decade)")]
        D2[("dim_genre<br/>(SK, genre_name)")]
        D3[("dim_date<br/>(SK, year, decade)")]
        F1[("fact_track_performance<br/>(Grano: Track x Genre)")]
        V1["Vistas Analíticas de Negocio:<br/>v_kpi_r1_*, v_kpi_r2_*, v_kpi_r3_*"]
    end

    subgraph Consumo ["8. Capa Semántica y Visualización"]
        PBI["Power BI Dashboard<br/>(DirectQuery / Import)"]
    end

    S1 --> E1 --> P1 --> GX1 --> EV1
    S2 --> E2 --> P2 --> GX2 --> EV2
    P1 --> T1
    P2 --> T1
    T1 --> T_Sub1 --> T_Sub2 --> T_Sub3 --> P3
    P3 --> GX3 --> EV3
    GX1 -. "CRITICAL: Stop" .-> X1["Aborta Pipeline"]
    GX2 -. "CRITICAL: Stop" .-> X1
    GX3 -. "CRITICAL: Stop" .-> X2["Aborta Pipeline"]
    P3 --> L1 --> TX --> D1 & D2 & D3 & F1 --> V1 --> PBI
```

---

## 3. Topología de Apache Airflow (DAG)

El DAG `spotify_grammy_reliable_etl` implementa una topología lineal con ramificaciones de validación y un diseño modular:

```mermaid
graph LR
    subgraph Airflow DAG ["DAG: spotify_grammy_reliable_etl"]
        start((Start)) --> t_extract_spotify["extract_spotify_task"]
        start --> t_extract_grammy["extract_grammy_task"]
        
        t_extract_spotify --> t_val_raw_spotify["validate_raw_spotify_task"]
        t_extract_grammy --> t_val_raw_grammy["validate_raw_grammy_task"]
        
        t_val_raw_spotify --> t_transform["transform_integrate_task"]
        t_val_raw_grammy --> t_transform
        
        t_transform --> t_val_prep["validate_prepared_task"]
        t_val_prep --> t_load["load_dw_task"]
        t_load --> finish((End))
    end
```

### Configuración del DAG
- **Schedule**: `@daily` (o `@once` para carga histórica).
- **Catchup**: `False` (evita detonaciones retroactivas accidentales).
- **Default Args**:
  - `retries`: 1
  - `retry_delay`: `timedelta(minutes=5)`
  - `email_on_failure`: `False`
- **Gestión de Fallos**:
  - Si `validate_raw_spotify_task` o `validate_raw_grammy_task` detectan una anomalía crítica, lanzan `AirflowFailException`, impidiendo la ejecución de `transform_integrate_task`.
  - Si `validate_prepared_task` detecta anomalía crítica, lanza `AirflowFailException`, protegiendo la tarea `load_dw_task`.

---

## 4. Grano del Modelo Dimensional y Estrategia de Carga

### Definición del Grano
El grano de la tabla de hechos `fact_track_performance` es:
> **Un registro por cada combinación única de Canción (Track) y Género Musical.**

- En el dataset crudo de Spotify existen canciones categorizadas simultáneamente en más de un género (e.g. *acoustic*, *folk*, *pop*).
- La deduplicación estricta por `(track_id, track_genre)` elimina 451 duplicados técnicos exactos, preservando exactamente **113,549 observaciones analíticas**.
- La clave primaria de la tabla de hechos es la clave compuesta:
  $$\text{PK} = (\text{track\_id}, \text{genre\_sk})$$

### Transaccionalidad e Idempotencia en MySQL
En motores relacionales como MySQL, `TRUNCATE TABLE` ejecuta un commit implícito DDL, impidiendo el rollback dentro de una transacción activa. Para garantizar transaccionalidad ACID genuina:
1. Se emplea `DELETE FROM` dentro de un contexto `engine.begin()`.
2. Las claves foráneas se desactivan y reactivan controladamente (`SET FOREIGN_KEY_CHECKS = 0; ... SET FOREIGN_KEY_CHECKS = 1;`).
3. Se cargan las dimensiones (`dim_artist`, `dim_genre`, `dim_date`) con claves sustitutas (Surrogate Keys).
4. Se carga la tabla de hechos con mapeo íntegro de dimensiones.
5. Si ocurre cualquier error durante la carga de hechos, SQLAlchemy revierte todas las operaciones anteriores mediante `ROLLBACK`.

---

## 5. Entorno de Ejecución Docker

El entorno se encuentra orquestado en `docker-compose.yaml` mediante 5 servicios intercomunicados:
1. `mysql-dw`: Contenedor MySQL 8.0 dedicado al Data Warehouse analítico (puerto expuesto: 3307).
2. `postgres`: Base de datos PostgreSQL para los metadatos internos de Apache Airflow.
3. `airflow-webserver`: Interfaz web de usuario y monitor de DAGs (puerto 8080).
4. `airflow-scheduler`: Planificador de tareas de Airflow.
5. `airflow-init`: Contenedor de inicialización de migraciones y usuario administrador de Airflow.

Todos los contenedores comparten los volúmenes `./data:/opt/airflow/data`, `./dags:/opt/airflow/dags`, `./src:/opt/airflow/src`, `./sql:/opt/airflow/sql`, y `./docs:/opt/airflow/docs`.
