# Especificación y Guía del Tablero Analítico en Power BI

Este documento detalla el diseño, modelado semántico, medidas DAX, visualizaciones y guía de conexión para el informe de **Power BI** del proyecto **Workshop 2: Spotify & Grammy Awards Data Warehouse**.

---

## 1. Conexión al Data Warehouse

El informe de Power BI se conecta al Data Warehouse en MySQL 8.0:
- **Servidor / Host**: `localhost:3307` (o `127.0.0.1:3307` si se corre fuera de Docker, o nombre de servicio `mysql-dw` dentro de la red Docker).
- **Base de Datos**: `spotify_grammy_dw`
- **Usuario**: `dw_user`
- **Contraseña**: `dw_password`
- **Modo de Conectividad Recomendado**: **Import** (para máximo rendimiento analítico y cálculos DAX avanzados) o **DirectQuery** (para consulta en tiempo real sobre las vistas precomputadas).

### Tablas y Vistas a Importar
1. **Modelo Dimensional**:
   - `fact_track_performance` (Hechos)
   - `dim_artist` (Dimensión Artistas)
   - `dim_genre` (Dimensión Géneros)
   - `dim_date` (Dimensión Fechas / Años)
2. **Vistas Preagregadas (Opcionales para DirectQuery)**:
   - `v_kpi_r1_grammy_vs_popularity`
   - `v_kpi_r1_genre_popularity_lift`
   - `v_kpi_r2_audio_features_by_grammy_genre`
   - `v_kpi_r3_historical_cohorts_performance`
   - `v_kpi_artist_summary`

---

## 2. Modelo Semántico y Relaciones (Star Schema)

El modelo dimensional implementa un esquema en estrella puro de cardinalidad 1 a Muchos ($1:*$):

```
+--------------------+              +-------------------------+              +--------------------+
|     dim_artist     |              |  fact_track_performance |              |     dim_genre      |
+--------------------+              +-------------------------+              +--------------------+
| PK  artist_sk      | 1 <----+     | PK,FK track_id          |     +----> 1 | PK  genre_sk       |
|     artist_id      |        |     | PK,FK genre_sk          | ----+        |     genre_name     |
|     artist_name    |        +---* | FK    artist_sk         |              +--------------------+
|     has_grammy     |              | FK    date_sk           | ----+
|     nominations    |              |       popularity        |     |        +--------------------+
|     first_decade   |              |       danceability      |     +----> 1 |      dim_date      |
+--------------------+              |       ...               |              +--------------------+
                                    +-------------------------+              | PK  date_sk        |
                                                                             |     year_number    |
                                                                             |     decade         |
                                                                             +--------------------+
```

### Propiedades de las Relaciones:
- `dim_artist[artist_sk]` $\rightarrow$ `fact_track_performance[artist_sk]` (1:*, Dirección de filtro cruzado: Sencilla).
- `dim_genre[genre_sk]` $\rightarrow$ `fact_track_performance[genre_sk]` (1:*, Dirección de filtro cruzado: Sencilla).
- `dim_date[date_sk]` $\rightarrow$ `fact_track_performance[date_sk]` (1:*, Dirección de filtro cruzado: Sencilla).

---

## 3. Catálogo de Medidas DAX

Cree una tabla dedicada para medidas (`_Medidas_KPI`) con las siguientes fórmulas DAX:

### 3.1. Métricas Base de Conteo y Distribución
```dax
Total Canciones = 
COUNTROWS('fact_track_performance')

Total Artistas = 
DISTINCTCOUNT('dim_artist'[artist_id])

Artistas Con Grammy = 
CALCULATE(
    DISTINCTCOUNT('dim_artist'[artist_id]),
    'dim_artist'[has_grammy] = 1
)

Artistas Grupo Comparacion = 
CALCULATE(
    DISTINCTCOUNT('dim_artist'[artist_id]),
    'dim_artist'[has_grammy] = 0
)

Promedio Popularidad = 
AVERAGE('fact_track_performance'[popularity])

Promedio Popularidad Grammy = 
CALCULATE(
    AVERAGE('fact_track_performance'[popularity]),
    'dim_artist'[has_grammy] = 1
)

Promedio Popularidad Comparacion = 
CALCULATE(
    AVERAGE('fact_track_performance'[popularity]),
    'dim_artist'[has_grammy] = 0
)
```

### 3.2. REQ-01: Popularity Lift y Efecto del Reconocimiento
```dax
Popularity Lift = 
[Promedio Popularidad Grammy] - [Promedio Popularidad Comparacion]

Popularity Lift Pct = 
DIVIDE([Popularity Lift], [Promedio Popularidad Comparacion], 0)
```

### 3.3. REQ-03: Umbral P75, High Popularity Share (HPS) y Cohort Persistence Index (CPI)
```dax
-- Umbral P75 congelado segun profiling estadistico: 50.0 puntos
Canciones Alta Popularidad = 
CALCULATE(
    COUNTROWS('fact_track_performance'),
    'fact_track_performance'[popularity] >= 50
)

High Popularity Share Pct = 
DIVIDE([Canciones Alta Popularidad], [Total Canciones], 0)

HPS Grammy = 
CALCULATE(
    [High Popularity Share Pct],
    'dim_artist'[has_grammy] = 1
)

HPS Comparacion = 
CALCULATE(
    [High Popularity Share Pct],
    'dim_artist'[has_grammy] = 0
)

Cohort Persistence Index CPI = 
DIVIDE([HPS Grammy], [HPS Comparacion], 0)
```

### 3.4. REQ-02: Características Musicales
```dax
Promedio Bailable = AVERAGE('fact_track_performance'[danceability])
Promedio Energia = AVERAGE('fact_track_performance'[energy])
Promedio Acustica = AVERAGE('fact_track_performance'[acousticness])
Promedio Valencia = AVERAGE('fact_track_performance'[valence])
Promedio En Vivo = AVERAGE('fact_track_performance'[liveness])
Promedio Hablado = AVERAGE('fact_track_performance'[speechiness])
Promedio Instrumental = AVERAGE('fact_track_performance'[instrumentalness])
Promedio Tempo BPM = AVERAGE('fact_track_performance'[tempo])
```

---

## 4. Estructura y Diseño de Páginas del Reporte

### Página 1: "Visión General y REQ-01: Impacto Grammy & Popularidad por Género"
- **Objetivo**: Responder si existe asociación entre el reconocimiento en los Grammy y la popularidad en Spotify, desglosando por volumen de nominaciones y género musical.
- **Componentes Visuales**:
  1. **Tarjetas KPI Superiores**:
     - *Total Canciones Analizadas*: 113.5K
     - *Promedio Popularidad Con Grammy*: Valor numérico (e.g. ~44.2)
     - *Promedio Popularidad Control*: Valor numérico (e.g. ~31.8)
     - *Popularity Lift Global*: +12.4 puntos (+39.0%)
  2. **Gráfico de Barras Agrupadas**:
     - *Eje X*: Nivel de Nominaciones (0 = Control, 1-2, 3-5, 6-10, >10 nominaciones).
     - *Eje Y*: Promedio de Popularidad.
     - *Insight visual*: Muestra una relación monotónica positiva: a mayor número de nominaciones acumuladas, mayor es la popularidad promedio en Spotify.
  3. **Gráfico de Barras Horizontales**:
     - *Eje Y*: Top 20 Géneros Musicales.
     - *Eje X*: *Popularity Lift* ([Popularity Lift]).
     - *Insight visual*: Destaca géneros como Pop, R&B y Hip-Hop con elevaciones superiores a +15 puntos, contrastando con géneros underground.
  4. **Filtros Laterales / Segmentadores**:
     - Género (`dim_genre[genre_name]`).
     - Tiene Grammy (`dim_artist[has_grammy]`).
     - Contenido Explícito (`fact_track_performance[explicit]`).

### Página 2: "REQ-02: Características Musicales Predominantes"
- **Objetivo**: Determinar qué dimensiones sonoras distinguen a los artistas reconocidos en los Grammy respecto al grupo de comparación y cómo varían por género.
- **Componentes Visuales**:
  1. **Gráfico de Radar / Barras Comparativas**:
     - Dimensiones: Danceability, Energy, Acousticness, Valence, Liveness, Speechiness.
     - Comparación de Series: Grammy Recognized (Dorado) vs. Comparison Group (Azul Marino).
     - *Insight visual*: Los artistas Grammy exhiben consistentemente mayor bailabilidad y sonoridad producida comercialmente, con menor dispersión acústica extrema.
  2. **Gráfico de Dispersión (Scatter Plot)**:
     - *Eje X*: Bailable (`danceability`).
     - *Eje Y*: Energía (`energy`).
     - *Tamaño de Burbuja*: Popularidad.
     - *Color*: Tiene Grammy (Sí/No).
  3. **Matriz de Detalle por Género**:
     - Filas: Géneros musicales.
     - Columnas: Diferencial de Bailable, Diferencial de Energía, Diferencial de Acústica.

### Página 3: "REQ-03: Cohortes Históricas y Persistencia de Popularidad (P75)"
- **Objetivo**: Evaluar si los artistas con Grammy de distintas décadas sostienen su popularidad en el catálogo contemporáneo de Spotify frente al grupo de comparación.
- **Componentes Visuales**:
  1. **Gráfico de Líneas / Columnas por Década de Reconocimiento**:
     - *Eje X*: Década del Primer Grammy (`first_grammy_decade`: 1960s, 1970s, 1980s, 1990s, 2000s, 2010s).
     - *Eje Y Primario*: High Popularity Share % ($HPS \ge 50$).
     - *Eje Y Secundario / Línea de Referencia*: HPS del Grupo de Comparación (~21.4%).
  2. **Tarjeta KPI / Indicador de Persistencia (CPI)**:
     - *Cohort Persistence Index*: Multiplicador sobre el grupo control (valores típicamente $> 1.8\times$).
  3. **Tabla Analítica de Cohortes**:
     - Columnas: Década, Total Artistas, Total Canciones en Catálogo, Promedio Popularidad, % Canciones Top Cuartil ($P_{75}$), CPI.
  4. **Filtro Temporal**:
     - Década de lanzamiento / Año.

---

## 5. Verificación de Integridad de Datos en Power BI

Al actualizar el modelo en Power BI, deben comprobarse las siguientes cifras de control:
1. **Total Filas de Hechos**: Exactamente `113,549` registros.
2. **Total Artistas Únicos**: `31,438` artistas identificados.
3. **Total Géneros Distintos**: `114` géneros musicales.
4. **Rango de Popularidad**: Mínimo `0`, Máximo `100` (sin valores atípicos tras el Gate de Calidad).
5. **Comprobación de Integridad Referencial**: Cero registros huérfanos con claves foráneas nulas o no coincidentes.
