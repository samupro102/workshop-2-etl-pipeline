# Riesgos de calidad y reglas de validación

## 1. Riesgos de calidad (evidencia del perfilado)

Evidencia reproducible en `notebooks/data_profiling.ipynb` y `docs/evidence/`.

| ID | Dataset / atributo | Evidencia | Riesgo | Req. |
|---|---|---|---|---|
| RK01 | Spotify / `track_id` | 89 741 ids en 114 000 filas; 40 900 filas con id repetido | Contar una canción varias veces (una por género) | R1, R2 |
| RK02 | Spotify / `(track_id, track_genre)` | 450 filas duplicadas exactas | Doble conteo | R1-R3 |
| RK03 | Spotify / `(artists, track_name)` | 4657 pares con más de un `track_id` | La misma canción con distinto id no se detecta | R1, R2 |
| RK04 | Spotify / `popularity` | 16 020 filas con valor 0 (14.05 %) | Ceros que pueden ser ausencia de dato y sesgan el promedio | R1 |
| RK05 | Spotify / `time_signature`, `tempo` | 1136 filas con compás 0 o 1; 157 con tempo 0 | Valores fuera de dominio | R2 |
| RK06 | Spotify / `duration_ms` | 17 pistas de menos de 30 s; 81 de más de 20 min | Atípicos | R2 |
| RK07 | Spotify / `artists` | 30 075 filas (26.4 %) con varios artistas separados por `;` | Un artista por fila no coincide con Grammy | R1-R3 |
| RK08 | Grammy / `artist` | 1840 nulos (38.25 %); 186 con `workers` también nulo | Nominaciones sin clave de integración | R1, R3 |
| RK09 | Grammy / `winner` | `True` en las 4810 filas | No se distinguen ganadores de nominados | R1-R3 |
| RK10 | Grammy / `artist` | Separadores `&`, `,`, `Featuring`, `and` | Dividir mal rompe nombres de bandas | R1-R3 |
| RK11 | Cruce de fuentes | 32.81 % de artistas por nombre completo; 51.39 % por nombre o partes | Cobertura limitada y falsos "no nominados" | R1-R3 |
| RK12 | Spotify / muestra | Exactamente 1000 filas por género | Los conteos de canciones por género no representan el mercado | R3 |

## 2. Reglas de calidad

| ID | Capa | Atributo | Dimensión de calidad | Regla | Umbral | Severidad | Riesgo | Req. |
|---|---|---|---|---|---|---|---|---|
| DQ01 | Raw Spotify | columnas requeridas | Completitud | Existen `track_id`, `artists`, `track_genre`, `popularity` y los campos de audio | 100 % | Critical | contrato de fuente | R1-R3 |
| DQ02 | Raw Spotify | `popularity` | Validez | Valores entre 0 y 100 | 100 % | Critical | RK04 | R1 |
| DQ03 | Raw Spotify | `danceability`, `energy`, `valence`, `acousticness` | Validez | Valores entre 0 y 1 | 100 % | Critical | RK05 | R2 |
| DQ04 | Raw Spotify | `(track_id, track_genre)` | Unicidad | Filas únicas | >= 99 % | Warning | RK02 | R1-R3 |
| DQ05 | Raw Spotify | `popularity` | Validez | Proporción de ceros | solo se registra | Informational | RK04 | R1 |
| DQ06 | Raw Grammy | columnas requeridas | Completitud | Existen `year`, `category`, `nominee`, `artist`, `workers` | 100 % | Critical | contrato de fuente | R1, R3 |
| DQ07 | Raw Grammy | `category` | Completitud | No nula | 100 % | Critical | RK09 | R3 |
| DQ08 | Raw Grammy | `artist`, `workers` | Completitud | Al menos uno presente | >= 95 % | Warning | RK08 | R1, R3 |
| DQ09 | Raw Grammy | `winner` | Consistencia | Contiene `True` y `False` | solo se registra | Informational | RK09 | R1-R3 |
| DQ10 | Prepared | clave de artista | Completitud | No nula en las nominaciones con artista asignado | 100 % | Critical | RK08, RK10 | R1, R3 |
| DQ11 | Prepared | dimensión de artista | Unicidad | Clave normalizada única | 100 % | Critical | RK07, RK10 | R1-R3 |
| DQ12 | Prepared | hechos de nominación | Consistencia | Nominaciones distintas en hechos = filas crudas de Grammy | 100 % | Critical | RK08 | R3 |

## 3. Justificación de umbrales

- **Critical al 100 %:** un valor fuera de rango, una columna faltante o una clave nula distorsiona directamente R1, R2 o R3, por eso se bloquea.
- **DQ04 (99 %, Warning):** los duplicados exactos son redundantes y la transformación los elimina; se toleran hasta 1 %. Más que eso sugiere un problema de origen.
- **DQ08 (95 %, Warning):** por debajo de ese nivel el cruce por artista perdería demasiadas nominaciones; el pipeline puede continuar documentando las filas excluidas.
- **DQ05 y DQ09 (Informational):** son limitaciones conocidas que no deben bloquear, pero deben quedar registradas en cada ejecución. DQ09 falla a propósito en cada lote para dejar constancia.
