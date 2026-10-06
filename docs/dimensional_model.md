# Modelo dimensional

![Esquema estrella de music_dw](star_schema.png)

## 1. Proceso de negocio

Relacionar el reconocimiento de la industria musical (nominaciones a los premios Grammy) con el desempeno y el perfil de audio de las canciones en Spotify, a nivel de artista.

## 2. Grano de las tablas de hechos

| Tabla de hechos | Una fila representa | Llave primaria |
|---|---|---|
| `fact_nomination` | La participacion de un artista en una nominacion Grammy. Una nominacion con varios artistas genera varias filas. | (`nomination_id`, `artist_key`) |
| `fact_track_audio` | Una cancion unica de Spotify (`track_id`). Si aparece en varios generos se cuenta una sola vez. | `track_key` |

Decisiones asociadas al grano:

- **Popularidad repetida:** si un mismo `track_id` trae valores distintos de `popularity` entre generos (720 canciones), se toma el maximo.
- **Misma cancion con distinto `track_id`:** se tratan como canciones distintas (4657 casos). Es una limitacion declarada.
- **Nominaciones sin artista recuperable (442):** se cargan con el artista marcador "Desconocido" para que los conteos por categoria queden completos.

Las dos tablas de hechos comparten la dimension `dim_artist`, que es donde se integran Spotify y Grammy.

## 3. Dimensiones

| Dimension | Llave sustituta (PK) | Llave de negocio | Otros atributos |
|---|---|---|---|
| `dim_artist` | `artist_key` | `artist_norm` (nombre normalizado, UNIQUE) | `artist_name`, `in_spotify`, `in_grammy`, `is_placeholder` |
| `dim_track` | `track_key` | `track_id` de Spotify (UNIQUE) | `explicit` |
| `dim_genre` | `genre_key` | `genre_name` (UNIQUE) | - |
| `dim_category` | `category_key` | `category_name` (UNIQUE) | - |
| `dim_year` | `year_key` (el propio anio) | `year_key` | `decade` |

`dim_artist` es la dimension conformada: la comparten las dos estrellas y es donde se cruzan Spotify y Grammy. Las columnas `in_spotify` e `in_grammy` miden cuantos artistas aparecen en cada fuente, y `is_placeholder` marca al artista "Desconocido", que no es una persona real.

## 4. Medidas

| Tabla de hechos | Medida | Descripcion | Requerimiento |
|---|---|---|---|
| `fact_nomination` | `nomination_count` | Siempre 1; se suma para contar nominaciones | R1, R3 |
| `fact_track_audio` | `popularity` | Popularidad en Spotify, de 0 a 100 | R1 |
| `fact_track_audio` | `danceability`, `energy`, `valence`, `acousticness` | Caracteristicas de audio, de 0 a 1 | R2 |

Atributos degenerados de `fact_nomination` (viven en la tabla de hechos, sin dimension propia): `nomination_id`, `nominee`, `winner` y `artist_source`. En los datos `winner` es siempre verdadero, asi que no distingue ganadores de nominados.

## 5. Llaves y relaciones

- **Llaves sustitutas:** numeros generados por la base (`GENERATED ALWAYS AS IDENTITY`) en las dimensiones, excepto `dim_year`.
- **Llaves de negocio:** restriccion UNIQUE en `artist_norm`, `track_id`, `genre_name` y `category_name`, para evitar duplicados en la carga.
- **`nomination_id`:** codigo determinista calculado a partir de (anio, categoria, nominado, artista crudo). Junto con `artist_key` forma la llave primaria de `fact_nomination`, lo que permite recargar sin duplicar.

| Tabla | Llave foranea | Referencia |
|---|---|---|
| `fact_nomination` | `artist_key` | `dim_artist` |
| `fact_nomination` | `category_key` | `dim_category` |
| `fact_nomination` | `year_key` | `dim_year` |
| `fact_track_audio` | `track_key` | `dim_track` |
| `bridge_track_artist` | `track_key`, `artist_key` | `dim_track`, `dim_artist` |
| `bridge_track_genre` | `track_key`, `genre_key` | `dim_track`, `dim_genre` |

Las tablas puente resuelven la relacion de muchos a muchos: una cancion tiene varios artistas y varios generos.

Restricciones de dominio en la base (segunda barrera despues de Great Expectations): `popularity` entre 0 y 100, medidas de audio entre 0 y 1, `nomination_count` igual a 1 y `artist_source` limitado a cuatro valores.
