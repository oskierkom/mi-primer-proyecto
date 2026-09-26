# Identificador de alumno
myrna

# Observaciones
## CSV/JSON
Los CSV no tienen tipos, todos figuran como string. Aún cuando se aplica `inferSchema=true`, la columna `amount` aparece como string. Esto se debe a que la columna tiene filas con valor 'N/A' y Spark elige el tipo que sea válido para todos los casos.

El JSON sí tiene distintos tipos de datos (cuestomer_id y event_id son de tipo long). Además, los archivos JSON permiten tener estructuras anidadas para preservar jerarquías (context: platform y session_id).

## Parquet
Los archivos parquet contienen el esquema emebebido, los tipos de datos se extraen de los metadatos del archivo.

## Delta
Agrega una capa de abstracción con metaddatos transaccionales para relacionar lo operacional con lo analítico.


# 5Vs de Big Data
1. Volumen: se registran 200.000 eventos, 5.000m clientes y 500 productos. Si bien no es una cantidad masiva de datos y podrían procesarse con capacidades de cómputo estándar, exige evaluar partición de datos y formatos columnares en vez de CSVs planos.

2. Velocidad: los campos de timestamp (ej: event_ts) sugieren que, en un escenario real, los datos llegan de forma continua (streaming) en vez de en batch.

3. Variedad: los orígenes son archivos distintos (CSV, JSON, Parquet y formato transaccional vía Delta). Cada uno posee estructura propia.

4. Veracidad: la celda de diagnóstico de calidad detecta que, de 50.011 transacciones, 11 están duplicadas (mismo transaction_id). Además, la columna `amount` tiene 52 valores "N/A" que no pueden convertirse a numéricos.

5. Valor: el valor de los datos se ve en la capa Gold. Bronze implica solo la carga inicial de los datos, en Silver se limpian y se cruzan según corresponda, para finalmente aplicar métricas en Gold. El valor en Bronze está en la trazabilidad de algunos datos (por ejemplo: con los campos _source, _ingested_at, etc.).

# Desafío

Detectar la evolución de un esquema es una operación de diagnóstico necesaria. Se compara la estreuctura y valores existentes con los nuevos, permitiendo la incorporación de nuevos campos y valores. Es una etapa descriptiva, no implica criterios de decisión.
Aceptar la evolución garantiza que la integración permita incorporar los cambios entrantes sin comprometer la integridad de los datos. En este caso, el uso de `unionByName` y de `allowMissingColumns=true` admite la ausencia de `app_version` en registros históricos; `dropDuplicates` sobre `event_id` preserva la unicidad de los identificadores.
Por último, la validez en el negocio implica determinar si, por ejemplo, el evento refund es un comportamiento legítimo de la aplicación o, por el contrario, es una anomalía en el origen entrante. Esto excede el alcance técnico y requiere del criterio de usuarios de negocio, quienes tienen conocimiento del dominio.