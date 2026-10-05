# Clase práctica 2 — Silver, Gold y orquestación

## Propósito

Transformar las tablas Bronze de la práctica 1 en datos confiables y productos analíticos. El flujo se ejecuta como un Lakeflow Job, admite archivos nuevos y demuestra idempotencia mediante validaciones automáticas.

## Objetivos

- Aplicar contratos de tipos y reglas de calidad.
- Separar registros válidos de una cuarentena explicable.
- Resolver duplicados y correcciones con `MERGE`.
- Construir tablas Gold con un grano de negocio explícito.
- Orquestar notebooks dependientes con Lakeflow Jobs.
- Probar una carga nueva sin cambiar el código del pipeline.
- Verificar reconciliación e idempotencia.

## Requisitos previos

La práctica 1 debe haber creado, en el mismo esquema:

```text
bronze_customers
bronze_products
bronze_transactions
bronze_events
```

Usá en todos los notebooks el mismo `student_id` y la misma escala de la práctica 1. Si trabajaste con `small`, no cambies a `test`: las claves foráneas se generan de acuerdo con la escala.

## Secuencia

1. Ejecutá `00_preflight.ipynb`.
2. Ejecutá manualmente `00_generate_new_batch.ipynb` con `batch_002`.
3. Creá el Job siguiendo `GUIA_CREAR_JOB.md`.
4. Ejecutá el Job con `expected_batch_id=batch_002`.
5. Revisá las cuatro tareas y las tablas resultantes.
6. Ejecutá nuevamente el mismo Job sin crear otro archivo.
7. Confirmá que la segunda validación informa métricas estables.
8. Volvé a ejecutar manualmente `00_generate_new_batch.ipynb`, esta vez con `batch_id=batch_003`.
9. Ejecutá el Job con `expected_batch_id=batch_003` y verificá que el nuevo lote aparezca en `gold_batch_summary`.
10. Ejecutá manualmente `05_visualizacion.ipynb` y resolvé las cuatro preguntas usando las tablas Gold.

### Qué hace `expected_batch_id`

El pipeline **no necesita** `expected_batch_id` para encontrar ni procesar archivos. La tarea de ingesta usa `COPY INTO` sobre el directorio de entrada, detecta automáticamente `batch_003` como un archivo nuevo y lo incorpora de manera incremental. Silver y Gold procesan después los datos resultantes sin requerir cambios en el código.

`expected_batch_id` se utiliza solamente en `04_validate_pipeline.ipynb`: expresa qué lote esperamos comprobar en esa ejecución. Al crear `batch_003`, cambiar el parámetro a `batch_003` permite validar que ese lote llegó a Bronze, fue aceptado o enviado a cuarentena, aparece en Gold y aplicó la corrección esperada.

Si se deposita `batch_003` pero se conserva `expected_batch_id=batch_002`, el ETL igualmente procesará el archivo nuevo. Sin embargo, la tarea `validate` puede fallar porque seguirá evaluando las expectativas de `batch_002` y comparando la corrida como si fuera una reejecución sin datos nuevos. El estado rojo representa entonces una expectativa de prueba incorrecta, no una incapacidad del pipeline para detectar el archivo.

## Tablas resultantes

```text
bronze_transactions_incremental
bronze_transactions_all       (vista)
silver_customers
silver_products
silver_transactions
silver_transactions_quarantine
gold_daily_sales
gold_customer_risk
gold_batch_summary
pipeline_run_audit
```

## Preguntas de análisis y comprensión

Respondé las siguientes preguntas después de completar las ejecuciones con `batch_002` y `batch_003`. En las preguntas de análisis incluí la consulta SQL o PySpark utilizada y el resultado relevante. En las preguntas sobre código, indicá el notebook y la sección que fundamentan tu respuesta.

### Análisis de las tablas

1. ¿Cuántas filas físicas recibió cada lote en `bronze_transactions_incremental`? Escribí una consulta que muestre el resultado por `source_batch_id`.
2. Para cada lote, ¿cuántas transacciones fueron aceptadas y cuántas quedaron en `silver_transactions_quarantine`? Reconciliá tus resultados con `gold_batch_summary`.
3. ¿Qué motivos de rechazo aparecen en la cuarentena y cuántos registros tiene cada uno por lote? ¿Los rechazos observados coinciden con los casos introducidos por el generador?
4. Seguí la transacción `42` desde `bronze_transactions_all` hasta `silver_transactions`. ¿Cuántas versiones existen en Bronze y cuál quedó vigente en Silver? Mostrá las columnas que justifican la elección.
5. Comprobá mediante una consulta que `silver_transactions` tiene una sola fila por `transaction_id`. ¿Qué resultado indicaría que la deduplicación falló?
6. Calculá la tasa de rechazo de cada lote como `rechazadas / (aceptadas + rechazadas)` en `gold_batch_summary`. ¿Es correcto comparar solamente las cantidades absolutas si los lotes tienen tamaños diferentes?
7. ¿Qué día presenta el mayor monto total y cuál presenta la mayor cantidad de transacciones? Consultá `gold_daily_sales` y explicá si ambos máximos coinciden.
8. ¿Qué canal de pago tiene la mayor tasa global de fraude? Calculala como `SUM(fraud_transactions) / SUM(transaction_count)` y explicá por qué no corresponde promediar directamente `fraud_rate`.
9. ¿Qué combinación de país y categoría concentra el mayor monto vendido? Mostrá también la combinación líder dentro de cada país.
10. Compará las dos primeras filas de `pipeline_run_audit` correspondientes a la reejecución de `batch_002`. ¿Qué métricas permanecen iguales y qué columna demuestra que se realizó la comparación de idempotencia?

### Interpretación del código y del pipeline

11. En `01_ingest_bronze_incremental.ipynb`, ¿qué problema resuelve `COPY INTO` y qué información utiliza para evitar cargar dos veces el mismo archivo físico?
12. ¿Por qué la vista `bronze_transactions_all` usa `UNION ALL` en lugar de eliminar duplicados? ¿En qué capa se resuelven los duplicados de negocio y por qué?
13. ¿Por qué las transacciones iniciales reciben `source_batch_id='initial'` y usan `event_ts` como `updated_at`? ¿Cómo afecta eso a la corrección de la transacción `42`?
14. En `quality_rules.py`, ¿qué ventaja ofrece `try_cast` frente a un `cast` convencional cuando llega un importe como `N/A`?
15. Las reglas de calidad asignan una única `quality_reason`. ¿Qué sucede si un registro viola más de una regla y por qué importa el orden de las condiciones?
16. Explicá cómo se construye `_record_key` y cómo se usa junto con `row_number`. ¿Qué caso cubre el hash cuando `transaction_id` no puede convertirse a un número?
17. Interpretá las dos cláusulas principales del `MERGE` de `silver_transactions`. ¿Cuándo se actualiza una fila existente y cuándo se inserta una nueva?
18. ¿Por qué las tablas Gold se reconstruyen completamente en esta práctica mientras Silver se actualiza con `MERGE`? Mencioná una ventaja y una limitación de cada estrategia.
19. ¿Por qué `expected_batch_id` no participa en la detección del archivo nuevo? Indicá qué parte del pipeline descubre `batch_003` y qué parte utiliza el parámetro.
20. Si la tarea `build_silver` falla, ¿qué ocurre con `build_gold` y `validate` en el Job? Explicá cómo las dependencias del DAG evitan publicar o validar resultados incompletos.

## Duración estimada

| Bloque | Minutos |
|---|---:|
| Repaso y preflight | 15 |
| Contratos, calidad y cuarentena | 25 |
| Silver y `MERGE` | 30 |
| Pausa | 10 |
| Gold y reconciliación | 25 |
| Construcción del Job | 25 |
| Archivo nuevo y primera ejecución | 20 |
| Segunda ejecución y cierre | 10 |
| Visualización orientada a preguntas | 20 |

## Entrega

En tu repositorio personal creá `resolucion-practica-2/` con:

```text
resolucion-practica-2/
├── README.md
├── 02_build_silver.ipynb
├── 03_build_gold.ipynb
├── 04_validate_pipeline.ipynb
└── 05_visualizacion.ipynb
```

El `README.md` debe incluir:

- Nombre y `student_id`.
- Captura del DAG del Job con las cuatro tareas.
- URL del Job o su nombre exacto.
- Resultados de la primera y segunda ejecución.
- Cantidades aceptadas y rechazadas para `batch_002`.
- Explicación breve de por qué `COPY INTO` y `MERGE` resuelven problemas diferentes.
- Explicación del grano de cada tabla Gold.
- Las cuatro visualizaciones y una respuesta explícita para cada pregunta.
- Respuestas a las 20 preguntas de análisis y comprensión, incluyendo las consultas utilizadas cuando corresponda.

No incluyas datos, credenciales ni tokens.

