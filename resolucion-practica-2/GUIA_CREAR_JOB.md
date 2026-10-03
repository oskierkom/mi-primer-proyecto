# Guía — Crear el Lakeflow Job

El Job debe ejecutar cuatro notebooks en secuencia. El generador de archivos **no forma parte del Job**: representa un sistema externo que deposita datos.

## 1. Crear el Job

1. Abrí **Jobs & Pipelines** en la barra lateral.
2. Seleccioná **Create** → **Job**.
3. Nombralo `bigdata_<student_id>_silver_gold`.
4. Usá compute **Serverless** en todas las tareas.

## 2. Parámetros del Job

En **Job details** → **Edit parameters**, agregá:

| Nombre | Valor predeterminado |
|---|---|
| `student_id` | Tu identificador de la práctica 1 |
| `scale` | `small` o la escala usada en la práctica 1 |
| `expected_batch_id` | `batch_002` |
| `job_run_id` | `{{job.run_id}}` |

Los parámetros se propagan a las tareas de notebook. No escribas el correo como `student_id`.

## 3. Tareas y dependencias

Creá exactamente este DAG:

| Task name | Notebook | Depends on |
|---|---|---|
| `ingest_bronze` | `01_ingest_bronze_incremental.ipynb` | — |
| `build_silver` | `02_build_silver.ipynb` | `ingest_bronze` |
| `build_gold` | `03_build_gold.ipynb` | `build_silver` |
| `validate` | `04_validate_pipeline.ipynb` | `build_gold` |

En cada tarea:

1. Elegí **Notebook** como tipo.
2. Seleccioná el notebook dentro del Git folder.
3. Confirmá **Serverless**.
4. No agregues librerías.
5. Guardá antes de crear la siguiente tarea.

El gráfico debe ser una línea de cuatro nodos. Si una tarea falla, las posteriores deben quedar omitidas.

`05_visualizacion.ipynb` se ejecuta manualmente después de validar el Job. No lo agregues como tarea: consume Gold, pero no forma parte del procesamiento de datos.

## 4. Primera ejecución

1. Confirmá que `00_generate_new_batch.ipynb` creó `batch_002`.
2. Seleccioná **Run now**.
3. Abrí la ejecución y observá el estado de cada tarea.
4. Entrá a cada tarea para revisar salida, parámetros y duración.
5. `validate` debe finalizar correctamente y registrar una fila en `pipeline_run_audit`.

## 5. Prueba de idempotencia

Sin ejecutar nuevamente el generador:

1. Seleccioná otra vez **Run now**.
2. `COPY INTO` no debe cargar nuevamente el mismo archivo físico.
3. `MERGE` no debe duplicar `transaction_id`.
4. Las métricas de Silver y Gold deben coincidir con la ejecución anterior.
5. `validate` mostrará `idempotence_compared=True`.

La tabla de auditoría tendrá dos filas: una por ejecución, aunque las métricas de negocio sean iguales.

## 6. Probar otro lote

1. Ejecutá el generador con `batch_003`.
2. Usá **Run now with different parameters**.
3. Cambiá solamente `expected_batch_id` a `batch_003`.
4. Ejecutá y verificá `gold_batch_summary`.

## Errores frecuentes

| Síntoma | Revisión |
|---|---|
| El notebook usa otro esquema | `student_id` o `scale` no coinciden con la práctica 1 |
| `expected_batch_id` no aparece | El archivo no fue creado o el Job apunta a otro alumno |
| Las tareas corren en paralelo | Faltan dependencias en el DAG |
| `validate` queda omitida | Alguna tarea anterior falló; abrir su salida |
| La segunda ejecución cambia métricas | Revisar deduplicación y condición del `MERGE` |
| Cuota agotada | Esperar el reinicio de Free Edition y evitar escala `demo` |

