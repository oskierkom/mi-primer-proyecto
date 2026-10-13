# Entrega práctica 4 — Streaming

- **Nombre:** Myrna Oskierko
- **student_id:** myrna
- **Escala:** small

Respondé cada pregunta en una a tres oraciones. Cuando se pide un dato, copiá el valor que te dio el notebook.

### Log y offsets (`01_log_y_offsets`)

1. En el ejemplo en Python, ¿cuántos mensajes quedaron en la cola y cuántos en el log después de leer tres? ¿Qué diferencia hay entre una cola tradicional y un log como Kafka?

Quedaron dos mensajes en cola y cinco en el log con un offset del consumidor = 3. En una cola tradicional, los mensajes se borran tras la lectura, mientras que en Kafka los mensajes se conservan y cada consumidor avanza solo su offset, permitiendo que varios consumidores puedan leer lo mismo y que se pueda hacer replay.

2. Después de la llegada 2, ¿cuántos mensajes había leído el consumidor `antifraude` y cuántos `reportes`? ¿El atraso de `reportes` afectó a `antifraude`? ¿Por qué?

Al terminar la llegada 2 se leyeron 180 mensajes "antifraude" (60 + 120) y ningún reporte. El atraso de reportes no afecta porque cada consumidor tiene su checkpoint.

3. ¿Qué es un offset y dónde lo guarda cada consumidor en esta práctica?

El offset indica hasta dónde un consumidor leyó en el log. En el notebook es la versión de la delta del topic. Cada consumidor guarda su offset en su propio checkpoint, permitiendo que en caso de reejecución no se tenga que leer todo de nuevo.

4. ¿Cuántos mensajes leyó el replay? ¿Qué propone la arquitectura Kappa para reprocesar datos y en qué se diferencia de Lambda?

El replay leyó 280 mensajes. La arquitectura Kappa presenta un unico codigo de streaming y reprocesa lanzando una consulta nueva que lee el log desde el principio. Lambda, en cambio, mantiene dos sistemas, batch y streaming, con la misma lógica duplicada.


### Ingesta y enriquecimiento (`02_ingesta_y_enriquecimiento`)

5. ¿Cuántas filas nuevas procesó Bronze en cada llegada y cuántas al reejecutar sin archivos nuevos? ¿Qué componente evita leer dos veces el mismo archivo?

|llegada|filas en el archivo|nuevas en Bronze|nuevas en Silver|
|---|---|---|---|
|stream_001|60|60|60|
|stream_002|120|120|120|
|stream_003|40|40|40|
|stream_004|40|40|40|
|stream_005|20|20|20|

En bronze no se procesaron filas al reejecutar dado el checkpoint del auto loader que mantiene los archivos ya leídos.

6. ¿Qué motivos de rechazo aparecieron en la cuarentena y en qué llegada? ¿Por qué Bronze guarda el texto original en lugar de descartar los registros con errores?

Aparecieron INVALID_AMOUNT y UNKNOWN_CUSTOMER, ambos en el stream_002. Bronze guarda todo tal cual llega para mantener el dato original disponible en caso de cambios.

7. ¿Qué columnas agregó el join con `silver_customers` y `silver_products`? ¿Qué tipo de join es (stream-stream, stream-tabla o tabla-tabla) según la teoría?

El join agrega los campos country y category de silver_customers y silver_products respectivamente. El join es de tipo stream-tabla, el stream se enriquece con tablas estaticas de referencia.

### Tiempo y ventanas (`03_tiempo_y_ventanas`)

8. ¿Qué diferencia hay entre event time y processing time? Usá `late_ok` como ejemplo.

`event time` indica el timestamp de cuándo ocurrió el evento, mientras que `processing time` indica cuándo llegó al sistema. `late_ok` ocurrió a las 12:03 pero apareció en la llegada 2 luego de los eventos de las 12:04.

9. ¿Cuánto valía el watermark al terminar cada llegada? ¿Cómo se calcula a partir del máximo event time visto?

|llegada|lote|máximo event time visto|watermark al terminar|
|---|---|---|---|
|1|stream_001|12:04|11:54|
|2|stream_002|12:08|11:58|
|3|stream_003|12:26|12:16|
|4|stream_004|12:27|12:17|
|5|stream_005|12:45|12:35|

Se calcula restandole 10 minutos al maximo event time visto.

10. ¿En qué llegada aparecieron las primeras ventanas tumbling en modo append y por qué no antes?

Los primeros tumbling en modo append aparecen en la llegada 3. No llegan antes porque el watermark no habia superado el fin de ninguna ventana. El stream_003 pasó a 12:16 y cerró la ventana 12-12:10.

11. ¿Qué pasó con `late_bad`? ¿Por qué se aceptó `late_ok` y se descartó `late_bad`?

late_bad con 12:02 llega en el stream_004 con el watermark 12:16, se descarta y no suma ninguna ventana. late_ok con 12:03, en cambio, llega en el stream_002 con el watermark en 11:54. Al estar dentro del margen de tolerancia, se sumó a su ventana.

12. Mirando el gráfico de ventanas, ¿en qué se diferencian tumbling, hopping y session? Da un ejemplo de negocio para cada una.

- Tumbling: ventanas fijas de 5 minutos que no se solapan. Ej: compras por canal cada 5 minutos.
- Hopping: ventanas de 10 minutos que se solapan cada 5 mintuos. Ej: compras de los ultimos 10 minutos recalculadas cada 5.
- Session: ventana por cliente que se cierra tras 5 minutos sin compras, duración variable. Ej: racha de compras de un cliente.

13. Para la ventana [12:00, 12:05) del canal card, ¿cuántas veces y con qué valores se emitió en modo update y en modo append? ¿Qué ventaja y qué desventaja tiene cada modo?

|emitida_en_llegada|compras|monto|modo|
|---|---|---|---|
|1|40|6000.00|update|
|2|60|8400.00|update|
|3|60|8400.00|append|

Update da resultados rápidos pero se deben tolerar correcciones, append entrega un único valor final, pero tiene más demora.

14. ¿Qué se gana y qué se pierde si el watermark fuera de 1 minuto en lugar de 10?

Qué watermark elegir es un trade-off entre latencia y completitud. Con un minuto se ganaría en latencia porque las ventanas se cerrarían antes, pero se perdería completitud porque eventos tardíos serían descartados.

### Garantías y fallas (`04_garantias_y_fallas`)

15. ¿Cuántas filas quedaron en el destino sin deduplicar y cuántas deduplicando por `event_id`? ¿Por qué el productor puede enviar el mismo evento dos veces?

Quedaron 220 filas sin deduplicar y 200 deduplicadas por event_id. El productor puede mandar el mismo evento dos veces porque se trabaja con at-least-once (reintentar en caso de duda).

16. ¿Qué pasó al reiniciar con el mismo checkpoint y qué pasó al perderlo? ¿Qué garantía de entrega se observa en cada caso?

Al reiniciar con el mismo checkpoint el destino siguió en 200 filas, sin reprocesar nada (exactly-one). Al perder el checkpoint, la consulta empezó de nuevo y el destino append pasó a 400 filas (at-least-one).

17. ¿Por qué con `MERGE` el resultado no cambió aunque se perdiera el checkpoint? ¿Qué significa que una escritura sea idempotente?

El `merge` por event_id solo inserta si el evento no existe, por lo que reprocesar todo con otro checkpoint deja el destino igual. Una escritura idempotente significa que, sin importar cuantas veces se realice, siempre tiene el mismo resultado.

### CDC (`05_cdc`)

18. ¿Cuántos eventos de cambio generó el log y de qué tipos? ¿Por qué un `UPDATE` genera dos eventos?

El log generó: 6 inserts + 3 update_preimage + 3 update_postimage + 1 delete = 13 eventos. Un update genera dos eventos porque registra la fila antes (preimage) y despues (postimage) del cambio.

19. ¿Se pudo reconstruir la tabla a partir del log? ¿Qué información tiene el log que la tabla actual no tiene?

Se pudo reconstruir la tabla a partir del log quedandose con el último cambio por customer_id (sin preimage y descartando los que terminan en delete). El log contiene la historia, como que el cliente 2 estuvo en UY, luego en CL y finalmente en BR, así como que existió un cliente 3 y que fue eliminado.

### Cierre

20. Según la comparación de motores de la teoría, ¿cuándo elegirías Spark Structured Streaming, Flink o Kafka Streams? Mencioná una ventaja de cada uno.

- Spark Structured Streaming: conviene cuando ya se tiene conocimientos en Spark y la latencia no es prioritaria. Como ventajas, utiliza la misma API de dataframes y SQL y se conecta directo con Kafka, Delta Lake y MLLib.
- Flink: conviene cuando la latencia es crítica. Procesa evento por evento sin microbatches con latencias muy bajas.
- Kafka Streams: para microservicios. Al ser una libreria que se embebe en la app, no requiere cluster propio. También es evento a evento y con latencias sub-segundo.