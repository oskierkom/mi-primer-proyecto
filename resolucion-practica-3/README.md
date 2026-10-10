# Entrega práctica 3 — Bases de datos NoSQL

- **Nombre:** Myrna Oskierko
- **student_id:** myrna
- **Escala:** small

Respondé cada pregunta en una a tres oraciones. Cuando se pide un dato, copiá el valor que te dio el notebook.

## CAP y PACELC

**1.** Con la red partida, ¿qué respondió la réplica `C` en modo CP y qué respondió en modo AP? ¿Qué propiedad de CAP sacrifica cada modo?

En CP devuelve `ERROR: no disponible`. Al quedar aislada, no alcanza el quórum de 2. Sacrifica la disponibilidad.
En AP devuelve `100` de su copia local, valor que quedó viejo por la escritura de `saldo=80`. Sacrifica consistencia.

**2.** En modo AP, ¿qué saldo quedó en las tres réplicas después de la reparación y qué escritura se perdió?

Todas las réplicas quedan con `saldo=50`. Si bien la réplica C se perdió la escritura de `saldo=80`, el método `heal()` se queda con la versión más nueva.

**3.** Según el gráfico de latencia, ¿cuál es la p99 con `W=1` y con `W=3`? ¿Qué se gana a cambio de esperar más réplicas? (PACELC)

9.6 y 55.6 milisegundos respectivamente. Con `W=3` se espera a `C` siempre, confirmando la escritura en todas las réplicas y asegurando que ninguna lectura pueda leer un dato desactualizado. A más réplicas, más latencia a cambio de la consistencia.

**4.** Con cinco réplicas partidas en `ABC | DE`, ¿qué lado pudo seguir escribiendo en modo CP y por qué?

El quórum se calcula como 5//2 +1 = 3. El grupo `ABC`, al tener tres nodos, por lo que puede escribir correctamente. El grupo `DE`, en cambio, no alcanza el quórum, por lo que la escritura y lectura dan error. Como solo un lado puede tener la mayoría, nunca hay dos escrituras en simultáneo.

## Clave-valor

**5.** ¿Cuántas veces más rápido fue el GET en memoria que el GET con Spark? ¿Por qué Redis guarda los datos en memoria?

El `GET` en memoria fue ~2638481 veces más rápido que el de Spark (0.0002 vs 531 ms).
Redis guarda todo en memoria por usar `hash tables`. Acceder por clave es O(1) sin I/O en disco. Spark, en cambio, planifica y ejecuta consultas analíticas para encontrar una fila.

**6.** ¿Por qué para contar los clientes de AR hubo que leer y parsear todos los valores? Mencioná una ventaja y una desventaja del modelo clave-valor.

Al no conocerse los campos del JSON ni haber un índice, deben leerse y parsearse todos los valores vía `from_json` para luego filtrar.
Ventaja: el acceso por clave es O(1). Desventaja: no se puede consultar por valor.

**7.** Al pasar de 4 a 5 nodos, ¿qué porcentaje de claves se movió con `hash mod N` y con hashing consistente? ¿Por qué conviene el segundo?

`hash mod N` movió el 79.6% de las claves, mientras que el `hashing consistente` movió el 21.2%, más cercano al ideal del 20%.
El hashing consistente conviene porque al agregar un nodo solo se migran las claves del tramo que ocupa. Los nodos virtuales balancean la carga.

## Documental

**8.** ¿Cuántas filas devolvió la consulta relacional del cliente 42 y cuántos documentos la documental? ¿Qué datos quedaron embebidos dentro del documento?

La consulta relacional devolvió 11 filas, mientras que la documental retornó solo un documento, quedando embebidos el perfil del cliente, un resumen de sus transacciones y el detalle de los productos dentro de cada transacción.

**9.** Al leer documentos con campos distintos, ¿qué hizo Spark con los campos que faltaban en algunos? ¿Por qué se dice que el modelo documental tiene esquema flexible?

Spark infiere un esquema a partir de la unión de los campos de todos los documentos, completando con `null` los faltantes. Se dice que el modelo documental tiene un esquema flexible porque cada documento puede tener campos distintos y no hay necesidad de definir ni migrar nada antes de su carga. Como desventaja está que el `null` que se agrega no distingue entre un campo que no existía, o un campo que vino como nulo. Para ello, hay que inspeccionar el texto original.

**10.** ¿Cuánto pesa el documento más grande? ¿Qué problema aparece si un documento crece sin límite?

El documento más grande pesa 5.2KB con un aproximado de 225B por transacción. Si un documento crece sin límite los costos de lectura y reescritura son cada vez mayores, cada lectura implica traer el documento completo aún si solo se necesita una parte, y la reescritura es cada vez mayor. La solucón sería referenciar en vez de embeber o partir en varios documentos.

**11.** Para calcular el monto por categoría hubo que usar `explode`. ¿Qué tipo de consultas resuelve bien el modelo documental y cuáles le cuestan más?

El modo documental resuelve bien las consultas por entidad que se contestan leyendo un solo documento. Por el otro lado, son más complejas las consultas que cruzan documentos (ej. monto por categoría). Esto porque se tienen que desarmar los arrays con `explode` y pasar a filas para las agregaciones.

## Grafos

**12.** ¿Cuántos nodos `Customer`, nodos `Device` y aristas `USES` tiene el grafo?

Customer = 5000 nodos, 738 marcados.
Device = 2361 nodos, 0 marcados.
USES = 5400 aristas.

**13.** ¿Cuántos joins necesitó SQL para el patrón de 2 saltos y para el de 4? ¿Por qué Neo4j recorre relaciones más eficientemente que una base relacional?

SQL necesitó 3 joins para 2 saltos y 5 para 4 saltos. Neo4j es más eficiente porque cada nodo guarda punteros a sus vecinos, por lo que recorrer una relación es unicamente seguir los punteros. El costo depende de los vecinos que se visitan, no del tamaño del grafo.

**14.** ¿Cuántas iteraciones tardó en converger la búsqueda de componentes conexos y cuántos nodos tiene el componente más grande?

Tardó 14 iteraciones. El componente `c:122` es el más grande, con 23 nodos.

**15.** ¿Qué porcentaje de aristas quedó entre máquinas distintas al repartir los nodos con `hash(id) mod 4`? ¿Por qué es difícil distribuir una base de grafos?

El 74.4% de las aristas quedaron en máquinas distintas, mientras que la partición por componente cortó 0%.

La distribución de bases de grafos se dificulta porque las relaciones pueden cruzar cualquier partición. Cada arista que se corta obliga un salto en la red, los cuales se acumulan a mayor cantidad de saltos por consulta. Para evitar cortes habría que particionar por topología, pero es más costoso y puede desbalancear la carga.

## Vectorial

**16.** ¿Qué representa el vector de cada cliente y qué mide la similitud coseno?

Los vectores representan el comportamiento de compra de los clientes: contiene proporciones de gastos por categoria, pagos por canal, ticket promedio y compras altas.
La similitud del coseno mide el angulo de los vectores. Mientras más cercano a uno, mayor es la similitud.

**17.** ¿Qué porcentaje de clientes marcados hay entre los vecinos de clientes marcados y cuál es la tasa base? ¿Para qué sirve buscar "vecinos parecidos"?

De los 10 vecinos más parecidos de un cliente marcado, el 31.6% también lo está. La tasa base es 14.8%.
Buscar vecinos parecidos en este caso sirve para detectar candidatos fraude por comportamiento y para clasificar con KNN. No obstante, el clasificador tuvo una exactitud del 85.9%, menor a la probabilidad de 'no marcado' (86.3%).

**18.** En la curva del índice IVF, ¿qué recall y qué porcentaje de vectores escaneados se obtienen con `nprobe=4`? ¿Qué se gana y qué se pierde con la búsqueda aproximada (ANN)?

Con `nprobe=4` se obtiene un un recall@10 de 87.2% al escanear el 5.99% de los vectores. Esto da velocidad y menores costos, al comparar con una fracción chica del total, pero pierde exactitud porque la búsqueda puede dejar fuera vecinos. Subir `nprobe` mejora el recall a costa de escanear más vectores.

## Columnar

**19.** ¿Cuánto ocupan los datos en CSV, JSON y Parquet? ¿Qué columnas leyó Spark para la consulta de `amount` sobre Parquet (`ReadSchema`)? ¿Cuántos archivos se pueden saltear con los datos ordenados?

CSV = 50MB, JSON = 121.1MB, Parquet = 5.3MB.

Para consultar `amount` del Parquet, Spark solo leyó esa columna (`ReadSchema: struct<amount:decimal(12,2)>`)

Con los datos ordenados, para un `amount > 1900` se pueden saltear 7 de 8 archivos.

**20.** ¿Por qué el formato columnar es bueno para analítica y poco conveniente para modificar una fila por vez?

Conviene para la analítica porque se guardan los valores de cada columna juntos, permitiendo leer solo las columnas necesarias, mejorar la compresión y saltear bloques con estadísticas tipo min/max.

En cambio, no es eficiente para modificaciones por fila dado que se reparten entre varias columnas y bloques comprimidos, por lo que sobrescribir una sola fila implica reescribir varios bloques, aumentando los costos.