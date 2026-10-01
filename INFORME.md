# Estado inicial del proyecto

Se tiene un sistema que buscar calcular un top configurable para descubrir el top de la cantidad máxima de frutas máximas
que reciba de los clientes.

Para ello tiene los siguientes componentes:
- Gateway:  Engloba la lógica de comunicación entre clientes y el servidor. Se encarga de procesar las requests de los clientes de forma concurrente, donde los mensajes que puede recibir son FRUIT RECORDS que contienen la tupla (cantidad, nombre_fruta) o bien un END OF RECORDS, al recibir este último es cuando se indica que  todas las frutas con las que se calculará el top total de ese cliente fueron enviadas.Es importante mencionar que toda la data de rutas que recibe van a parar a la misma cola de entrada conectada con el componente **Sum**. Por otro lado, para enviar el top se espera constantemente recibirlo de la cola de respuestas, una vez se recibe el mensaje con el top calculado hace que todos los clientes lo escriban en el socket del cliente correspondiente.
- Sum: Recibe los datos que fueron procesados por el **gateway** y hace una suma total por tipo de fruta, y las envía esas sumas parciales a cada uno de los componentes *Agregations* configurados. Sin embargo, no se distribuye la información recibida en los agregations con lo cual hace que se procese información repetida.
- Agregator: Calcula el top parcial con la información de los componentes **Sum** recibida, en este caso siempre se recibirá la isma información de todos entonces no está aportando valor en la implementación.
- Joiner: Actualmente simplemente manda el top recibido por el Agregator. Es decir no soporta que haya más de uno.

Con toda esa base, podemos tenemos las siguientes tareas pendientes:

### Para el componente Sum
-  Distribuir la información de la data para no hacer broadcast a todas las instancias de los Agregator.
-  Implementar algún mecanismo de sincronización para terminar todos los compnoenentes de Sum, una vez alguno recibe el EOF.

### Para el componente Agregator
-  Soportar múltiples clientes Agregators y enviar los tops parciales al Joiner.

### Para el Joiner
-  Calcular top global para cada cliente, en base a los tops parciales recibidos por el Agregator.

### Cómo y qué cambiar el sistema
Si pensamos aplicar una arquitectura de MapReduce teniendo en cuenta que habría que cambiar el sistema para que calcule el top para la información de cada cliente lo que podríamos hacer es:
* Map: El componente Sum particiona la información por tipo de fruta para un id de cliente específico asociandolo a un contador con la cantidad total recibida que se envía  a un único  agregator.
* Agregator: Cada componente calcula el top parcial de solo las frutas que le tocaron, separando también las cantidades de una misma futa por cliente.
* Join:  Junta todos los tops totales de cada cliente para  cada fruta y se queda con la cantidad requerida. Para ello habría que verificar recibir un EOF de cada agregator asociado.

### ¿Cómo segregar la data?
Habría que decidir qué frutas van en cada agregator para que se calcule bien su cantidad total por cliente y asegurarnos que la distribución sea uniforme y determinística podemos decidir que
en base al nombre de la fruta redireccionamos a qué agregator va imponiendo como routing key del exchange algo como `hash(nom_fruta) % AGREGATION_AMOUNT`.

### ¿Cómo distinguir la data de clientes?
*MessageHandler* no maneja el caso de devolver el top correspondiente en la función `DeserializeResultMessage`, es en ésta misma donde debemos devolver **nil**
en caso de que un cliente use el handler que no le corresponde al intentar procesar un mensaje de la cola de output que no es el resultado de su top.
Para este trabajo debemos utilizar una técnica de hashing para mapear las conexiones de los clientes con su id único.¿?

### Manejo de EOF por clientes
Es importante notar que también debemos menejar el caso de manejar el EOF por cliente, para que se pueda ir avanzando con el cálculo de tops de forma concurrente. Por ejemplo en Sum habría que guardar las routing keys a las que le envío el cliente x y a cada una de ellas notificarle que ya no hay más data que procesar para ese cliente x; de esta forma, es seguro decir que todas las instancias de Sum recibieron todos los datos esperados del cliente x.


### ¿Cómo coordinar los componentes?
Se utilizará una cola adicional de tipo topic para difundir el EOF entre todas las instancias de Sum por ejemplo. Y para los Agregator difundir la terminación de todos y unificar un único EOF al joiner.

# Estrategias aplicadas

## Idea general
Se busca modificar el sistema para poder soportar clientes de forma concurrente en los componentes `Sum`, `Agregation` y `Join` sin modificar el resto de la arquitectura.

### Componente `messageHandler`
Para poder distinguir entre clientes, es necesario  tener un identificador único, en este caso un número único. Además para determinar cuándo un cliente terminó de recibir todos los fruit items en total para todas las instancias de sum, agregamos un campo en el EOF con la cantidad de items enviados por ese cliente.

### Componente `Sum`
Para lograr que cada componente de Sum notificque a los agregators  correspondientes que ya no recibirá más data para un cliente específico, es necesario hacer que los componentes de Sum reciban la notifciación del EOF que solamente un componente recibió, esta información se propagará por una cola con mensajes de control de manera que todas las instancias que no recibieron dicho mensaje queden notificadas y puedan enviar esta información al resto de compoenentes que pudieron no haberlo recibido. Esto último es necesario ya que los Sum deben enviar sus sumas parciales luego de este evento.

# Implementación

## Discriminación de mensajes según cliente
Para manejar múltiples clientes de forma concurrente, es necesario agregar un `client_id` para identificar la información que cada uno envía, 
esto lo agregamos en `messageHandler` para que al serializar y desserializar los mensajes de **EOF y DATA** se tengan en cuenta. Para no modificar el gateway
lo que hacemos es mantener un contador atómico global que se utilice a medida que se inicialice un nuevo hanlder poder asegurar un id único por cliente.

Hacemos un doble hasheo para poder discriminar sumas de cantidades en base al identificador creado.

## Distrubución de la información hacia los aggregators
Con el fin de distribuir el procesamiento de la información que tiene el componente **Sum**
hacia los **Agregators** usamos una función de hashing sobre el nombre de la fruta para elegir
en qué componente serán procesados y tomamos módulo para que sea de entre los definidos por el sistema. Así logramos una distribución uniforme y determinística para una misma nonbre de fruta.
Hacemos la división por el nombre puesto a que si lo hacemos por el cliente podríamos sobrecargar un único compoenente pues puede variar la cantidad de items por cliente. 

### Envío de EOF: ¿Cuándo están listos todos?
El único EOF envíado por el getaway llega a una sola instancia de Sum. Cómo no es completamente seguro que al llegar este
todos los semás componentes hayan terminano de procesar los fruititems, es necesario implementar un mecanismo de coordinación
entre estos. Para asegurarnos de que todos recibieron la información del cliente que manda el EOF, agregamos un nuevo campo al
mensaje con la cantidad de items enviados para ese cliente, de esta forma nos podemos asegurar que todos los componentes hayan procesado en total
dicha cantidad.

Ahora bien, luego de que cada componente haya alcanzado ese `target`, debe enviar sus sumas parciales a los `aggregations` correspondientes
y luego de asegurarnos de su envío, propagar el EOF del cliente a los `aggregations`.

#### Solución: Cola de eventos

##### **Primera Etapa: Confirmar que se recibió la información esperada**
La idea sería tener un exchange en el que los componentes Sum publiquen su progreso y todos los 
demás puedan acceder a esa información, y a la hora de que llegue un EOF sea sencillo saber cuál
fue la cantidad total de datos consumida para un cliente por todos los compoentes Sum.

Los mensajes a publicar serían como `"client_id,#fruit_items"` y cada componente al leerlo suma a su contador de información total. De esta forma nos evitamos mecanismos más complejos como un scatter-gather posible en el que el nodo que reciba el EOF deba recolectar el estado de los demás.

En el código, inicializamos un exchange con el cual enviará mensajes a todo componente sum que haya hecho bind
al mismo, esto es posible utilizando la wildcard `#` provista por el exchange de tipo `topic`. Luego tendremos un hilo (gorutina) uno que consume de la cola y actualiza los contadores de info total recibida por cliente, y las notificaciones de recibido se publicarán a medida que se procesan los datos llegados.

Como el contador si se utilizará en dos hilos distintos debemos protegerlo con un mutex para evitar race conditions. Además para evitar hacer una espera activa una vez se recibe EOF, de forma bloquante esperamos que se cierre el canal, este mismo se cerrará cuando el acumulador del cliente alcance el la cantidad target correspondiente recibida como parámtero en el EOF. El chequeo de cierra el canal se hace cada vez que se actualiza el acumulador al recibir un evento.

NOTA: Que el el hash que lleva los conteos para los clientes se usa en dos hilos distintos, luego es necesario usar mutex sobre el mismo para evitar raceconditions.

El resultado es, el primer Sum en recibir el EOF, evalúa si ya todos los componentes del mismo tipo han recibido la data esperada total para ese cliente, una vez confirmado cada componente envía sus sumas parciales y en posteriormente el EOF correspondiente a todos los agregators.


##### Conteo de FruitItems enviados por cliente

Para implementar el mecanismo de coordinación descripto en el EOF (sección *Envío de EOF: ¿Cuándo están listos todos?*), se agregó en `MessageHandler` un contador `itemsSent` de tipo `uint64` que se incrementa en cada llamada a `SerializeDataMessage`. Al momento de llamar `SerializeEOFMessage`, ese total se embebe en el mensaje de EOF.

## Cálculo de tops parciales hacia el resultado final

En los Aggregators se calcula la cantidad total de frutas de un tipo específico para cada cliente. Esto es posible gracias a la forma en que se distribuye la información en el componente precedente.

En este componente se utilizan estas cantidades totales para calcular los tops parciales que luego serán utilizados por el Joiner para obtener el resultado final.

### Problemáticas: ¿Cuándo enviar el top parcial de  un agregator?

Para asegurarnos de haber recibido todas las sumas parciales, es necesario que en un Agregator se hayan recibido
todas las sumas parciales de todos los sums; Esto lo podemos confirmar esperando reibir en un Agregator una cantidad de EOF´s igual a la cantidad de Sums, pues como cada uno envío su propio EOF nos aseguramos de que esta condición sea cierta.

De esta forma, cada Agregator enviará su EOF sin necesidad de coordinarlos.

## Cálculo de tops de un cliente: resultado final
Al igual que los componentes anteriorres, el procesamiento de múltiples clientes en forma concurrente no está soportado, con lo cual debemos distinguir por `client_id` los tops enviados como `fruits records`, para ello usamos la misma técinca de hashing aplicada en **Sum** y **Agreegator**.
Análogo al componente Agregator, espera recibir una cantidad de  EOF´s igual a la cantidad de Agregations para asegurarse haber recibido todos los tops parciales,luego es posible calcular el top final, y enviarlo por la cola de resultados.

Además, es posible asegurar que luego el gateway enviará el mensaje al cliente correcto pues este caso se cubre en el `MessageHandler`, al intentar deserializar un mensaje que no es para el cliente se saltea el envío al cliente en el componente *Gateway*.


## Graceful Shutdown
Para el caso del componente sum, usamos WaitGroup para asegurarnos de
que el hilo principal espere que terminanen los jobs corriendo en go rutinas; para ello,
incrementamos el contador cada vez que se inicia una nueva gorutina. 
Al recibir la señal de **sigterm** dejamos de consumir de las colas usadas y adicionalmente
usamos un channel para notificar al hilo que espera que todos  los sums terminen de procesar la información de un cliente.
Finalmente, cerramos todas los canales usados por las colas con `Close`.

PAra el resto de componentes se aplica la misma lógica sin necesidad de `WaitGroup` pues es seguro que la gorutina
de `handleSignals` termina luego de recibir la señal.