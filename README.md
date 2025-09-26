<h1>Analisis de Ventas ETL</h1>
- Estado del proyecto: Finalidado.

<P>Hola, este es un proyecto en donde se implementa el proceso ETL (entiendase como Extract, Transform y Load) hecho en C# usando la biblioteca CSVHelper para el análisis de ventas.</P> 
En este se estan extrayendo la data desde archivos CSV que contienen los Clientes, productos, órdenes y los detalles de estas órdenes. Luego de ello se transforma, ya sea eliminando los duplicados, y finalmente se cargan estos a una BD que en este caso se llama AnalisisVentasDO. 
<p>Su estrctura esta en clases de servicio para cada entidad (CustomersService, ProductsService, OrdersService y OrderDetailsService), una clase DatabaseManager para manejar la conexión y las consultas SQL, y un archivo Program.cs que coordina la ejecución completa del flujo ETL.</p>

