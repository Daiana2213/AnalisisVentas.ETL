using AnalisisVentas.ETL.Class;
using AnalisisVentas.ETL.Services;
using System;
using System.IO; 

namespace AnalisisVentas.ETL
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("--- Inicio del Proceso ETL para Análisis de Ventas ---");

            DatabaseManager dbManager = new DatabaseManager();
            dbManager.TestConnection(); 

            CustomersService customersService = new CustomersService();
            ProductsService productsService = new ProductsService();
            OrdersService ordersService = new OrdersService();
            OrderDetailsService orderDetailsService = new OrderDetailsService();

            string rutaArchivoCustomers = @"C:\Users\Daiana\OneDrive - Instituto Tecnológico de Las Américas (ITLA)\ITLA\2do año\Electiva 1 y 2\Archivo CSV Análisis de Ventas-20250924\customers.csv";
            string rutaArchivoProducts = @"C:\Users\Daiana\OneDrive - Instituto Tecnológico de Las Américas (ITLA)\ITLA\2do año\Electiva 1 y 2\Archivo CSV Análisis de Ventas-20250924\products.csv";
            string rutaArchivoOrders = @"C:\Users\Daiana\OneDrive - Instituto Tecnológico de Las Américas (ITLA)\ITLA\2do año\Electiva 1 y 2\Archivo CSV Análisis de Ventas-20250924\orders.csv";
            string rutaArchivoOrderDetails = @"C:\Users\Daiana\OneDrive - Instituto Tecnológico de Las Américas (ITLA)\ITLA\2do año\Electiva 1 y 2\Archivo CSV Análisis de Ventas-20250924\order_details.csv";

            if (!File.Exists(rutaArchivoCustomers)) { Console.WriteLine($"Error: Archivo no encontrado en {rutaArchivoCustomers}"); return; }

            // OJO ETAPA DE EXTRACCION

            // ETL para Clientes (Customers) ---
            Console.WriteLine("\n==============================================");
            Console.WriteLine(">>> INICIO ETL: Customers <<<");

            //Aqui se extrae y se transforma
            var validCustomers = customersService.ExtractAndTransform(rutaArchivoCustomers);
            Console.WriteLine("--- FASE DE CARGA Customers (UPSERT) ---");

            int loadedCustomers = customersService.Load(validCustomers);
            Console.WriteLine($"\n CARGA FINALIZADA: {loadedCustomers} los registros ha sido procesados y cargados :)");
            Console.WriteLine("==============================================");

            // ETL para Producto (Products) ---
            Console.WriteLine("\n==============================================");
            Console.WriteLine(">>> INICIO ETL: Products <<<");

            var validProducts = productsService.ExtractAndTransform(rutaArchivoProducts);
            Console.WriteLine("--- FASE DE CARGA Products (UPSERT) ---");

            int loadedProducts = productsService.Load(validProducts);
            Console.WriteLine($"\n CARGA FINALIZADA: {loadedProducts} los registros ha sido procesados y cargados :)");
            Console.WriteLine("==============================================");

            // ETL para Ordenes (Ordes) ---
            Console.WriteLine("\n==============================================");
            Console.WriteLine(">>> INICIO ETL: Orders <<<");

            var validOrders = ordersService.ExtractAndTransform(rutaArchivoOrders);
            Console.WriteLine("--- FASE DE CARGA Orders (INSERT) ---");

            int loadedOrders = ordersService.Load(validOrders);
            Console.WriteLine($"\n CARGA FINALIZADA: {loadedOrders} los registros ha sido procesados y cargados :)");
            Console.WriteLine("==============================================");

            // ETL Detalles de Pedido (Order_Details) ---
            Console.WriteLine("\n--- Procesando Detalles de (Order_Details) ---");
            var validOrderDetails = orderDetailsService.ExtractAndTransform(rutaArchivoOrderDetails);

            int loadedOrderDetails = orderDetailsService.Load(validOrderDetails);
            Console.WriteLine($"\n Carga de Detalles de Pedido finalizada :). Registros cargados: {loadedOrderDetails}");

            Console.WriteLine("\n--- Mi Proceso ETL esta COMPLETO ---");
            Console.ReadKey();
        }
    }
}