
using AnalisisVentas.ETL.Class;
using CsvHelper;
using System.Data.SqlClient;
using System.Globalization;


namespace AnalisisVentas.ETL.Services
{
    public class OrdersService
    {
        private readonly DatabaseManager _dbManager = new DatabaseManager();

        public List<Orders> ExtractAndTransform(string filePath)
        {
            List<Orders> orders;

            //OJO EXTRACCIÓN 
            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                orders = csv.GetRecords<Orders>().ToList();
            }
            Console.WriteLine($"Pedidos extraídos del CSV: {orders.Count}");

            //OJO TRANSFORMACIÓN 
            var distinctOrders = orders
                .GroupBy(o => o.OrderID)
                .Select(g => g.First())
                .ToList();
            Console.WriteLine($"Pedidos deduplicados en memoria: {distinctOrders.Count}");

            // El filtrado 
            var validOrders = new List<Orders>();
            foreach (var order in distinctOrders)
            {
                if (order.OrderID <= 0 || order.CustomerID <= 0)
                {
                    Console.WriteLine($"Advertencia: Pedido ID {order.OrderID} con CustomerID {order.CustomerID} inválido. Saltando.");
                    continue;
                }

                if (order.OrderDate == default(DateTime))
                {
                    Console.WriteLine($"Advertencia: Pedido ID {order.OrderID} con fecha inválida '{order.OrderDate}'. Saltando.");
                    continue;
                }

                validOrders.Add(order);
            }
            Console.WriteLine($"Pedidos válidos después de limpieza: {validOrders.Count}");

            return validOrders;
        }

        public int Load(List<Orders> orders)
        {
            const string sqlInsert = @"
                INSERT INTO Orders (OrderID, CustomerID, OrderDate, Status)
                VALUES (@OrderID, @CustomerID, @OrderDate, @Status);";

            int loadedRecords = 0;
            foreach (var order in orders)
            {
                try
                {
                    var parameters = new[]
                    {
                        new SqlParameter("@OrderID", order.OrderID),
                        new SqlParameter("@CustomerID", order.CustomerID),
                        new SqlParameter("@OrderDate", order.OrderDate),
                        new SqlParameter("@Status", (object)order.Status ?? DBNull.Value)
                    };

                    _dbManager.ExecuteNonQuery(sqlInsert, parameters);
                    loadedRecords++;
                }
                catch (SqlException ex)
                {
                    Console.WriteLine($"Error al cargar pedido ID {order.OrderID} (Cliente: {order.CustomerID}): {ex.Message}");
                }
            }
            return loadedRecords;
        }
    }
}