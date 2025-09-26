
using AnalisisVentas.ETL.Class;
using CsvHelper;
using System.Data.SqlClient;
using System.Globalization;
using System.Collections.Generic;
using System.Linq;
using System;

namespace AnalisisVentas.ETL.Services
{
    public class OrderDetailsService
    {
        private readonly DatabaseManager _dbManager = new DatabaseManager();

        public List<Order_Details> ExtractAndTransform(string filePath)
        {
            List<Order_Details> orderDetails;

            // OJ0 EXTRACCIÓN 
            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                orderDetails = csv.GetRecords<Order_Details>().ToList();
            }
            Console.WriteLine($"Detalles de Pedido extraídos del CSV: {orderDetails.Count}");

            // ojo la TRANSFORMACIÓN de la data de los archivos csv
            var distinctDetails = orderDetails
                .GroupBy(d => new { d.OrderID, d.ProductID })
                .Select(g => g.First())
                .ToList();
            Console.WriteLine($"Detalles de Pedido deduplicados en memoria: {distinctDetails.Count}");

            // Filtrado de data 
            var validDetails = new List<Order_Details>();
            foreach (var detail in distinctDetails)
            {
                if (detail.OrderID <= 0 || detail.ProductID <= 0)
                {
                    Console.WriteLine($"Advertencia: Detalle con OrderID {detail.OrderID} o ProductID {detail.ProductID} inválido. Saltando.");
                    continue;
                }
                if (detail.Quantity <= 0) 
                {
                    Console.WriteLine($"Advertencia: Detalle (Pedido: {detail.OrderID}, Producto: {detail.ProductID}) con Cantidad '{detail.Quantity}' inválida. Saltando.");
                    continue;
                }
                if (detail.TotalPrice < 0) 
                {
                    Console.WriteLine($"Advertencia: Detalle (Pedido: {detail.OrderID}, Producto: {detail.ProductID}) con Precio Total '{detail.TotalPrice}' inválido. Saltando.");
                    continue;
                }

                validDetails.Add(detail);
            }
            Console.WriteLine($"Detalles de Pedido válidos después de limpieza: {validDetails.Count}");

            return validDetails;
        }

        public int Load(List<Order_Details> orderDetails)
        {
            const string sqlInsert = @"
                INSERT INTO Order_Details (OrderID, ProductID, Quantity, TotalPrice)
                VALUES (@OrderID, @ProductID, @Quantity, @TotalPrice);";

            int loadedRecords = 0;
            foreach (var detail in orderDetails)
            {
                try
                {
                    var parameters = new[]
                    {
                        new SqlParameter("@OrderID", detail.OrderID),
                        new SqlParameter("@ProductID", detail.ProductID),
                        new SqlParameter("@Quantity", detail.Quantity),
                        new SqlParameter("@TotalPrice", detail.TotalPrice)
                    };

                    _dbManager.ExecuteNonQuery(sqlInsert, parameters);
                    loadedRecords++;
                }
                catch (SqlException ex)
                {
                    Console.WriteLine($"Error al cargar detalle (Pedido: {detail.OrderID}, Producto: {detail.ProductID}): {ex.Message}");
                }
            }
            return loadedRecords;
        }
    }
}