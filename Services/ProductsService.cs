
using AnalisisVentas.ETL.Class;
using CsvHelper;
using System.Data.SqlClient;
using System.Globalization;
using System.Collections.Generic;
using System.Linq;
using System;

namespace AnalisisVentas.ETL.Services
{
    public class ProductsService
    {
        private readonly DatabaseManager _dbManager = new DatabaseManager();

        public List<Product> ExtractAndTransform(string filePath)
        {
            List<Product> products;

            //OJO EXTRACCIÓN
            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                products = csv.GetRecords<Product>().ToList();
            }
            Console.WriteLine($"Productos extraídos del CSV: {products.Count}");

            //OJO TRANSFORMACIÓN 
            var distinctProducts = products
                .GroupBy(p => p.ProductID)
                .Select(g => g.First())
                .ToList();
            Console.WriteLine($"Productos deduplicados en memoria: {distinctProducts.Count}");

            //OJO Aqui el filtrado 
            var validProducts = distinctProducts
                .Where(p => p.ProductID > 0) 
                .Where(p => !string.IsNullOrWhiteSpace(p.ProductName)) 
                .Where(p => p.Price >= 0) 
                .Where(p => p.Stock >= 0) 
                .ToList();
            Console.WriteLine($"Productos válidos después de limpieza: {validProducts.Count}");

            return validProducts;
        }
        public int Load(List<Product> products)
        {
            const string sqlUpsert = @"
                MERGE Products AS target
                USING (SELECT @ProductID AS ProductID, @ProductName AS ProductName, @Category AS Category, 
                              @Price AS Price, @Stock AS Stock) AS source
                ON (target.ProductID = source.ProductID)
                WHEN MATCHED THEN
                    UPDATE SET 
                        ProductName = source.ProductName,
                        Category = source.Category,
                        Price = source.Price,
                        Stock = source.Stock
                WHEN NOT MATCHED THEN
                    INSERT (ProductID, ProductName, Category, Price, Stock)
                    VALUES (source.ProductID, source.ProductName, source.Category, source.Price, source.Stock);";

            int loadedRecords = 0;
            foreach (var product in products)
            {
                try
                {
                    var parameters = new[]
                    {
                        new SqlParameter("@ProductID", product.ProductID),
                        new SqlParameter("@ProductName", product.ProductName),
                        new SqlParameter("@Category", (object)product.Category ?? DBNull.Value),
                        new SqlParameter("@Price", product.Price),
                        new SqlParameter("@Stock", product.Stock)
                    };

                    _dbManager.ExecuteNonQuery(sqlUpsert, parameters);
                    loadedRecords++;
                }
                catch (SqlException ex)
                {
                    Console.WriteLine($"Error al cargar/actualizar producto ID {product.ProductID}: {ex.Message}");
                }
            }
            return loadedRecords;
        }
    }
}