
using AnalisisVentas.ETL.Class;
using CsvHelper;
using System.Data.SqlClient;
using System.Globalization;
using System.Collections.Generic;
using System.Linq;
using System;

namespace AnalisisVentas.ETL.Services
{
    public class CustomersService
    {
        private readonly DatabaseManager _dbManager = new DatabaseManager();

        public List<Customer> ExtractAndTransform(string filePath)
        {
            List<Customer> customers;

            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                customers = csv.GetRecords<Customer>().ToList();
            }
            Console.WriteLine($"\nClientes extraídos del CSV: {customers.Count}");

            //OJO TRANSFORMACIÓN
            var distinctCustomers = customers
                .GroupBy(c => c.CustomerID)
                .Select(g => g.First()) 
                .ToList();
            Console.WriteLine($"Clientes deduplicados en memoria: {distinctCustomers.Count}");

            //OJO Filtrado
            var validCustomers = distinctCustomers
                .Where(c => c.CustomerID > 0) 
                .Where(c => !string.IsNullOrWhiteSpace(c.FirstName)) 
                .Where(c => !string.IsNullOrWhiteSpace(c.LastName))  
                                                                     
                .ToList();
            Console.WriteLine($"Clientes válidos después de limpieza: {validCustomers.Count}");

            return validCustomers;
        }

        public int Load(List<Customer> customers)
        {

            const string sqlUpsert = @"
                MERGE Customers AS target
                USING (SELECT @CustomerID AS CustomerID, @FirstName AS FirstName, @LastName AS LastName, 
                              @Email AS Email, @Phone AS Phone, @City AS City, @Country AS Country) AS source
                ON (target.CustomerID = source.CustomerID)
                WHEN MATCHED THEN
                    UPDATE SET 
                        FirstName = source.FirstName,
                        LastName = source.LastName,
                        Email = source.Email,
                        Phone = source.Phone,
                        City = source.City,
                        Country = source.Country
                WHEN NOT MATCHED THEN
                    INSERT (CustomerID, FirstName, LastName, Email, Phone, City, Country)
                    VALUES (source.CustomerID, source.FirstName, source.LastName, source.Email, source.Phone, source.City, source.Country);";

            int loadedRecords = 0;
            foreach (var customer in customers)
            {
                try
                {
                    var parameters = new[]
                    {
                        new SqlParameter("@CustomerID", customer.CustomerID),
                        new SqlParameter("@FirstName", customer.FirstName),
                        new SqlParameter("@LastName", customer.LastName),
                        new SqlParameter("@Email", (object)customer.Email ?? DBNull.Value),
                        new SqlParameter("@Phone", (object)customer.Phone ?? DBNull.Value),
                        new SqlParameter("@City", (object)customer.City ?? DBNull.Value),
                        new SqlParameter("@Country", (object)customer.Country ?? DBNull.Value)
                    };

                    _dbManager.ExecuteNonQuery(sqlUpsert, parameters);
                    loadedRecords++;
                }
                catch (SqlException ex)
                {
                    Console.WriteLine($"Error al cargar/actualizar cliente ID {customer.CustomerID}: {ex.Message}");
                }
            }
            return loadedRecords;
        }
    }
}