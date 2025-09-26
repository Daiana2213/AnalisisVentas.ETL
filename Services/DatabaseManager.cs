using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace AnalisisVentas.ETL.Services
{
    public class DatabaseManager
    {
        private readonly string connectionString = "Server=DAIANA-LT\\SQLEXPRESS;Database=AnalisisVentasDO;Integrated Security=True;";

        public SqlConnection GetConnection()
        {
            return new SqlConnection(connectionString);
        }
        public int ExecuteNonQuery(string sql, params SqlParameter[] parameters)
        {
            using (var conn = GetConnection())
            {
                conn.Open();
                using (var cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddRange(parameters);
                    return cmd.ExecuteNonQuery();
                }
            }
        }
        public object ExecuteScalar(string sql, params SqlParameter[] parameters)
        {
            using (var conn = GetConnection())
            {
                conn.Open();
                using (var cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddRange(parameters);
                    return cmd.ExecuteScalar();
                }
            }
        }
        public void TestConnection()
        {
            try
            {
                using (var conn = GetConnection())
                {
                    conn.Open();
                    Console.WriteLine("Conexión a la base de datos exitosa.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("¡ERROR DE CONEXIÓN A LA BASE DE DATOS!");
                Console.WriteLine($"Detalles: {ex.Message}");
            }
        }
    }
}
