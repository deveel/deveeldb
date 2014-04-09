using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Deveel.Data.Client;

namespace ddbperftest {
	class Program {
		static void Main(string[] args) {
			Console.Out.WriteLine("DeveelDB Performance Tests");
			Console.Out.WriteLine("--------------------------");
			Console.Out.WriteLine();

			var dbPath = Path.Combine(Environment.CurrentDirectory, "./test");

			if (Directory.Exists(dbPath)) {
				Console.Out.WriteLine("Path {0} already exists: deleting ...", dbPath);
				Directory.Delete(dbPath, true);
			}

			Console.Out.WriteLine("Creating Database: {0}", dbPath);

			var connection = CreateConnection(dbPath);
			connection.Open();

			try {
				Console.Out.WriteLine("Warming up ...");
				WarmUp(connection);

				Console.Out.WriteLine("Running tests");
				RunInsert(connection, 5000);
				RunQuery(connection, 500);
			} catch (Exception e) {
				Console.Error.WriteLine("An error occurred while executing tests: {0}", e.Message);
				Console.Error.WriteLine(e.StackTrace);
			} finally {
				connection.Close();
			}

			Console.In.Read();
		}

		private static DeveelDbConnection CreateConnection(string dbPath) {
			var connStringBuilder = new DeveelDbConnectionStringBuilder {
				Host = "Local",
				Database = "Test",
				Create = true,
				UserName = "SA",
				Password = "12345%abc",
				Path = dbPath
			};

			return new DeveelDbConnection(connStringBuilder.ConnectionString);
		}

		private static void WarmUp(DeveelDbConnection connection) {
			var command = connection.CreateCommand("CREATE TABLE TestData (id INTEGER NOT NULL, data VARCHAR NOT NULL, d DATE)");
			command.ExecuteNonQuery();
		}

		private static void CleanUp(DeveelDbConnection connection) {
			var command = connection.CreateCommand("DROP TABLE IF EXISTS TestData");
			command.ExecuteNonQuery();
		}

		private static void RunInsert(DeveelDbConnection connection, int total) {
			Console.Out.WriteLine("Inserting {0} entries", total);

			DateTime start = DateTime.Now;

			for (int i = 0; i < total; i++) {
				var sb = new StringBuilder("INSERT INTO TestData (id, data, d) VALUES (");
				sb.AppendFormat("{0:D}, 'data{0:D5}', '{1}'", i, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
				sb.Append(")");

				var command = connection.CreateCommand(sb.ToString());
				command.ExecuteNonQuery();
			}

			TimeSpan elapsed = DateTime.Now.Subtract(start);
			Console.Out.WriteLine("The test inserted {0} in {1:c}", total, elapsed);
		}

		private static void RunQuery(DeveelDbConnection connection, int total) {
			Console.Out.WriteLine("Selecting {0} entries", total);

			DateTime start = DateTime.Now;

			for (int i = 0; i < total; i++) {
				var s = String.Format("SELECT * FROM TestData WHERE id = {0}", i);
				var command = connection.CreateCommand(s);
				using (var reader = command.ExecuteReader()) {
					reader.Read();
				}
			}

			TimeSpan elapsed = DateTime.Now.Subtract(start);
			Console.Out.WriteLine("The test selected {0} in {1:c}", total, elapsed);
		}
	}
}
