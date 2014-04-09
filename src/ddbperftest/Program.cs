using System;
using System.IO;
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
				using (var tnx = connection.BeginTransaction()) {
					RunInsert(connection, tnx, 5000);
					tnx.Commit();
				}

				Console.Out.WriteLine();

				RunQuery(connection, 500);
			} catch (Exception e) {
				Console.Error.WriteLine("An error occurred while executing tests: {0}", e.Message);
				Console.Error.WriteLine(e.StackTrace);
			} finally {
				CleanUp(connection);
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
			var command = connection.CreateCommand("CREATE TABLE TestData (id INTEGER NOT NULL, data VARCHAR NOT NULL, d TIMESTAMP)");
			command.ExecuteNonQuery();
		}

		private static void CleanUp(DeveelDbConnection connection) {
			var command = connection.CreateCommand("DROP TABLE IF EXISTS TestData");
			command.ExecuteNonQuery();
		}

		private static void RunInsert(DeveelDbConnection connection, DeveelDbTransaction transaction, int total) {
			Console.Out.WriteLine("Inserting {0} entries", total);

			DateTime start = DateTime.Now;

			for (int i = 0; i < total; i++) {
				var sb = new StringBuilder("INSERT INTO TestData (id, data, d) VALUES (");
				sb.AppendFormat("{0:D}, 'data{0:D5}', '{1}'", i, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
				sb.Append(")");

				var command = connection.CreateCommand(sb.ToString());
				command.Transaction = transaction;
				command.ExecuteNonQuery();

				var perc = i == 0 ? 0.0 : ((double) i/total)*100;
				Console.Out.Write("\r  {0} ({1}%)", i, perc);
			}

			Console.Out.Write("\r  {0} (100%)", total);
			Console.Out.WriteLine();

			TimeSpan elapsed = DateTime.Now.Subtract(start);
			TimeSpan avgPerOp = new TimeSpan(elapsed.Ticks / total);
			Console.Out.WriteLine("The test inserted {0} in {1} ({2} per op)", total, elapsed, avgPerOp);
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

				var perc = i == 0 ? 0.0 : ((double)i / total) * 100;
				Console.Out.Write("\r  {0} ({1}%)", i, perc);
			}

			Console.Out.Write("\r  {0} (100%)", total);
			Console.Out.WriteLine();

			TimeSpan elapsed = DateTime.Now.Subtract(start);
			TimeSpan avgPerOp = new TimeSpan(elapsed.Ticks / total);
			Console.Out.WriteLine("The test selected {0} in {1} ({2} per op)", total, elapsed, avgPerOp);
		}
	}
}
