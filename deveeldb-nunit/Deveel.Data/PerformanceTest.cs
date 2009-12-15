using System;

using Deveel.Data.Client;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public class PerformanceTest : TestBase {
		protected override void OnSetUp() {
			DeveelDbConnection connection = CreateConnection();
			DeveelDbCommand command = connection.CreateCommand();
			command.CommandText = "CREATE TABLE IF NOT EXISTS TestTable (" +
			                      "    id IDENTITY," +
			                      "    inc INTEGER," +
			                      "    var_name VARCHAR(200)" +
			                      ")";
			command.ExecuteNonQuery();
		}

		protected override void OnTearDown() {
			DeveelDbConnection connection = CreateConnection();
			DeveelDbCommand command = connection.CreateCommand("DROP TABLE TestTable");
			command.ExecuteNonQuery();
		}

		[Test]
		public void Insert1000RowsAndCount_NonCommit() {
			DeveelDbConnection conn = CreateConnection();

			// we deisable the AUTO_COMMIT flag for such this operation
			DeveelDbTransaction transaction = conn.BeginTransaction();

			DeveelDbCommand command;

			DateTime start = DateTime.Now;

			for (int i = 0; i < 1000; i++) {
				command = conn.CreateCommand();
				command.CommandText = "INSERT INTO TestTable (inc, var_name) VALUES (?, ?)";
				command.Parameters.Add(i);
				command.Parameters.Add("var_" + i);
				command.ExecuteNonQuery();
			}

			DateTime end = DateTime.Now;
			Console.Out.WriteLine("Inserted 1000 rows in {0}ms.", (end - start).TotalMilliseconds);

			command = conn.CreateCommand("SELECT COUNT(*) FROM TestTable");
			long count = (long) command.ExecuteScalar();

			Assert.AreEqual(1000, count, "Not all the rows were inserted");

			start = DateTime.Now;

			transaction.Rollback();

			end = DateTime.Now;

			Console.Out.WriteLine("Rolled-back in {0}", (end - start));

			command = conn.CreateCommand("SELECT COUNT(*) FROM TestTable");
			count = (long)command.ExecuteScalar();

			Assert.AreEqual(0, count, "After rollback there shouldn't be any rows in the table.");
		}

		[Test]
		public void Insert1000RowsAndCount_Commit() {
			DeveelDbConnection conn = CreateConnection();

			// we deisable the AUTO_COMMIT flag for such this operation
			DeveelDbTransaction transaction = conn.BeginTransaction();

			DeveelDbCommand command = conn.CreateCommand();;

			DateTime start = DateTime.Now;

			for (int i = 0; i < 1000; i++) {
				command.CommandText = "INSERT INTO TestTable (inc, var_name) VALUES (?, ?)";
				command.Parameters.Add(i);
				command.Parameters.Add("var_" + i);
				command.ExecuteNonQuery();
			}

			DateTime end = DateTime.Now;
			Console.Out.WriteLine("Inserted 1000 rows in {0}ms.", (end - start));

			start = DateTime.Now;

			transaction.Commit();

			end = DateTime.Now;

			Console.Out.WriteLine("Committed in {0}ms.", (end - start));

			command = conn.CreateCommand("SELECT COUNT(*) FROM TestTable");
			long count = (long)command.ExecuteScalar();

			Console.Out.WriteLine("Numer of rows inserted into TestTable : {0}", count);
		}
	}
}