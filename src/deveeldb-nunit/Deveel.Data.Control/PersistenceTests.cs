using System;
using System.Data;

using Deveel.Data.Configuration;
using Deveel.Diagnostics;

using NUnit.Framework;

namespace Deveel.Data.Control {
    [TestFixture]
    public class PersistenceTests {
		const string testDbName = "testdb";
		const string testDbAdmin = "SA";
		const string testDbPass = "1234";

	    private IDbConfig Config;

		[SetUp]
	    public void SetUp() {
			Config = DbConfig.Default;
			Config.StorageSystem(ConfigDefaultValues.FileStorageSystem);
			Config.LoggerType(typeof (ConsoleLogger));

			using (var controller = DbController.Create(Config)) {
				if (controller.DatabaseExists(testDbName))
					controller.DeleteDatabase(testDbName, testDbAdmin, testDbPass);
			}
		}

		[Test]
		[Category("Create")]
	    public void FullCycle() {
			// First create the controller for the system context
			using (var controller = DbController.Create(Config)) {
				Console.Out.WriteLine("A new system controller was created at path {0}", controller.Config.BasePath());

				// Then create the database 'testdb'
				using (var dbSystem = controller.CreateDatabase(testDbName, testDbAdmin, testDbPass)) {
					Console.Out.WriteLine("The database {0} was created at the path {1}",
						dbSystem.Config.DatabaseName(),
						dbSystem.Config.DatabaseFullPath());

					Assert.IsTrue(dbSystem.Database.Exists, "The database was created but was not not physically found.");

					// now open am ADO.NET connection to write data to the database
					using (var connection = dbSystem.GetConnection(testDbAdmin, testDbPass)) {
						Assert.IsTrue(connection.State == ConnectionState.Open, "The connection is not open");

						using (var transaction = connection.BeginTransaction()) {
							// Create the table 'people'
							var command = connection.CreateCommand();
							command.CommandText = "CREATE TABLE people (first_name VARCHAR(255), last_name VARCHAR(255), age INT)";
							command.ExecuteNonQuery();

							Console.Out.WriteLine("The table 'people' was created in the database");

							// insert an entry into the table
							command = connection.CreateCommand();
							command.CommandText = "INSERT INTO people (first_name, last_name, age) VALUES ('Antonello', 'Provenzano', 33)";
							command.ExecuteNonQuery();

							Console.Out.WriteLine("An entry was inserted into the table 'people'.");

							// assert the entry exists within this context
							command = connection.CreateCommand();
							command.CommandText = "SELECT COUNT(*) FROM people";

							var count = (BigNumber) command.ExecuteScalar();
							Assert.AreEqual(1, count.ToInt32(), "The number of entries in the table is not coherent.");

							transaction.Commit();
						}
					}
				}
			}

			// The previous system context was disposed, any reference to the databases within the
			// context has been released. So create another system controller
			using (var controller = DbController.Create(Config)) {
				// Check the database physically exists in the system
				Assert.IsTrue(controller.DatabaseExists(testDbName), "The database {0} was not physically found at the path {1}", testDbName, Config.BasePath());
			}

			// Open another system context that is isolated from the previous ones.
			using (var controller = DbController.Create(Config)) {
				// Open an existing database within the system context. If the database doesn't exist
				// this will throw an exception.
				using (var dbSystem = controller.StartDatabase(testDbName)) {
					// Open a connection to the database
					using (var connection = dbSystem.GetConnection(testDbAdmin, testDbPass)) {
						// Assert the connection state is open
						Assert.IsTrue(connection.State == ConnectionState.Open, "The connection is not open");

						// Check the 'people' table and count the items. If the table doesn't physically
						// exist this will throw an exception
						var command = connection.CreateCommand();
						command.CommandText = "SELECT COUNT(*) FROM people";

						// Assert there is exactly one element in the table
						var count = (BigNumber) command.ExecuteScalar();
						Assert.AreEqual(1, count.ToInt32(), "An incorrect number of items was found in the table.");

						// Now select the entry in the table
						command = connection.CreateCommand();
						command.CommandText = "SELECT * FROM people";

						// Assert the data structure is coherent with the one created in
						// the previous passage
						var reader = command.ExecuteReader();
						Assert.AreEqual(3, reader.FieldCount, "An incorrect number of fields was found in the table.");
						Assert.AreEqual(0, reader.GetOrdinal("first_name"), "The first field in the table is not 'first_name'");
						Assert.AreEqual(1, reader.GetOrdinal("last_name"), "The second field in the table is not 'last_name'");
						Assert.AreEqual(2, reader.GetOrdinal("age"), "The third field in the table is not 'age'");

						// Assert at least one entry can be read
						Assert.IsTrue(reader.Read(), "It was not possible to read from the result");

						// Assert the entry read is exactly the one created in the previous stage
						Assert.AreEqual("Antonello", reader.GetString(0), "The value of 'first_name' is not 'Antonello'");
						Assert.AreEqual("Provenzano", reader.GetString(1), "The value of 'last_name' is not 'Provenzano'");
						Assert.AreEqual(33, reader.GetInt32(2), "The value of 'age' is not 33");
					}
				}
			}
		}
    }
}