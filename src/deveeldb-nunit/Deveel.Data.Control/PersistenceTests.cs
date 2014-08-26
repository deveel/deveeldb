using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Deveel.Data.Configuration;
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

			var controller = DbController.Create(Config);
			if (controller.DatabaseExists(testDbName))
				controller.DeleteDatabase(testDbName, testDbAdmin, testDbPass);
		}

		[Test]
		[Category("Create")]
	    public void FullCycle() {
			using (var controller = DbController.Create(Config)) {

				using (var dbSystem = controller.CreateDatabase(testDbName, testDbAdmin, testDbPass)) {
					Assert.IsTrue(dbSystem.Database.Exists);

					using (var connection = dbSystem.GetConnection(testDbAdmin, testDbPass)) {
						Assert.IsTrue(connection.State == ConnectionState.Open);

						var command = connection.CreateCommand();
						command.CommandText = "CREATE TABLE people (first_name VARCHAR(255), last_name VARCHAR(255), age INT)";
						command.ExecuteNonQuery();

						command = connection.CreateCommand();
						command.CommandText = "INSERT INTO people (first_name, last_name, age) VALUES ('Antonello', 'Provenzano', 33)";
						command.ExecuteNonQuery();

						command = connection.CreateCommand();
						command.CommandText = "SELECT COUNT(*) FROM people";

						var count = (BigNumber) command.ExecuteScalar();
						Assert.AreEqual(1, count.ToInt32());
					}
				}
			}

			using (var controller = DbController.Create(Config)) {
				Assert.IsTrue(controller.DatabaseExists(Config, testDbName));
			}

			using (var controller = DbController.Create(Config)) {
				using (var dbSystem = controller.StartDatabase(testDbName)) {
					using (var connection = dbSystem.GetConnection(testDbAdmin, testDbPass)) {
						Assert.IsTrue(connection.State == ConnectionState.Open);

						var command = connection.CreateCommand();
						command.CommandText = "SELECT COUNT(*) FROM people";

						var count = (BigNumber) command.ExecuteScalar();
						Assert.AreEqual(1, count.ToInt32());
					}
				}
			}
		}
    }
}