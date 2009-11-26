using System;

using Deveel.Data.Client;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class CursorTest : TestBase {
		[Test]
		public void Declare() {
			const string commandText = "DECLARE person_cursor CURSOR FOR " +
			                           "SELECT * FROM Person WHERE age > 2 " + 
									   "   ORDER BY name ASC";

			using (DeveelDbConnection connection = CreateConnection()) {
				DeveelDbCommand command = connection.CreateCommand(commandText);
				Assert.AreEqual(1, command.ExecuteNonQuery());
			}
		}

		[Test]
		public void Open() {
			using(DeveelDbConnection connection = CreateConnection()) {
				DeveelDbCommand command = connection.CreateCommand("OPEN person_cursor");
				Assert.AreEqual(1, command.ExecuteNonQuery());
			}
		}

		[Test]
		public void FetchNext() {
			using (DeveelDbConnection connection = CreateConnection()) {
				DeveelDbCommand command = connection.CreateCommand("FETCH NEXT FROM person_cursor");

				using(DeveelDbDataReader reader = command.ExecuteReader()) {
					while (reader.Read()) {
						Console.Out.WriteLine("Name: {0}", reader.GetString(1));
					}
				}
			}
		}

		[Test]
		public void Close() {
			using (DeveelDbConnection connection = CreateConnection()) {
				DeveelDbCommand command = connection.CreateCommand("CLOSE person_cursor");
				Assert.AreEqual(1, command.ExecuteNonQuery());
			}
		}
	}
}