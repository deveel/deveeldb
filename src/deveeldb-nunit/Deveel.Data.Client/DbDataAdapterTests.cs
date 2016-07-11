using System;

using NUnit.Framework;

namespace Deveel.Data.Client {
	[TestFixture]
	public sealed class DbDataAdapterTests {
		[Test]
		public void SelectTests() {
			var adapter = new DeveelDbDataAdapter("SELECT * FROM test_table");
			var command = adapter.SelectCommand;

			Assert.IsNotNull(command);
			Assert.AreEqual("SELECT * FROM test_table", command.CommandText);
		}

		[Test]
		public void UpdateTests() {
			var adapter = new DeveelDbDataAdapter();
			adapter.UpdateCommand = new DeveelDbCommand("UPDATE test_table SET a = 22 WHERE b = 'one'");

			var command = adapter.UpdateCommand;

			Assert.IsNotNull(command);
			Assert.AreEqual("UPDATE test_table SET a = 22 WHERE b = 'one'", command.CommandText);
		}
	}
}
