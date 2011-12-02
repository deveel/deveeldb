using System;

using Deveel.Data.Client;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class VariableTest : TestBase {
		private DeveelDbConnection connection;

		protected override void OnSetUp() {
			connection = CreateConnection();
			connection.AutoCommit = false;
		}

		protected override void OnTearDown() {
			connection.Close();
		}

		[Test]
		public void DeclareVariables() {
			connection.CreateCommand("test_var STRING").ExecuteNonQuery();
			connection.CreateCommand("test_var2 NUMERIC NOT NULL").ExecuteNonQuery();
			connection.CreateCommand("test_var3 CONSTANT VARCHAR(100) = 'test'").ExecuteNonQuery();
		}

		[Test]
		public void SetVariables() {
			connection.CreateCommand("SET test_var = 'test1'").ExecuteNonQuery();
			connection.CreateCommand("SET test_var2 = 245").ExecuteNonQuery();
		}

		[Test]
		public void ShowVariables() {
			object value = connection.CreateCommand("SELECT :test_var").ExecuteScalar();
			Console.Out.WriteLine("test_var = {0}", value);

			value = connection.CreateCommand("SELECT :test_var2").ExecuteScalar();
			Console.Out.WriteLine("test_var2 = {0}", value);

			value = connection.CreateCommand("SELECT :test_var3").ExecuteScalar();
			Console.Out.WriteLine("test_var3 = {0}", value);
		}
	}
}