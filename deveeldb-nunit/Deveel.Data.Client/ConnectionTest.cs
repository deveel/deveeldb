using System;
using System.Data;

using NUnit.Framework;

namespace Deveel.Data.Client {
	[TestFixture]
	public sealed class ConnectionTest {
		[Test]
		public void ConnectToLocal() {
			const string connString = "Host=Local;User=SA;Password=123456;CreateOrBoot=true;Database=testdb";
			DeveelDbConnection connection = null;

			try {
				connection = new DeveelDbConnection(connString);
				connection.Open();

				Assert.IsTrue(connection.State == ConnectionState.Open);
			} finally {
				if (connection != null) {
					connection.Close();
					Assert.IsTrue(connection.State == ConnectionState.Closed);
				}
			}
		}
	}
}