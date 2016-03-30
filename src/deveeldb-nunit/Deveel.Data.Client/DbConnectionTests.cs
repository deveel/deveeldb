// 
//  Copyright 2010-2014 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Data;

using NUnit.Framework;

namespace Deveel.Data.Client {
	[TestFixture]
	public sealed class DbConnectionTests : ContextBasedTest {
		[Test]
		public void OpenForAdmin() {
			var connString = new DeveelDbConnectionStringBuilder {
				UserName = AdminUserName,
				Password = AdminPassword,
				DataSource = "memory",
				Database = DatabaseName,
				Schema = "APP",
				Create = true
			};

			var connection = new DeveelDbConnection(connString);
			Assert.AreEqual(ConnectionState.Closed, connection.State);
			Assert.DoesNotThrow(() => connection.Open());
			Assert.AreEqual(ConnectionState.Open, connection.State);
			Assert.DoesNotThrow(() => connection.Close());
			Assert.AreEqual(ConnectionState.Closed, connection.State);
		}

		[Test]
		public void OpenForAdmin_Embedded() {
			var connection = Database.CreateDbConnection(AdminUserName, AdminPassword);
			Assert.IsNotNull(connection);

			Assert.AreEqual(ConnectionState.Closed, connection.State);
			Assert.DoesNotThrow(() => connection.Open());
			Assert.AreEqual(ConnectionState.Open, connection.State);
			Assert.DoesNotThrow(() => connection.Close());
			Assert.AreEqual(ConnectionState.Closed, connection.State);
		}

		[Test]
		public void QueryScalarForAdmin() {
			var connection = Database.CreateDbConnection(AdminUserName, AdminPassword);
			Assert.IsNotNull(connection);

			IDbCommand command = null;
			Assert.DoesNotThrow(() => command = connection.CreateCommand());
			Assert.IsNotNull(command);

			command.CommandText = "SELECT user()";

			var  value = command.ExecuteScalar();
			Assert.IsNotNull(value);
			Assert.IsInstanceOf<string>(value);
			Assert.AreEqual(AdminUserName, value);

			Assert.DoesNotThrow(() => connection.Dispose());
		}

		[Test]
		public void QueryForAdmin() {
			IDbConnection connection = null;
			Assert.DoesNotThrow(() => connection = Database.CreateDbConnection(AdminUserName, AdminPassword));
			Assert.IsNotNull(connection);

			IDbCommand command = null;
			Assert.DoesNotThrow(() => command = connection.CreateCommand());
			Assert.IsNotNull(command);

			command.CommandText = "SELECT user()";

			IDataReader reader = null;
			Assert.DoesNotThrow(() => reader = command.ExecuteReader());
			Assert.IsNotNull(reader);
			Assert.AreEqual(1, reader.FieldCount);
			Assert.IsTrue(reader.Read());

			object value = null;
			Assert.DoesNotThrow(() => value = reader.GetValue(0));
			Assert.IsInstanceOf<string>(value);
		}
	}
}
