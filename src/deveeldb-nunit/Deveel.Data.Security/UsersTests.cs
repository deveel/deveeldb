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
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;

using Deveel.Data.Client;
using Deveel.Data.Sql;

using NUnit.Framework;

namespace Deveel.Data.Security {
	[TestFixture]
	public class UsersTests : SqlTestBase {
		private UserGrantInfo[] GetUserGrants(string userName) {
			var dataTable = Connection.GetSchema(DeveelDbMetadataSchemaNames.UserPrivileges, new []{userName});
			if (dataTable.Rows.Count == 0)
				return new UserGrantInfo[0];

			var grants = new List<UserGrantInfo>();

			foreach (System.Data.DataRow row in dataTable.Rows) {
				grants.Add(new UserGrantInfo {
					Grantee = row["GRANTEE"].ToString(),
					Granter = row["GRANTER"].ToString(),
					Privileges = row["PRIVS"].ToString(),
					ObjectName = row["OBJECT_NAME"].ToString(),
					ObjectType = row["OBJECT_TYPE"].ToString(),
					IsGrantable = (bool)row["IS_GRANTABLE"]
				});
			}

			return grants.ToArray();
		}

		private bool HasPrivilege(string userName, string objectName, string priv) {
			return HasPrivileges(userName, objectName, new[] {priv});
		}

		private bool HasPrivileges(string userName, string objectName, IEnumerable<string> userPrivs) {
			var grants = GetUserGrants(userName);
			if (grants.Length == 0)
				return false;

			foreach (var grant in grants) {
				if (!String.Equals(objectName, grant.ObjectName, StringComparison.OrdinalIgnoreCase))
					continue;

				var privs = grant.Privileges.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
				if (userPrivs.All(x => privs.Any(y => String.Equals(x, y, StringComparison.OrdinalIgnoreCase))))
					return true;
			}

			return false;			
		}

		private bool HasGrantOption(string userName, string objectName, string priv) {
			var grants = GetUserGrants(userName);
			if (grants.Length == 0)
				return false;

			foreach (var grant in grants) {
				if (!String.Equals(objectName, grant.ObjectName, StringComparison.OrdinalIgnoreCase))
					continue;

				var privs = grant.Privileges.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
				if (privs.Any(x => String.Equals(x, priv, StringComparison.OrdinalIgnoreCase)))
					return grant.IsGrantable;
			}

			return false;						
		}

		private bool UserExists(string userName) {
			return (BigNumber)ExecuteScalar("SELECT COUNT(*) FROM SYSTEM.password WHERE UserName = '" + userName + "'") == 1;
		}

		protected override void OnTestSetUp() {
			base.OnTestSetUp();

			var testName = TestContext.CurrentContext.Test.Name;
			if (testName != "CreateUser") {
				ExecuteNonQuery("CREATE USER tester SET PASSWORD '93884£$'");
			}
			if (testName == "RevokeGrants") {
				ExecuteNonQuery("GRANT ALL ON APP.Person TO tester");
			} else if (testName == "AlterTableWithNoGrants") {
				ExecuteNonQuery("GRANT SELECT ON APP.Person TO tester");
			}
		}

		protected override void OnTestTearDown() {
			var testName = TestContext.CurrentContext.Test.Name;
			if (testName != "DropUser") {
				ExecuteNonQuery("DROP USER tester");
			}

			base.OnTestTearDown();
		}

		[Test]
		public void CreateUser() {
			ExecuteNonQuery("CREATE USER tester SET PASSWORD '93884£$'");
			Assert.IsTrue(UserExists("tester"));
		}

		[Test]
		public void GrantUser() {
			ExecuteNonQuery("GRANT SELECT ON App.Person TO tester");
			Assert.IsTrue(HasPrivilege("tester", "APP.Person", "SELECT"));
		}

		[Test]
		public void GrantUserWithOptions() {
			ExecuteNonQuery("GRANT SELECT, INSERT, UPDATE ON APP.Person TO tester WITH GRANT OPTION");
			Assert.IsTrue(HasPrivileges("tester", "APP.Person", new[]{"SELECT", "INSERT", "UPDATE"}));
			Assert.IsTrue(HasGrantOption("tester", "APP.Person", "INSERT"));
		}

		[Test]
		public void RevokeGrants() {
			ExecuteNonQuery("REVOKE SELECT, INSERT ON APP.Person FROM tester");
			Assert.IsTrue(HasPrivileges("tester", "APP.Person", new[]{"DELETE", "UPDATE"}));
		}

		[Test]
		public void AlterTableWithNoGrants() {
			using (var conn = CreateDbConnection("tester", "93884£$")) {
				var command = conn.CreateCommand("ALTER TABLE Person ADD COLUMN Foo INT NOT NULL");
				Assert.Throws<DeveelDbException>(() => command.ExecuteNonQuery());
			}
		}

		[Test]
		public void DropUser() {
			ExecuteNonQuery("DROP USER tester");
			Assert.IsFalse(UserExists("tester"));
		}

		#region UserGrantInfo

		class UserGrantInfo {
			public string Grantee;
			public string Granter;
			public string ObjectType;
			public string ObjectName;
			public bool IsGrantable;
			public string Privileges { get; set; }
		}

		#endregion
	}
}