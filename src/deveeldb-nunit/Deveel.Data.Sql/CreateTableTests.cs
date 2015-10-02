using System;

using Deveel.Data;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class CreateTableTests : ContextBasedTest {
		[Test]
		public void CreateSimpleTableAsAdmin_NewSession() {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("a", PrimitiveTypes.Integer(), true);
			tableInfo.AddColumn("b", PrimitiveTypes.String());

			using (var session = Database.CreateUserSession(AdminUserName, AdminPassword)) {
				using (var context = new SessionQueryContext(session)) {
					Assert.DoesNotThrow(() => {
						context.CreateTable(tableInfo);
					});
				}

				Assert.DoesNotThrow(() => session.Commit());
			}

			using (var session = Database.CreateUserSession(AdminUserName, AdminPassword)) {
				using (var context = new SessionQueryContext(session)) {
					bool exists = false;
					Assert.DoesNotThrow(() => exists = context.TableExists(tableName));
					Assert.IsTrue(exists);
				}
			}
		}
	}
}
