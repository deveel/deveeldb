using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class InsertTests : ContextBasedTest {
		protected override IUserSession CreateAdminSession(IDatabase database) {
			using (var session = base.CreateAdminSession(database)) {
				using (var query = session.CreateQuery()) {
					var tableInfo = new TableInfo(ObjectName.Parse("APP.people"));
					tableInfo.AddColumn("id", PrimitiveTypes.BigInt());
					tableInfo.AddColumn("first_name", PrimitiveTypes.String(), true);
					tableInfo.AddColumn("last_name", PrimitiveTypes.String());
					tableInfo.AddColumn("age", PrimitiveTypes.TinyInt());

					query.CreateTable(tableInfo);
					query.Commit();
				}
			}

			return base.CreateAdminSession(database);
		}

		[Test]
		public void InsertRegular() {
			var tableName = ObjectName.Parse("APP.people");
			var assignments = new SqlAssignExpression[] {
				SqlExpression.Assign(SqlExpression.Reference(new ObjectName(tableName, "id")),
					SqlExpression.Constant(1)),
				SqlExpression.Assign(SqlExpression.Reference(new ObjectName(tableName, "first_name")),
					SqlExpression.Constant("Antonello")),
				SqlExpression.Assign(SqlExpression.Reference(new ObjectName(tableName, "last_name")),
					SqlExpression.Constant("Provenzano"))
			};

			Query.InsertIntoTable(tableName, assignments);
		}
	}
}
