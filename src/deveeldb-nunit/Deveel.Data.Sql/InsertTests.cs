using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class InsertTests : ContextBasedTest {
		protected override IQueryContext CreateQueryContext(IDatabase database) {
			var context = base.CreateQueryContext(database);

			var tableInfo = new TableInfo(ObjectName.Parse("APP.people"));
			tableInfo.AddColumn("id", PrimitiveTypes.BigInt());
			tableInfo.AddColumn("first_name", PrimitiveTypes.String(), true);
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("age", PrimitiveTypes.TinyInt());

			context.CreateTable(tableInfo);

			return context;
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

			QueryContext.InsertIntoTable(tableName, assignments);
		}
	}
}
