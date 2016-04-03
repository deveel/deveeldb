using System;
using System.Collections.Generic;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class InsertTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			CreateTestTable(Query);
		}

		private static void CreateTestTable(IQuery query) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableInfo.TableName.FullName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			query.Session.Access().CreateTable(tableInfo);
			query.Session.Access().AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");
		}

		[Test]
		public void TwoValues() {
			var tableName = ObjectName.Parse("APP.test_table");
			var columns = new[] { "first_name", "last_name", "active" };
			var values = new List<SqlExpression[]> {
				new[] {
					SqlExpression.Constant("Antonello"),
					SqlExpression.Constant("Provenzano"),
					SqlExpression.Constant(true)
				},
				new [] {
					SqlExpression.Constant("Mart"),
					SqlExpression.Constant("Roosmaa"),
					SqlExpression.Constant(false)
				}
			};

			var count = Query.Insert(tableName, columns, values.ToArray());


			Assert.AreEqual(2, count);

			var table = Query.Access().GetTable(tableName);

			Assert.IsNotNull(table);
			Assert.AreEqual(2, table.RowCount);
		}

	}
}
