using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Deveel.Data {
	[TestFixture]
	public class SelectCompositeTests : ContextBasedTest {
		protected override void OnAfterSetup(string testName) {
			CreateTestTable(Query);
			AddTestData(Query);
		}

		private static void CreateTestTable(IQuery context) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableInfo.TableName.FullName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			context.Session.Access().CreateTable(tableInfo);
			context.Session.Access().AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");
		}

		private static void AddTestData(IQuery context) {
			var table = context.Access().GetMutableTable(ObjectName.Parse("APP.test_table"));
			var row = table.NewRow();

			// row.SetValue("id", Field.Integer(0));
			row.SetDefault(0, context);
			row.SetValue("first_name", Field.String("John"));
			row.SetValue("last_name", Field.String("Doe"));
			row.SetValue("birth_date", Field.Date(new SqlDateTime(1977, 01, 01)));
			row.SetValue("active", Field.Boolean(false));
			table.AddRow(row);

			row = table.NewRow();

			// row.SetValue("id", Field.Integer(1));
			row.SetDefault(0, context);
			row.SetValue("first_name", Field.String("Jane"));
			row.SetValue("last_name", Field.String("Doe"));
			row.SetValue("birth_date", Field.Date(new SqlDateTime(1978, 11, 01)));
			row.SetValue("active", Field.Boolean(true));
			table.AddRow(row);

			row = table.NewRow();

			// row.SetValue("id", Field.Integer(2));
			row.SetDefault(0, context);
			row.SetValue("first_name", Field.String("Roger"));
			row.SetValue("last_name", Field.String("Rabbit"));
			row.SetValue("birth_date", Field.Date(new SqlDateTime(1985, 05, 05)));
			row.SetValue("active", Field.Boolean(true));
			table.AddRow(row);
		}

		private ITable Execute(string s) {
			var query = (SqlQueryExpression)SqlExpression.Parse(s);
			var result = Query.Select(query);
			result.GetEnumerator().MoveNext();
			return result.Source;
		}

		[Test]
		public void SelectUnionAll() {
			var result =
				Execute("SELECT first_name, last_name FROM test_table WHERE test_table.active = true UNION ALL SELECT first_name, last_name FROM test_table WHERE first_name = 'John'");

			Assert.IsNotNull(result);
		}
	}
}
