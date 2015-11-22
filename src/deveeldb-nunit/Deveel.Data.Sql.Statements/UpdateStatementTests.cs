using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public class UpdateStatementTests : ContextBasedTest {
		protected override IQuery CreateQuery(IUserSession session) {
			var query = base.CreateQuery(session);
			CreateTestTable(query);
			AddTestData(query);
			return query;
		}

		private void CreateTestTable(IQuery context) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUE_KEY",
				new SqlExpression[] { SqlExpression.Reference(tableInfo.TableName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			context.CreateTable(tableInfo);
			context.AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");
		}

		private void AddTestData(IQuery context) {
			var table = context.GetMutableTable(ObjectName.Parse("APP.test_table"));
			var row = table.NewRow();
			row.SetValue("id", DataObject.Integer(0));
			row.SetValue("first_name", DataObject.String("John"));
			row.SetValue("last_name", DataObject.String("Doe"));
			row.SetValue("birth_date", DataObject.Date(new SqlDateTime(1977, 01, 01)));
			row.SetValue("active", DataObject.Boolean(false));
			table.AddRow(row);

			row = table.NewRow();
			row.SetValue("id", DataObject.Integer(1));
			row.SetValue("first_name", DataObject.String("Jane"));
			row.SetValue("last_name", DataObject.String("Doe"));
			row.SetValue("birth_date", DataObject.Date(new SqlDateTime(1978, 11, 01)));
			row.SetValue("active", DataObject.Boolean(true));
			table.AddRow(row);

			row = table.NewRow();
			row.SetValue("id", DataObject.Integer(2));
			row.SetValue("first_name", DataObject.String("Roger"));
			row.SetValue("last_name", DataObject.String("Rabbit"));
			row.SetValue("birth_date", DataObject.Date(new SqlDateTime(1985, 05, 05)));
			row.SetValue("active", DataObject.Boolean(true));
			table.AddRow(row);
		}

		[Test]
		public void ParseSimpleUpdate() {
			const string sql = "UPDATE table SET col1 = 'testUpdate', col2 = 22 WHERE id = 1";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var statement = statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<UpdateStatement>(statement);
		}

		[Test]
		public void ParseSimpleUpdateWithLimit() {
			const string sql = "UPDATE table SET col1 = 'testUpdate', col2 = 22 WHERE id = 1 LIMIT 20";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var statement = statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<UpdateStatement>(statement);
		}
	}
}
