using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public class AlterTableStatementTests : ContextBasedTest {
		protected override void OnSetUp() {
			CreateTestTable();
		}

		private void CreateTestTable() {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUE_KEY",
				new SqlExpression[] { SqlExpression.Reference(tableInfo.TableName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			QueryContext.CreateTable(tableInfo);
			QueryContext.AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");
		}

		[Test]
		public void AlterTableAddColumn() {
			const string sql = "ALTER TABLE test_table ADD COLUMN reserved BOOLEAN";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var list = statements.ToList();

			Assert.AreEqual(1, list.Count);

			var statement = list[0];

			Assert.IsNotNull(statement);
		}
	}
}
