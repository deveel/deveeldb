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

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public class SelectStatementTests : ContextBasedTest {
		protected override IQueryContext CreateQueryContext(IDatabase database) {
			var context = base.CreateQueryContext(database);
			CreateTestTable(context);
			AddTestData(context);
			return context;
		}

		private void CreateTestTable(IQueryContext context) {
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

		private void AddTestData(IQueryContext context) {
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
		public void ParseWithFromClause() {
			const string sql = "SELECT col1 AS a FROM table";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var statement = statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var selectStatement = (SelectStatement) statement;
			Assert.IsNotNull(selectStatement.QueryExpression);
			Assert.IsNull(selectStatement.OrderBy);
		}

		[Test]
		public void ParseWithVariable() {
			const string sql = "SELECT :a";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var statement = statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var selectStatement = (SelectStatement)statement;
			Assert.IsNotNull(selectStatement.QueryExpression);
			Assert.IsNull(selectStatement.OrderBy);

			Assert.IsNotNull(selectStatement.QueryExpression.SelectColumns);

			var selectCols = selectStatement.QueryExpression.SelectColumns.ToList();
			Assert.AreEqual(1, selectCols.Count);
			Assert.IsInstanceOf<SqlVariableReferenceExpression>(selectCols[0].Expression);
		}

		[Test]
		public void ParseWithFunction() {
			const string sql = "SELECT user()";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var statement = statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var selectStatement = (SelectStatement)statement;
			Assert.IsNotNull(selectStatement.QueryExpression);
			Assert.IsNull(selectStatement.OrderBy);

			Assert.IsNotNull(selectStatement.QueryExpression.SelectColumns);

			var selectCols = selectStatement.QueryExpression.SelectColumns.ToList();
			Assert.AreEqual(1, selectCols.Count);
			Assert.IsInstanceOf<SqlFunctionCallExpression>(selectCols[0].Expression);
		}

		[Test]
		public void ParseWithOrderByClause() {
			const string sql = "SELECT col1 AS a FROM table ORDER BY a ASC";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var statement = statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			var selectStatement = (SelectStatement)statement;
			Assert.IsNotNull(selectStatement.QueryExpression);
			Assert.IsNotNull(selectStatement.OrderBy);
		}

		[Test]
		public void ExecuteSimpleSelect() {
			const string sql = "SELECT * FROM test_table";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var statement = statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			ITable result = null;
			Assert.DoesNotThrow(() => result = statement.Evaluate(QueryContext));
			Assert.IsNotNull(result);
			Assert.AreEqual(3, result.RowCount);
		}

		[Test]
		public void ExecuteSimpleOrderedSelect() {
			const string sql = "SELECT * FROM test_table ORDER BY birth_date DESC";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var statement = statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);

			ITable result = null;
			Assert.DoesNotThrow(() => result = statement.Evaluate(QueryContext));
			Assert.IsNotNull(result);
			Assert.AreEqual(3, result.RowCount);

			var firstName = result.GetValue(0, 1);

			Assert.AreEqual("Roger", firstName.Value.ToString());
		}
	}
}
