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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class SelectIntoTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			CreateTables(query);
			InsertTestData(query);
			return true;
		}

		private void CreateTables(IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String());

			query.Access().CreateTable(tableInfo);

			tableName = ObjectName.Parse("APP.dest_table");
			tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String());

			query.Access().CreateTable(tableInfo);
		}

		private void InsertTestData(IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			var table = query.Access().GetMutableTable(tableName);

			var row = table.NewRow();
			row.SetValue(0, 13);
			row.SetValue(1, "test1");
			table.AddRow(row);

			row = table.NewRow();
			row.SetValue(0, 38);
			row.SetValue(1, "greetings");
			table.AddRow(row);
		}

		protected override void OnAfterSetup(string testName) {
			AdminQuery.Context.DeclareVariable("a", PrimitiveTypes.String());
			AdminQuery.Context.DeclareVariable("b", PrimitiveTypes.Integer());
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			query.DropTable(tableName);
			query.DropTable(ObjectName.Parse("APP.dest_table"));
			return true;
		}

		[Test]
		public void OneColumnIntoOneVariable() {
			var query = (SqlQueryExpression) SqlExpression.Parse("SELECT a FROM test_table");
			AdminQuery.SelectInto(query, "b");

			var variable = AdminQuery.Context.FindVariable("b");

			Assert.IsNotNull(variable);
			Assert.IsInstanceOf<NumericType>(variable.Type);
			Assert.IsFalse(variable.Evaluate(AdminQuery).IsNull);
			Assert.IsInstanceOf<SqlNumber>(variable.Evaluate(AdminQuery).Value);

			var number = (SqlNumber) variable.Evaluate(AdminQuery).Value;
			Assert.AreEqual(new SqlNumber(13), number);
		}

		[Test]
		public void TwoColumnsIntoTwoVariables() {
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT a, b FROM test_table");
			AdminQuery.SelectInto(query, "b", "a");

			var variable = AdminQuery.Context.FindVariable("b");

			Assert.IsNotNull(variable);
			Assert.IsInstanceOf<NumericType>(variable.Type);
			Assert.IsFalse(variable.Evaluate(AdminQuery).IsNull);
			Assert.IsInstanceOf<SqlNumber>(variable.Evaluate(AdminQuery).Value);

			var number = (SqlNumber)variable.Evaluate(AdminQuery).Value;
			Assert.AreEqual(new SqlNumber(13), number);
		}

		[Test]
		public void TwoColumnsIntoTable() {
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT a, b FROM test_table");
			AdminQuery.SelectInto(query, ObjectName.Parse("APP.dest_table"));

			var table = AdminQuery.Access().GetTable(ObjectName.Parse("APP.dest_table"));

			Assert.AreEqual(2, table.RowCount);
		}
	}
}
