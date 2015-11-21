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

using Deveel.Data;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class JoinTableTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			CreateTestTables();
			AddTestData();
		}

		private void AddTestData() {
			var table = QueryContext.GetMutableTable(new ObjectName(new ObjectName("APP"), "persons"));

			var row = table.NewRow();
			row["person_id"] = DataObject.Integer(1);
			row["name"] = DataObject.String("Antonello Provenzano");
			row["age"] = DataObject.Integer(34);
			table.AddRow(row);

			row = table.NewRow();
			row["person_id"] = DataObject.Integer(3);
			row["name"] = DataObject.String("John Doe");
			row["age"] = DataObject.Integer(56);
			table.AddRow(row);

			table = QueryContext.GetMutableTable(new ObjectName(new ObjectName("APP"), "codes"));
			row = table.NewRow();
			row["person_id"] = DataObject.Integer(1);
			row["code"] = DataObject.String("123456");
			row["registered"] = DataObject.Date(new SqlDateTime(2014, 01, 12));
			table.AddRow(row);
		}

		private void CreateTestTables() {
			var tableInfo = CreateFirstTable();
			QueryContext.CreateTable(tableInfo);
			QueryContext.AddPrimaryKey(tableInfo.TableName, "person_id");

			tableInfo = CreateSecondTable();
			QueryContext.CreateTable(tableInfo);
		}

		private TableInfo CreateSecondTable() {
			var tableInfo = new TableInfo(new ObjectName(new ObjectName("APP"), "codes"));
			tableInfo.AddColumn("person_id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("code", PrimitiveTypes.String());
			tableInfo.AddColumn("registered", PrimitiveTypes.DateTime());

			return tableInfo;
		}

		private TableInfo CreateFirstTable() {
			var tableInfo = new TableInfo(new ObjectName(new ObjectName("APP"), "persons"));
			tableInfo.AddColumn("person_id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("age", PrimitiveTypes.Integer());

			return tableInfo;
		}

		[Test]
		public void NaturalInnerJoin() {
			SqlExpression expression = null;
			Assert.DoesNotThrow(() => expression = SqlExpression.Parse("SELECT * FROM persons, codes"));
			Assert.IsNotNull(expression);
			Assert.IsInstanceOf<SqlQueryExpression>(expression);

			SqlExpression resultExpression = null;
			Assert.DoesNotThrow(() => resultExpression = expression.Evaluate(QueryContext, null));
			Assert.IsNotNull(resultExpression);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExpression);

			var constantExpression = (SqlConstantExpression) resultExpression;
			Assert.IsInstanceOf<QueryType>(constantExpression.Value.Type);

			var queryPlan = ((SqlQueryObject) constantExpression.Value.Value);
			ITable result = null;

			Assert.DoesNotThrow(() => result = queryPlan.QueryPlan.Evaluate(QueryContext));

			Assert.IsNotNull(result);
		}
	}
}
