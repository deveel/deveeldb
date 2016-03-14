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
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class JoinTableTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			CreateTestTables();
			AddTestData();
		}

		private void AddTestData() {
			var table = Query.IsolatedAccess.GetMutableTable(new ObjectName(new ObjectName("APP"), "persons"));

			var row = table.NewRow();
			row["person_id"] = Field.Integer(1);
			row["name"] = Field.String("Antonello Provenzano");
			row["age"] = Field.Integer(34);
			table.AddRow(row);

			row = table.NewRow();
			row["person_id"] = Field.Integer(3);
			row["name"] = Field.String("John Doe");
			row["age"] = Field.Integer(56);
			table.AddRow(row);

			table = Query.IsolatedAccess.GetMutableTable(new ObjectName(new ObjectName("APP"), "codes"));
			row = table.NewRow();
			row["person_id"] = Field.Integer(1);
			row["code"] = Field.String("123456");
			row["registered"] = Field.Date(new SqlDateTime(2014, 01, 12));
			table.AddRow(row);
		}

		private void CreateTestTables() {
			var tableInfo = CreateFirstTable();
			Query.Session.SystemAccess.CreateTable(tableInfo, false);
			Query.Session.SystemAccess.AddPrimaryKey(tableInfo.TableName, "person_id");

			tableInfo = CreateSecondTable();
			Query.Session.SystemAccess.CreateTable(tableInfo, false);
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
			Assert.DoesNotThrow(() => resultExpression = expression.Evaluate(Query, null));
			Assert.IsNotNull(resultExpression);
			Assert.IsInstanceOf<SqlConstantExpression>(resultExpression);

			var constantExpression = (SqlConstantExpression) resultExpression;
			Assert.IsInstanceOf<QueryType>(constantExpression.Value.Type);

			var queryPlan = ((SqlQueryObject) constantExpression.Value.Value);
			ITable result = null;

			Assert.DoesNotThrow(() => result = queryPlan.QueryPlan.Evaluate(Query));

			Assert.IsNotNull(result);
		}
	}
}
