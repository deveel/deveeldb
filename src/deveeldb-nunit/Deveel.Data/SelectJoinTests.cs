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

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class SelectJoinTests : ContextBasedTest {
		protected override void OnAfterSetup(string testName) {
			CreateTestTables(Query);
			AddTestData(Query);
		}

		private void AddTestData(IQuery query) {
			var table = query.Access().GetMutableTable(new ObjectName(new ObjectName("APP"), "persons"));

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

			row = table.NewRow();
			row["person_id"] = Field.Integer(5);
			row["name"] = Field.String("Charles Down");
			row["age"] = Field.Integer(22);
			table.AddRow(row);

			table = query.Access().GetMutableTable(new ObjectName(new ObjectName("APP"), "codes"));
			row = table.NewRow();
			row["person_id"] = Field.Integer(1);
			row["code"] = Field.String("123456");
			row["registered"] = Field.Date(new SqlDateTime(2014, 01, 12));
			table.AddRow(row);

			row = table.NewRow();
			row["person_id"] = Field.Integer(5);
			row["code"] = Field.String("9901009");
			row["registered"] = Field.Date(new SqlDateTime(2015, 04, 22));
			table.AddRow(row);
		}

		private void CreateTestTables(IQuery query) {
			var tableInfo = CreateFirstTable();
			query.Session.Access().CreateTable(tableInfo, false);
			query.Session.Access().AddPrimaryKey(tableInfo.TableName, "person_id");

			tableInfo = CreateSecondTable();
			query.Session.Access().CreateTable(tableInfo, false);
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

		private ITable Execute(string s) {
			var query = (SqlQueryExpression)SqlExpression.Parse(s);
			var result = Query.Select(query);
			result.GetEnumerator().MoveNext();
			return result.Source;
		}

		[Test]
		public void NaturalInnerJoin() {
			var result = Execute("SELECT * FROM persons a, codes b WHERE a.person_id = b.person_id");

			Assert.IsNotNull(result);
			Assert.AreEqual(2, result.RowCount);
		}

		[Test]
		public void InnerJoin() {
			var result = Execute("SELECT * FROM persons a INNER JOIN codes b ON a.person_id = b.person_id");

			Assert.IsNotNull(result);
			Assert.AreEqual(2, result.RowCount);
		}

		[Test]
		public void LeftOuterJoin() {
			var result = Execute("SELECT a.name, b.code FROM persons a LEFT OUTER JOIN codes b ON a.person_id = b.person_id");

			Assert.IsNotNull(result);
			Assert.AreEqual(3, result.RowCount);
		}


		[Test]
		public void RightOuterJoin() {
			var result = Execute("SELECT a.name, b.code FROM persons a RIGHT OUTER JOIN codes b ON a.person_id = b.person_id");

			Assert.IsNotNull(result);
			Assert.AreEqual(2, result.RowCount);
		}

		[Test]
		public void JoinWithSubquery() {
			var result = Execute("SELECT a.name, b.code FROM persons a, (SELECT * FROM codes) b WHERE a.person_id = b.person_id");

			Assert.IsNotNull(result);
			Assert.AreEqual(2, result.RowCount);
		}

		[Test]
		public void SimpleNaturalJoin() {
			var result = Execute("SELECT * FROM persons, codes");

			Assert.IsNotNull(result);
			Assert.AreEqual(6, result.RowCount);
		}
	}
}
