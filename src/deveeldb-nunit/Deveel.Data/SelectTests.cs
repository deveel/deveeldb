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
using System.Linq;

using Deveel.Data.Mapping;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class SelectTests : ContextBasedTest {
		protected override void OnAfterSetup(string testName) {
			CreateTestTable(Query);
			AddTestData(Query);
		}

		private static void CreateTestTable(IQuery context) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] {SqlExpression.Constant(tableInfo.TableName.FullName)});
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
			return Execute(s, null);
		}

		private ITable Execute(string s, QueryLimit limit) {
			var query = (SqlQueryExpression) SqlExpression.Parse(s);
			var result = Query.Select(query, limit);
			result.GetEnumerator().MoveNext();
			return result.Source;
		}

		[Test]
		public void AllColumns() {
			var result = Execute("SELECT * FROM test_table");

			Assert.IsNotNull(result);
			Assert.AreEqual(3, result.RowCount);
		}

		[Test]
		public void OrderedSelect() {
			var query = (SqlQueryExpression) SqlExpression.Parse("SELECT * FROM test_table");
			var sort = new[] {new SortColumn(SqlExpression.Reference(new ObjectName("birth_date")), false)};

			var result = Query.Select(query, sort);

			Assert.IsNotNull(result);

			Assert.IsTrue(result.GetEnumerator().MoveNext());

			Assert.AreEqual(3, result.Source.RowCount);

			var firstName = result.Source.GetValue(0, 1);

			Assert.AreEqual("Roger", firstName.Value.ToString());
		}

		[Test]
		public void SelectAliasedWithGroupedExpression() {
			var result = Execute("SELECT * FROM test_table t0 WHERE (t0.id = 1 AND t0.id <> 0)");

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
		}

		[Test]
		public void SelectFromAliased() {
			var result = Execute("SELECT * FROM test_table t0 WHERE t0.id = 1");

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
		}

		[Test]
		public void WhereGreaterThan() {
			var result = Execute("SELECT * FROM test_table WHERE id > 1");

			Assert.IsNotNull(result);
			Assert.AreEqual(2, result.RowCount);
		}

		[Test]
		public void WhereSmallerThanOrEqual() {
			var result = Execute("SELECT * FROM test_table WHERE id <= 1");

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
		}

		[Test]
		public void WhereGreaterThanOrEqual() {
			var result = Execute("SELECT * FROM test_table WHERE id >= 2");

			Assert.IsNotNull(result);
			Assert.AreEqual(2, result.RowCount);
		}


		[Test]
		public void WhereLikes_Start() {
			var result = Execute("SELECT * FROM test_table WHERE first_name LIKE 'J%'");

			Assert.IsNotNull(result);
			Assert.AreEqual(2, result.RowCount);
		}

		[Test]
		public void WhereLikes_End() {
			var result = Execute("SELECT * FROM test_table WHERE first_name LIKE '%er'");

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
		}

		[Test]
		public void WhereLikes_Contains() {
			var result = Execute("SELECT * FROM test_table WHERE first_name LIKE '%Rog%'");

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
		}

		[Test]
		public void WhereNotLikes_Start() {
			var result = Execute("SELECT * FROM test_table WHERE first_name NOT LIKE 'J%'");

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
		}

		[Test]
		public void WhereIsNotNull() {
			var result = Execute("SELECT * FROM test_table WHERE first_name IS NOT NULL");

			Assert.IsNotNull(result);
			Assert.AreEqual(3, result.RowCount);
		}

		[Test]
		public void WhereInArray() {
			var result = Execute("SELECT * FROM test_table WHERE id IN (2, 3)");

			Assert.IsNotNull(result);
			Assert.AreEqual(2, result.RowCount);
		}

		[Test]
		public void WhereBetween() {
			var result = Execute("SELECT * FROM test_table WHERE id BETWEEN 1 AND 2");

			Assert.IsNotNull(result);
			Assert.AreEqual(2, result.RowCount);
		}

		[Test]
		public void WhereAnyGreater() {
			var result = Execute("SELECT * FROM test_table WHERE id > ANY (1 ,2)");

			Assert.IsNotNull(result);
			Assert.AreEqual(2, result.RowCount);
		}

		[Test]
		public void LimitToOne() {
			var result = Execute("SELECT * FRPM test_table", new QueryLimit(1));

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
		}

		[Test]
		public void LiimitToTwoFromSecond() {
			var result = Execute("SELECT * FROM test_table", new QueryLimit(1, 2));
			Assert.IsNotNull(result);
			Assert.AreEqual(2, result.RowCount);
		}

		[Test]
		public void SelectSubQuery() {
			var result = Execute("SELECT * FROM (SELECT * FROM test_table WHERE id > 1) AS q");

			Assert.IsNotNull(result);
			Assert.AreEqual(2, result.RowCount);
		}

		[Test]
		public void SelectOr() {
			var result = Execute("SELECT * FROM test_table WHERE id = 1 OR id > 2");

			Assert.IsNotNull(result);
			Assert.AreEqual(2, result.RowCount);
		}

		[Test]
		public void GroupBy() {
			var result = Execute("SELECT * FROM test_table GROUP BY last_name");

			Assert.IsNotNull(result);
			Assert.AreEqual(2, result.RowCount);
		}

		[Test]
		public void GroupByAndHaving() {
			var result = Execute("SELECT * FROM test_table GROUP BY last_name HAVING first_name = 'John'");

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
		}

		[Test]
		public void FromMap() {
			var result = Query.Select<TestClass>("SELECT * FROM test_table");

			Assert.IsNotNull(result);
			Assert.AreEqual(3, result.Count());

			var first = result.ElementAt(0);

			Assert.IsNotNull(first);
			Assert.AreEqual(1, first.Id);
			Assert.AreEqual("John", first.FirstName);
			Assert.AreEqual("Doe", first.LastName);
			Assert.AreEqual(new DateTime(1977, 01, 01), first.BirthDate);
		}


		[TableName("test_table")]
		class TestClass {
			[Column(Name = "id"), Identity]
			public int Id { get; set; }

			[Column(Name = "first_name")]
			public string FirstName { get; set; }

			[Column(Name = "last_name")]
			public string LastName { get; set; }

			[Column(Name = "birth_date")]
			public DateTime? BirthDate { get; set; }
		}
	}
}
