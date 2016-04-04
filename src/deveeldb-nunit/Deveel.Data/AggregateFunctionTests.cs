using System;
using System.Linq;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture(StorageType.InMemory)]
	[TestFixture(StorageType.JournaledFile)]
	[TestFixture(StorageType.SingleFile)]
	public sealed class AggregateFunctionTests : FunctionTestBase {
		public AggregateFunctionTests(StorageType storageType)
			: base(storageType) {
		}

		protected override void OnSetUp(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String());

			query.Access().CreateObject(tableInfo);

			var table = query.Access().GetMutableTable(tableName);
			for (int i = 0; i < 15; i++) {
				var aValue = Field.Integer(i * i);
				var bValue = Field.String(i.ToString());

				var row = table.NewRow();
				row.SetValue(0, aValue);
				row.SetValue(1, bValue);

				table.AddRow(row);
			}
		}

		protected override void OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			query.Access().DropObject(DbObjectType.Table, tableName);
		}

		private Field SelectAggregate(string functionName, params SqlExpression[] args) {
			var column = new SelectColumn(SqlExpression.FunctionCall(functionName, args));
			var query = new SqlQueryExpression(new[] { column });
			query.FromClause.AddTable("APP.test_table");

			var result = Query.Select(query);

			var row = result.FirstOrDefault();
			if (row == null)
				throw new InvalidOperationException();

			return row.GetValue(0);
		}

		[Test]
		public void SimpleAvg() {
			var result = SelectAggregate("AVG", SqlExpression.Reference(new ObjectName("a")));
			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);
			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);

			var value = (SqlNumber) result.Value;
		}

		[Test]
		public void Count_Column() {
			var result = SelectAggregate("COUNT", SqlExpression.Reference(new ObjectName("a")));
			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);
			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);

			var value = (SqlNumber)result.Value;
		}

		[Test]
		public void CountAll() {
			var result = SelectAggregate("COUNT", SqlExpression.Constant("*"));
			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);
			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);

			var value = (SqlNumber)result.Value;
			Assert.AreEqual(new SqlNumber(15), value);
		}

		[Test]
		public void DistinctCount() {
			var result = SelectAggregate("DISTINCT_COUNT", SqlExpression.Reference(new ObjectName("a")));
			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);
			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);

			var value = (SqlNumber)result.Value;
			Assert.AreEqual(new SqlNumber(15), value);
		}

		[Test]
		public void Min_Column() {
			var result = SelectAggregate("MIN", SqlExpression.Reference(new ObjectName("a")));
			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);
			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);

			var value = (SqlNumber) result.Value;
			Assert.AreEqual(new SqlNumber(0), value);
		}

		[Test]
		public void Max_Column() {
			var result = SelectAggregate("MAX", SqlExpression.Reference(new ObjectName("a")));
			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);
			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);

			var value = (SqlNumber)result.Value;
			Assert.AreEqual(new SqlNumber(196), value);
		}

		[Test]
		public void Sum_Column() {
			var result = SelectAggregate("SUM", SqlExpression.Reference(new ObjectName("a")));
			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);
			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);

			var value = (SqlNumber)result.Value;
			Assert.AreEqual(new SqlNumber(1015), value);
		}
	}
}
