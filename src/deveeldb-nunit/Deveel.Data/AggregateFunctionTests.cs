using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class AggregateFunctionTests : FunctionTestBase {
		protected override void OnSetUp(string testName) {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String());

			Query.Access.CreateObject(tableInfo);

			var table = Query.Access.GetMutableTable(tableName);
			for (int i = 0; i < 15; i++) {
				var aValue = Field.Integer(i * i);
				var bValue = Field.String(i.ToString());

				var row = table.NewRow();
				row.SetValue(0, aValue);
				row.SetValue(1, bValue);

				table.AddRow(row);
			}
		}

		protected override void OnTearDown() {
			var tableName = ObjectName.Parse("APP.test_table");
			Query.Access.DropObject(DbObjectType.Table, tableName);
		}

		private Field SelectAggregate(string functionName, params SqlExpression[] args) {
			var column = new SelectColumn(SqlExpression.FunctionCall(functionName, args));
			var query = new SqlQueryExpression(new[] { column });
			query.FromClause.AddTable("APP.test_table");

			var result = Query.Select(query);

			if (result.RowCount > 1)
				throw new InvalidOperationException("Too many rows");

			return result.GetValue(0, 0);
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
		public void SimpleCount() {
			var result = SelectAggregate("COUNT", SqlExpression.Reference(new ObjectName("a")));
			Assert.IsNotNull(result);
			Assert.IsFalse(result.IsNull);
			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);

			var value = (SqlNumber)result.Value;
		}

		[Test]
		public void CountAll() {
			var result = SelectAggregate("COUNT", SqlExpression.Reference(new ObjectName("*")));
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
		}
	}
}
