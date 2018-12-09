using System;
using System.Threading.Tasks;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

using Xunit;

namespace Deveel.Data.Sql.Tables {
	public class TableSelectTests {
		private ITable left;

		public TableSelectTests() {
			var leftInfo = new TableInfo(ObjectName.Parse("tab1"));
			leftInfo.Columns.Add(new ColumnInfo("a", PrimitiveTypes.Integer()));
			leftInfo.Columns.Add(new ColumnInfo("b", PrimitiveTypes.Boolean()));
			leftInfo.Columns.Add(new ColumnInfo("c", PrimitiveTypes.Double()));

			var leftTable = new TemporaryTable(leftInfo);
			leftTable.AddRow(new[] { SqlObject.Integer(23), SqlObject.Boolean(true), SqlObject.Double(5563.22) });
			leftTable.AddRow(new[] { SqlObject.Integer(54), SqlObject.Boolean(null), SqlObject.Double(921.001) });
			leftTable.AddRow(new[] { SqlObject.Integer(23), SqlObject.Boolean(true), SqlObject.Double(2010.221) });

			leftTable.BuildIndex();

			left = leftTable;
		}

		[Theory]
		[InlineData("a", SqlExpressionType.Equal, 23, 2)]
		[InlineData("a", SqlExpressionType.LessThan, 50, 2)]
		[InlineData("c", SqlExpressionType.GreaterThan, 2500, 1)]
		[InlineData("b", SqlExpressionType.Is, true, 2)]
		public async Task SimpleSelect(string column, SqlExpressionType op, object value, long expectedRowCount) {
			var columnName = new ObjectName(left.TableInfo.TableName, column);
			var expression = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(value)));
			var result = await left.SimpleSelectAsync(null, columnName, op, expression);

			Assert.NotNull(result);
			Assert.Equal(expectedRowCount, result.RowCount);
		}

		[Theory]
		[InlineData("a", SqlExpressionType.All, SqlExpressionType.LessThan, 45, 77, 2)]
		[InlineData("a", SqlExpressionType.Any, SqlExpressionType.LessThan, 45, 77, 3)]
		[InlineData("a", SqlExpressionType.All, SqlExpressionType.NotEqual, 23, 54, 0)]
		[InlineData("a", SqlExpressionType.Any, SqlExpressionType.NotEqual, 23, 78, 0)]
		[InlineData("a", SqlExpressionType.Any, SqlExpressionType.Equal, 23, 190, 2)]
		[InlineData("a", SqlExpressionType.All, SqlExpressionType.GreaterThan, 2, 22, 3)]
		[InlineData("a", SqlExpressionType.Any, SqlExpressionType.GreaterThan, 22, 67, 3)]
		public async Task NonCorrelatedSelect(string column, SqlExpressionType op, SqlExpressionType subOp, object value1, object value2, long expected) {
			var columnName = new ObjectName(left.TableInfo.TableName, column);
			var exp1 = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(value1)));
			var exp2 = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(value2)));
			var array = SqlExpression.Constant(SqlObject.Array(new SqlArray(new[] {exp1, exp2})));
			var result = await left.SelectNonCorrelatedAsync(null, columnName, op, subOp, array);

			Assert.NotNull(result);
			Assert.Equal(expected, result.RowCount);
		}
	}
}