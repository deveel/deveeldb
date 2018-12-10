using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using Deveel.Data.Sql.Tables.Model;
using Deveel.Data.Sql.Types;

using Xunit;

namespace Deveel.Data.Sql.Tables {
	public class CrossJoinedTableTests {
		private ITable table;

		public CrossJoinedTableTests() {
			var leftInfo = new TableInfo(ObjectName.Parse("tab1"));
			leftInfo.Columns.Add(new ColumnInfo("a", PrimitiveTypes.Integer()));
			leftInfo.Columns.Add(new ColumnInfo("b", PrimitiveTypes.Boolean()));

			var left = new TemporaryTable(leftInfo);
			left.AddRow(new[] {SqlObject.Integer(23), SqlObject.Boolean(true)});
			left.AddRow(new[] {SqlObject.Integer(54), SqlObject.Boolean(null)});

			var rightInfo = new TableInfo(ObjectName.Parse("tab2"));
			rightInfo.Columns.Add(new ColumnInfo("a", PrimitiveTypes.Integer()));
			rightInfo.Columns.Add(new ColumnInfo("b", PrimitiveTypes.Boolean()));

			var right = new TemporaryTable(rightInfo);
			right.AddRow(new[] {SqlObject.Integer(15), SqlObject.Boolean(true)});
			right.AddRow(new[] {SqlObject.Integer(544), SqlObject.Boolean(false)});

			table = new CrossJoinedTable(left, right);
		}

		[Fact]
		public void CreateJoinedTable() {
			Assert.Equal(4, table.RowCount);
			Assert.Equal(4, table.TableInfo.Columns.Count);
		}

		[Fact]
		public async Task GetValueFromJoined() {
			Assert.Equal(4, table.RowCount);

			var value1 = await table.GetValueAsync(0, 0);
			var value2 = await table.GetValueAsync(1, 0);
			var value3 = await table.GetValueAsync(2, 0);
			var value4 = await table.GetValueAsync(3, 0);

			Assert.NotNull(value1);
			Assert.NotNull(value2);
			Assert.NotNull(value3);
			Assert.NotNull(value4);

			Assert.Equal(value1, value2);
			Assert.Equal(value3, value4);
			Assert.NotEqual(value1, value3);
			Assert.NotEqual(value2, value4);
		}

		[Fact]
		public void EnumerateRows() {
			var row1 = table.ElementAt(0);
			var row2 = table.ElementAt(1);

			Assert.NotNull(row1);
			Assert.NotNull(row2);
		}

	}
}