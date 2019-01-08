using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables.Model;
using Deveel.Data.Sql.Types;

using Xunit;

namespace Deveel.Data.Sql.Tables {
	public class OuterTableTests {
		private ITable left;
		private ITable right;

		public OuterTableTests() {
			var leftInfo = new TableInfo(ObjectName.Parse("tab1"));
			leftInfo.Columns.Add(new ColumnInfo("a", PrimitiveTypes.Integer()));
			leftInfo.Columns.Add(new ColumnInfo("b", PrimitiveTypes.Boolean()));

			var temp1 = new TemporaryTable(leftInfo);
			temp1.AddRow(new [] { SqlObject.Integer(23), SqlObject.Boolean(true) });
			temp1.AddRow(new [] { SqlObject.Integer(54),SqlObject.Boolean(null) });

			left = temp1;

			var rightInfo = new TableInfo(ObjectName.Parse("tab2"));
			rightInfo.Columns.Add(new ColumnInfo("a", PrimitiveTypes.Integer()));
			rightInfo.Columns.Add(new ColumnInfo("b", PrimitiveTypes.Boolean()));

			var temp2 = new TemporaryTable(rightInfo);
			temp2.AddRow(new [] { SqlObject.Integer(15), SqlObject.Boolean(true) });
			temp2.AddRow(new [] { SqlObject.Integer(544),SqlObject.Boolean(false) });

			right = temp2;
		}

		[Fact]
		public async Task RightOuterTable() {
			var leftRows = left.Select(x => x.Number);
			var rightRows = right.Select(x => x.Number);

			var v1 = new VirtualTable(new[]{ left, right}, new[]{ leftRows, rightRows});
			var result = v1.Outer(right);

			Assert.NotNull(result);
			Assert.Equal(4, result.RowCount);
			Assert.Equal(4, result.TableInfo.Columns.Count);

			// the outher right join has placed the right table on top 
			// while the previous merge had the columns at the end already
			// +--------+--------+--------+--------+
			// | tab1.a | tab1.b | tab2.a | tab2.b |
			// +-----------------------------------+
			//
			var value1 = await result.GetValueAsync(0, 2);
			var value2 = await result.GetValueAsync(0, 3);

			Assert.Equal(SqlObject.Integer(15), value1);
			Assert.Equal(SqlObject.Boolean(true), value2);
		}

		[Fact]
		public async Task LeftOuterTable() {
			var leftRows = left.Select(x => x.Number);
			var rightRows = right.Select(x => x.Number);

			var v1 = new VirtualTable(new[] { left, right }, new[] { leftRows, rightRows });
			var result = v1.Outer(left);

			Assert.NotNull(result);
			Assert.Equal(4, result.RowCount);
			Assert.Equal(4, result.TableInfo.Columns.Count);

			// the outher right join has placed the left table on top 
			// while the previous merge had the columns at the beginning
			// +--------+--------+--------+--------+
			// | tab1.a | tab1.b | tab2.a | tab2.b |
			// +-----------------------------------+
			//
			var value1 = await result.GetValueAsync(1, 0);
			var value2 = await result.GetValueAsync(1, 1);

			Assert.Equal(SqlObject.Integer(54), value1);
			Assert.Equal(SqlObject.Boolean(null), value2);
		}
	}
}