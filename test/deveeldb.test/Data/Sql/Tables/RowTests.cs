// 
//  Copyright 2010-2018 Deveel
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
//

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Deveel.Data.Query;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

using Moq;

using Xunit;

namespace Deveel.Data.Sql.Tables {
	public class RowTests : IDisposable {
		private readonly ITable table;

		public RowTests() {
			var tableMock = TableUtil.MockTable("sys.table1", new Dictionary<string, SqlType> {
				{"col1", PrimitiveTypes.VarChar()},
				{"col2", PrimitiveTypes.Double()}
			});

			var values = new SqlObject[][] {
				new SqlObject[] {
					SqlObject.String("test1"),
					SqlObject.Double(20.1),
				},
				new SqlObject[] {
					SqlObject.String("t2"),
					SqlObject.Double(3.11)
				}
			};

			tableMock.SetupGet(x => x.RowCount)
				.Returns(1);
			tableMock.Setup(x => x.GetValueAsync(It.IsAny<long>(), It.IsInRange(0, 1, Range.Inclusive)))
				.Returns<long, int>((x, y) => Task.FromResult(values[x][y]));

			table = tableMock.Object;
		}

		[Theory]
		[InlineData(0, "test1", 20.1)]
		[InlineData(1, "t2", 3.11)]
		public async void GetValueFromTable(long rowNumber, string col1, double col2) {
			var row = table.GetRow(rowNumber);

			Assert.True(row.IsAttached);

			var value1 = await row.GetValueAsync(0);
			
			Assert.NotNull(value1);
			Assert.NotEqual(SqlObject.Null, value1);
			Assert.IsType<SqlString>(value1.Value);
			Assert.Equal(col1, ((SqlString) value1.Value).Value);

			var value2 = await row.GetValueAsync(1);
			
			Assert.NotNull(value2);
			Assert.NotEqual(SqlObject.Null, value2);
			Assert.IsType<SqlNumber>(value2.Value);
			Assert.Equal(col2, (double) ((SqlNumber) value2.Value));
		}

		[Theory]
		[InlineData("test2-set")]
		public async void SetValueToRow(string colValue) {
			var row = new Row(table);

			Assert.False(row.IsAttached);

			await row.SetValueAsync(0, SqlObject.String(colValue));

			var value = await row.GetValueAsync(0);

			Assert.NotNull(value);
			Assert.NotEqual(SqlObject.Null, value);
			Assert.IsType<SqlString>(value.Value);
			Assert.Equal(colValue, ((SqlString) value.Value).Value);
		}

		[Theory]
		[InlineData(0, "col1", "test1", true)]
		[InlineData(0, "col1", "test2", false)]
		public async void ResolveColumnReference(long rowNumber, string refName, string colValue, bool expected) {
			var expression = SqlExpression.Equal(SqlExpression.Reference(new ObjectName(refName)),
				SqlExpression.Constant(SqlObject.String(colValue)));

			var row = table.GetRow(rowNumber);

			var context = new Mock<IQuery>();
			context.SetupGet(x => x.Resolver)
				.Returns(row.ReferenceResolver);

			var resolved = await expression.ReduceAsync(context.Object);

			Assert.NotNull(resolved);
			Assert.IsType<SqlConstantExpression>(resolved);

			var value = ((SqlConstantExpression) resolved).Value;

			Assert.IsType<SqlBoolean>(value.Value);
			Assert.Equal(expected, (bool) ((SqlBoolean)value.Value));
		}

		public void Dispose() {
			table?.Dispose();
		}
	}
}