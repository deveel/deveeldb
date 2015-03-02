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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

using Moq;

using NUnit.Framework;

namespace Deveel.Data.Index {
	[TestFixture]
	public sealed class BlindSearchTests {
		private ITable table;

		[SetUp]
		public void TestSetUp() {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric(), true);
			tableInfo.AddColumn("name", PrimitiveTypes.String(SqlTypeCode.VarChar));
			tableInfo.AddColumn("date", PrimitiveTypes.Date());

			var rows = new List<Row>();
			var tableMock = new Mock<ITable>();
			tableMock.Setup(x => x.FullName)
				.Returns(tableName);
			tableMock.Setup(x => x.RowCount)
				.Returns(() => rows.Count);
			tableMock.Setup(x => x.TableInfo)
				.Returns(tableInfo);
			tableMock.Setup(x => x.GetEnumerator())
				.Returns(() => new SimpleRowEnumerator(tableMock.Object));
			tableMock.Setup(x => x.ObjectType)
				.Returns(DbObjectType.Table);
			tableMock.Setup(x => x.GetValue(It.IsAny<long>(), It.IsAny<int>()))
				.Returns<long, int>((rowNum, colIndex) => {
					var row = rows.FirstOrDefault(x => x.RowId.RowNumber == rowNum);
					if (row == null)
						return DataObject.Null();

					return row.GetValue(colIndex);
				});
			tableMock.Setup(x => x.GetIndex(It.IsAny<int>()))
				.Returns<int>(offset => new BlindSearchIndex(tableMock.Object, offset));

			table = tableMock.Object;

			AddRow(rows, 1, "test1", DateTimeOffset.UtcNow);
			AddRow(rows, 2, "test2", DateTimeOffset.UtcNow.AddSeconds(2));
		}

		private void AddRow(List<Row> rows, long id, string name, DateTimeOffset date) {
			var row = new Row(table, new RowId(-1, rows.Count));
			row.SetValue(0, DataObject.BigInt(id));
			row.SetValue(1, DataObject.String(name));
			row.SetValue(2, DataObject.Date(date));
			rows.Add(row);
		}

		[Test]
		public void SelectEqualOneColumn() {
			var name = DataObject.String("test1");
			var result = table.SelectEqual(1, name);

			Assert.IsNotNull(result);
			Assert.IsNotEmpty(result);

			var index = result.First();
			Assert.AreEqual(0, index);
		}

		[Test]
		public void SelectEqualTwoColumns() {
			var name = DataObject.String("test1");
			var id = DataObject.BigInt(1);

			var result = table.SelectEqual(1, name, 0, id);
			Assert.IsNotNull(result);
			Assert.IsNotEmpty(result);

			var index = result.First();
			Assert.AreEqual(0, index);
		}
	}
}
