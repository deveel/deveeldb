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

		private DateTimeOffset cornerTime;

		[SetUp]
		public void TestSetUp() {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric(), true);
			tableInfo.AddColumn("name", PrimitiveTypes.String(SqlTypeCode.VarChar));
			tableInfo.AddColumn("date", PrimitiveTypes.Date());

			cornerTime = DateTimeOffset.UtcNow;

			var rows = new List<DataObject[]>();

			AddRow(rows, 1, "test1", cornerTime);
			AddRow(rows, 2, "test2", cornerTime.AddSeconds(2));
			AddRow(rows, 3, "test3", cornerTime.AddSeconds(5));

			table = new TestTable(tableInfo, rows);
		}

		private void AddRow(List<DataObject[]> rows, long id, string name, DateTimeOffset date) {
			var row = new DataObject[3];
			row[0] = DataObject.BigInt(id);
			row[1] = DataObject.String(name);
			row[2] = DataObject.Date(date);
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

		[Test]
		public void SelectGreater() {
			var id = DataObject.BigInt(1);

			var result = table.SelectGreater(0, id);

			Assert.IsNotNull(result);
			Assert.IsNotEmpty(result);
			Assert.AreEqual(2, result.Count());

			Assert.AreEqual(1, result.First());
		}
	}
}
