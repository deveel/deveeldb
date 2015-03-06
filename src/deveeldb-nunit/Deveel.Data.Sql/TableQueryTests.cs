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
using Deveel.Data.Deveel.Data.Sql;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class TableQueryTests {
		private ITable table;

		private void AddRow(TemporaryTable tmpTable, long id, string name, DateTimeOffset date) {
			var row = new DataObject[3];
			row[0] = DataObject.BigInt(id);
			row[1] = DataObject.String(name);
			row[2] = DataObject.Date(date);
			tmpTable.NewRow(row);
		}

		[SetUp]
		public void TestSetUp() {
			var tableInfo = new TableInfo(new ObjectName("test_table"));
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("date", PrimitiveTypes.Date());

			var cornerTime = DateTimeOffset.UtcNow;

			var tmpTable = new TemporaryTable(tableInfo);

			AddRow(tmpTable, 1, "test1", cornerTime);
			AddRow(tmpTable, 2, "test2", cornerTime.AddSeconds(2));
			AddRow(tmpTable, 3, "test3", cornerTime.AddSeconds(5));

			tmpTable.BuildIndexes();

			table = tmpTable;
		}

		[TestCase(1, 0)]
		[TestCase(3, 2)]
		public void SelectRowsWhereStaticId(int id, int expectedRow) {
			var result = table.SelectRows(0, BinaryOperator.Equal, DataObject.BigInt(id));
			var list = result.ToList();

			Assert.IsNotEmpty(list);
			Assert.AreEqual(1, list.Count);
			Assert.AreEqual(expectedRow, list[0]);
		}

		[TestCase("test2", 1)]
		[TestCase("test3", 2)]
		public void SelectRowsWhereStaticName(string name, int expectedRow) {
			var result = table.SelectRows(1, BinaryOperator.Equal, DataObject.VarChar(name));
			var list = result.ToList();

			Assert.IsNotEmpty(list);
			Assert.AreEqual(1, list.Count);
			Assert.AreEqual(expectedRow, list[0]);
		}
	}
}
