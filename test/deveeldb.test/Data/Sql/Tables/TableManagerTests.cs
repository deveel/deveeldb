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
using System.Linq;
using System.Threading.Tasks;

using Deveel.Data.Events;
using Deveel.Data.Sql.Indexes;
using Deveel.Data.Sql.Types;
using Deveel.Data.Transactions;

using Moq;

using Xunit;

namespace Deveel.Data.Sql.Tables {
	public class TableManagerTests {
		private TableManager manager;
		private TransactionState state;
		private List<ITableEvent> events;

		public TableManagerTests() {
			var tableSystem = new Mock<ITableSystem>();
			tableSystem.Setup(x => x.CreateTableSourceAsync(It.IsAny<TableInfo>(), It.IsAny<bool>()))
				.Returns<TableInfo, bool>((info, temp) => {
					var source = new Mock<ITableSource>();
					source.SetupGet(x => x.TableInfo)
						.Returns(info);
					source.SetupGet(x => x.TableId)
						.Returns(1);
					return Task.FromResult<ITableSource>(source.Object);
				});

			var tableSource1 = new Mock<ITableSource>();
			tableSource1.SetupGet(x => x.TableInfo)
				.Returns(() => {
					var tableInfo = new TableInfo(ObjectName.Parse("sys.table2"));
					tableInfo.Columns.Add(new ColumnInfo("g", PrimitiveTypes.String()));
					return tableInfo;
				});
			tableSource1.SetupGet(x => x.TableId)
				.Returns(2);

			state = new TransactionState(new[] {tableSource1.Object}, new IRowIndexSet[]{null});

			events = new List<ITableEvent>();

			var transaction = new Mock<ITransaction>();
			transaction.SetupGet(x => x.State)
				.Returns(state);
			transaction.SetupGet(x => x.Registry)
				.Returns(() => {
					var registry = new Mock<IEventRegistry>();
					registry.Setup(x => x.Register(It.IsAny<ITableEvent>()))
						.Callback<IEvent>(e => events.Add((ITableEvent) e));

					return registry.Object;
				});

			manager = new TableManager(transaction.Object, tableSystem.Object);
		}

		[Fact]
		public async void CreateNewTable() {
			var tableInfo = new TableInfo(ObjectName.Parse("sys.table1"));
			tableInfo.Columns.Add(new ColumnInfo("a", PrimitiveTypes.BigInt()));

			await manager.CreateTableAsync(tableInfo);

			Assert.NotEmpty(manager.Transaction.State.VisibleTables);
			Assert.Equal(2, manager.Transaction.State.VisibleTables.Count());
			Assert.Single(events);
			Assert.IsType<TableCreatedEvent>(events[0]);
			Assert.Equal(tableInfo.TableName, events[0].TableName);
		}

		[Fact]
		public async void DropTableAsync() {
			var tableName = ObjectName.Parse("sys.table2");
			var result = await manager.DropTableAsync(tableName);

			Assert.True(result);
			Assert.Single(events);
			Assert.IsType<TableDroppedEvent>(events[0]);
			Assert.Equal(tableName, events[0].TableName);
		}
	}
}