using System;

using Deveel.Data.Configurations;
using Deveel.Data.Services;
using Deveel.Data.Sql.Types;
using Deveel.Data.Storage;

using Moq;

using Xunit;

namespace Deveel.Data.Sql.Tables {
	public class TableSystemV2Tests : IDisposable {
		private Database database;

		public TableSystemV2Tests() {
			var container = new ServiceContainer();
			container.Register<ITableSystem, TableSystemV2>();
			container.Register<IStoreSystem, InMemoryStoreSystem>();

			var system = new DatabaseSystem(container, new Configuration());
			system.Start();

			database = system.CreateDatabase("testdb", new Configuration(), new IDatabaseFeature[0]);
		}

		[Fact]
		public void CreateTableSource() {
			var sys = database.TableSystem;

			var tableInfo = new TableInfo(ObjectName.Parse("sys.tab1"));
			tableInfo.Columns.Add(new ColumnInfo("a", PrimitiveTypes.Integer()));
			tableInfo.Columns.Add(new ColumnInfo("b", PrimitiveTypes.String()));

			var source = sys.CreateTableSource(tableInfo, false);

			Assert.NotNull(source);
			Assert.Equal(tableInfo.TableName, source.TableInfo.TableName);
		}

		public void Dispose() {
			database?.Dispose();
		}
	}
}