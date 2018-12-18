using System;

using Deveel.Data.Configurations;
using Deveel.Data.Services;
using Deveel.Data.Sql.Types;
using Deveel.Data.Storage;

using Moq;

using Xunit;

namespace Deveel.Data.Sql.Tables {
	public class TableSystemV2Tests {
		private IDatabase database;
		private InMemoryStoreSystem storeSystem;

		public TableSystemV2Tests() {
			var db = new Mock<IDatabase>();
			db.SetupGet(x => x.Scope)
				.Returns(new ServiceContainer());
			db.SetupGet(x => x.Configuration)
				.Returns(new Configuration());

			database = db.Object;

			storeSystem = new InMemoryStoreSystem();
		}

		[Fact]
		public void CreateNew() {
			var sys = new TableSystemV2(database, storeSystem);
			sys.Create();

			Assert.False(sys.IsClosed);
			Assert.False(sys.IsReadOnly);
		}

		[Fact]
		public void CreateTableSource() {
			var sys = new TableSystemV2(database, storeSystem);
			sys.Create();

			var tableInfo = new TableInfo(ObjectName.Parse("sys.tab1"));
			tableInfo.Columns.Add(new ColumnInfo("a", PrimitiveTypes.Integer()));
			tableInfo.Columns.Add(new ColumnInfo("b", PrimitiveTypes.String()));

			var source = sys.CreateTableSource(tableInfo, false);

			Assert.NotNull(source);
			Assert.Equal(tableInfo.TableName, source.TableInfo.TableName);

			sys.Dispose();
		}
	}
}