using System;

using Deveel.Data.Configurations;
using Deveel.Data.Services;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Storage;
using Deveel.Data.Transactions;

using Moq;

using Xunit;

namespace Deveel.Data {
	public class DatabaseTests : IDisposable {
		private IDatabaseSystem system;

		public DatabaseTests() {
			var container = new ServiceContainer();
			container.Register<ITableSystem, TableSystemV2>(KnownScopes.Database);
			container.Register<IDbObjectManager, TableManager>(KnownScopes.Transaction, DbObjectType.Table);
			container.Register<IStoreSystem, InMemoryStoreSystem>();

			var sysMock = new Mock<IDatabaseSystem>();
			sysMock.SetupGet(x => x.Scope)
				.Returns(container);

			system = sysMock.Object;
		}

		[Fact]
		public void CreateNewDatabase() {
			var db = new Database(system, "testdb", new Configuration());

			Assert.NotNull(db.TableSystem);
			Assert.NotNull(db.OpenTransactions);
			Assert.Empty(db.OpenTransactions);

			db.Create(null);

			db.Dispose();
		}

		[Fact]
		public void CreateNewDatabaseOverrideStorage() {
			var config = new Configuration();
			config.SetValue("store.type", "Deveel.Data.Storage.InMemoryStoreSystem");

			var db = new Database(system, "testdb", config);

			Assert.NotNull(db.TableSystem);
			Assert.NotNull(db.StoreSystem);
			Assert.IsType<InMemoryStoreSystem>(db.StoreSystem);
			Assert.NotNull(db.OpenTransactions);
			Assert.Empty(db.OpenTransactions);

			db.Create(null);

			db.Dispose();
		}

		[Fact]
		public void CreateNewTransaction() {
			var db = new Database(system, "testdb", new Configuration());
			db.Create(null);

			var transaction = db.CreateTransaction();

			Assert.NotNull(transaction);
			Assert.Equal(TransactionStatus.Started, transaction.Status);
			Assert.NotNull(transaction.State);

			transaction.Dispose();
			db.Dispose();
		}

		public void Dispose() {
			system?.Dispose();
		}
	}
}
