using System;

using Deveel.Data.Configurations;
using Deveel.Data.Services;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Storage;
using Deveel.Data.Transactions;

using Xunit;

namespace Deveel.Data {
	public class DatabaseTests : IDisposable {
		private IDatabaseSystem system;

		public DatabaseTests() {
			var container = new ServiceContainer();
			container.Register<ITableSystem, TableSystemV2>(KnownScopes.Database);
			container.Register<IDbObjectManager, TableManager>(KnownScopes.Transaction, DbObjectType.Table);
			container.Register<IStoreSystem, InMemoryStoreSystem>();

			system = new DatabaseSystem(container, new Configuration());
			system.Start();
		}

		[Fact]
		public void CreateNewDatabase() {
			var db = system.CreateDatabase("testdb", new Configuration(), new IDatabaseFeature[0]);

			Assert.NotNull(db);
			Assert.IsType<Database>(db);
			Assert.NotNull(db.OpenTransactions);
			Assert.Empty(db.OpenTransactions);

			db.Dispose();
		}

		[Fact]
		public void CreateNewDatabaseOverrideStorage() {
			var config = new Configuration();
			config.SetValue("store.type", "Deveel.Data.Storage.InMemoryStoreSystem");

			var db = system.CreateDatabase("testdb", config, new IDatabaseFeature[0]);

			Assert.NotNull(db);
			Assert.IsType<Database>(db);

			var database = (Database) db;
			Assert.NotNull(database.TableSystem);
			Assert.NotNull(database.StoreSystem);
			Assert.IsType<InMemoryStoreSystem>(database.StoreSystem);
			Assert.NotNull(db.OpenTransactions);
			Assert.Empty(db.OpenTransactions);

			db.Dispose();
		}

		[Fact]
		public void CreateNewTransaction() {
			var db = system.CreateDatabase("testdb", new Configuration(), new IDatabaseFeature[0]);

			Assert.NotNull(db);
			Assert.NotNull(db.OpenTransactions);
			Assert.Empty(db.OpenTransactions);

			var transaction = db.CreateTransaction();

			Assert.NotNull(transaction);
			Assert.Equal(TransactionStatus.Started, transaction.Status);
			Assert.NotNull(transaction.State);

			Assert.NotEmpty(db.OpenTransactions);

			transaction.Dispose();
			db.Dispose();
		}

		public void Dispose() {
			system?.Dispose();
		}
	}
}
