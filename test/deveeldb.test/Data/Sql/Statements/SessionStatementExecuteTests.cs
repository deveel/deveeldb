using System;

using Deveel.Data.Configurations;
using Deveel.Data.Services;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Storage;

using Xunit;

namespace Deveel.Data.Sql.Statements {
	public class SessionStatementExecuteTests : IDisposable {
		private IDatabaseSystem system;
		private IDatabase database;
		private ISession session;

		public SessionStatementExecuteTests() {
			var container = new ServiceContainer();
			container.Register<ITableSystem, TableSystemV2>(KnownScopes.Database);
			container.Register<IStoreSystem, InMemoryStoreSystem>();

			system = new DatabaseSystem(container, new Configuration());
			system.Start();

            database = system.CreateDatabase("test", new Configuration(), null);
            session = database.CreateSystemSession("app");
		}

        [Fact]
		public async void ExecuteNullStatement() {
			var result = await session.ExecuteStatementAsync(new NullStatement());
            
            Assert.NotNull(result);
            Assert.IsType<EmptyStatementResult>(result);
		}

		public void Dispose() {
			session?.Dispose();
            database?.Dispose();
            system?.Dispose();
		}
	}
}