using System;

using IQToolkit.Data;

namespace Deveel.Data.Linq {
	public class DbQueryContext : IDisposable {
		public DbQueryContext(IQuery context) 
			: this(context, null) {
		}

		public DbQueryContext(IQuery context, DbCompiledModel model) {
			if (context == null)
				throw new ArgumentNullException("context");

			Context = context;
			Model = model;
		}

		~DbQueryContext() {
			Dispose(false);
		}

		protected IQuery Context { get; private set; }

		protected DbCompiledModel Model { get; private set; }

		private QueryProvider QueryProvider { get; set; }

		public DbTable<T> Table<T>() where T : class {
			EnsureProvider();
			// TODO: Get the type mapping and if not throw and exception
			return new DbTable<T>(QueryProvider);
		}

		private void EnsureProvider() {
			if (QueryProvider == null) {
				if (Model == null)
					Model = BuildModel();

				var mapping = Model.CreateMapping();
				QueryProvider = new QueryProvider(Context, mapping, new EntityPolicy());
			}
		}

		private DbCompiledModel BuildModel() {
			var builder = new DbModelBuilder(GetType());
			OnBuildModel(builder);
			return builder.Compile();
		}

		protected virtual void OnBuildModel(DbModelBuilder modelBuilder) {
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			Context = null;
		}
	}
}
