using System;
using System.Linq;

using Deveel.Data.Mapping;

using IQToolkit;

namespace Deveel.Data.Linq {
	public abstract class QueryContext : IDisposable {
		public QueryContext(IQueryContext context) {
			ParentContext = context;
		}

		~QueryContext() {
			Dispose(false);
		}

		public IQueryContext ParentContext { get; private set; }

		private IQueryProvider Provider { get; set; }

		private DeveelDbProvider CreateProvider() {
			if (Provider == null) {
				// TODO: Get all other settings as metadata

				var mappingContext = new MappingContext();
				OnBuildMap(mappingContext);

				var model = mappingContext.CreateModel();

				Provider = ParentContext.GetQueryProvider(model);
			}

			return (DeveelDbProvider) Provider;
		}

		protected abstract void OnBuildMap(MappingContext mappingContext);

		internal IEntityTable<T> GetTable<T>() where T : class {
			return CreateProvider().GetTable<T>();
		} 

		public QueryTable<T> Table<T>() where T : class {
			return new QueryTable<T>(this);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
			}

			Provider = null;
		}
	}
}
