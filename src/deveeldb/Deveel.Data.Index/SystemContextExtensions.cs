using System;
using System.Collections.Generic;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;

namespace Deveel.Data.Index {
	public static class SystemContextExtensions {
		public static IIndexFactory ResolveIndexFactory(this ISystemContext context, string indexType) {
			return context.ServiceProvider.Resolve<IIndexFactory>(indexType);
		}

		public static ColumnIndex CreateColumnIndex(this ISystemContext context, string indexType,
			ColumnIndexContext indexContext) {
			var factory = context.ResolveIndexFactory(indexType);
			if (factory == null)
				throw new NotSupportedException(String.Format("None index factory for type '{0}' was configured in the system.", indexType));

			return factory.CreateIndex(indexContext);
		}

		public static ColumnIndex CreateColumnIndex(this ISystemContext context, string indexType, ITable table,
			int columnOffset) {
			return CreateColumnIndex(context, indexType, table, columnOffset, null);
		}

		public static ColumnIndex CreateColumnIndex(this ISystemContext context, string indexType, ITable table,
			int columnOffset, IEnumerable<KeyValuePair<string, object>> metadata) {
			return context.CreateColumnIndex(indexType, new ColumnIndexContext(table, columnOffset, metadata));
		}
	}
}
