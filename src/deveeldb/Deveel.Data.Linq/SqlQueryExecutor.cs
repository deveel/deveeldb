using System;
using System.Collections.Generic;

using IQToolkit.Data.Common;

namespace Deveel.Data.Linq {
	class SqlQueryExecutor : QueryExecutor {
		public SqlQueryExecutor(QueryProvider provider) {
			Provider = provider;
		}

		public QueryProvider Provider { get; private set; }

		public override object Convert(object value, Type type) {
			throw new NotImplementedException();
		}

		public override IEnumerable<T> Execute<T>(QueryCommand command, Func<FieldReader, T> fnProjector, MappingEntity entity, object[] paramValues) {
			throw new NotImplementedException();
		}

		public override IEnumerable<int> ExecuteBatch(QueryCommand query, IEnumerable<object[]> paramSets, int batchSize, bool stream) {
			throw new NotImplementedException();
		}

		public override IEnumerable<T> ExecuteBatch<T>(QueryCommand query, IEnumerable<object[]> paramSets, Func<FieldReader, T> fnProjector, MappingEntity entity,
			int batchSize, bool stream) {
			throw new NotImplementedException();
		}

		public override IEnumerable<T> ExecuteDeferred<T>(QueryCommand query, Func<FieldReader, T> fnProjector, MappingEntity entity, object[] paramValues) {
			throw new NotImplementedException();
		}

		public override int ExecuteCommand(QueryCommand query, object[] paramValues) {
			throw new NotImplementedException();
		}

		public override int RowsAffected { get; }
	}
}
