using System;
using System.Data.Common;

using Deveel.Data.Client;

using IQToolkit.Data;
using IQToolkit.Data.Common;

namespace Deveel.Data.Linq {
	class DeveelDbProvider : DbEntityProvider {
		public DeveelDbProvider(DbConnection connection, QueryMapping mapping, QueryPolicy policy) 
			: base(connection, new DeveelDbLanguage(), mapping, policy) {
		}

		public override DbEntityProvider New(DbConnection connection, QueryMapping mapping, QueryPolicy policy) {
			return new DeveelDbProvider(connection, mapping, policy);
		}

		protected override QueryExecutor CreateExecutor() {
			return new DeveelDbExecutor(this);
		}

		#region Executor

		class DeveelDbExecutor : DbEntityProvider.Executor {
			public DeveelDbExecutor(DbEntityProvider provider) 
				: base(provider) {
			}

			public override int ExecuteCommand(QueryCommand query, object[] paramValues) {
				return base.ExecuteCommand(query, paramValues);
			}

			protected override void AddParameter(DbCommand command, QueryParameter parameter, object value) {
				base.AddParameter(command, parameter, value);
			}
		}

		#endregion
	}
}
