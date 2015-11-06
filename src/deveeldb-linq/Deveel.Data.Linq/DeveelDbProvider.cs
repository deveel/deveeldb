using System;
using System.Data.Common;

using Deveel.Data.Client;
using Deveel.Data.Sql.Statements;

using IQToolkit.Data;
using IQToolkit.Data.Common;

namespace Deveel.Data.Linq {
	class DeveelDbProvider : EntityProvider {
		private readonly IQueryContext context;

		public DeveelDbProvider(IQueryContext context, QueryMapping mapping, QueryPolicy policy) 
			: base(new DeveelDbLanguage(), mapping, policy) {
			this.context = context;
		}

		protected override QueryExecutor CreateExecutor() {
			return new DeveelDbExecutor(context);
		}

		public override void DoTransacted(Action action) {
			throw new NotImplementedException();
		}

		public override void DoConnected(Action action) {
			action();
		}

		public override int ExecuteCommand(string commandText) {
			// TODO: what to return here?
			context.ExecuteQuery(commandText);
			return -1;
		}
	}
}
