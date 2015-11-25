using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data {
	public sealed class BlockExecuteContext {
		public BlockExecuteContext(IQuery query) 
			: this(query, null) {
		}

		public BlockExecuteContext(IQuery query, IVariableResolver resolver) {
			Query = query;
			VariableResolver = resolver;
		}

		public IQuery Query { get; private set; }

		public IVariableResolver VariableResolver { get; private set; }

		public ITable Result { get; private set; }

		public bool HasResult { get; private set; }

		public void SetResult(ITable table) {
			Result = table;
			HasResult = true;
		}
	}
}
