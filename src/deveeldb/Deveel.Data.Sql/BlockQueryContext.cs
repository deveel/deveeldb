using System;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql {
	class BlockQueryContext : ChildQueryContext, IBlockQueryContext {
		public BlockQueryContext(BlockExecuteContext executeContext, IQueryContext parentContext) 
			: base(parentContext) {
			ExecuteContext = executeContext;
		}

		protected BlockExecuteContext ExecuteContext { get; private set; }

		protected PlSqlBlock Block {
			get { return ExecuteContext.Block; }
		}

		public void Raise(string exceptionName) {
			throw new NotImplementedException();
		}

		public void ControlLoop(LoopControlType controlType, string label) {
			throw new NotImplementedException();
		}

		public void GoTo(string label) {
			if (String.IsNullOrEmpty(label))
				throw new ArgumentNullException("label");

			ExecuteContext.SetNextNodeTo(label);
		}
	}
}
