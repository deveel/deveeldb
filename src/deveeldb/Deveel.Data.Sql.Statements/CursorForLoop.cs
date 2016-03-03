using System;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class CursorForLoop : LoopBlock {
		public CursorForLoop(string indexName, string cursorName) {
			if (String.IsNullOrEmpty(indexName))
				throw new ArgumentNullException("indexName");
			if (String.IsNullOrEmpty(cursorName))
				throw new ArgumentNullException("cursorName");

			IndexName = indexName;
			CursorName = cursorName;
		}

		public string IndexName { get; private set; }

		public string CursorName { get; private set; }

		protected override void BeforeLoop(ExecutionContext context) {
			// TODO: define the index variable into the context

			base.BeforeLoop(context);
		}

		protected override bool Loop(ExecutionContext context) {
			// TODO: Get the cursor and check if it is still enumerating

			return base.Loop(context);
		}

		protected override void AfterLoop(ExecutionContext context) {
			// TODO: Get the index variable from the context
			// TODO: Increment the value of the variable
			// TODO: Redefine the value of the variable into the context
			// TODO: Advance the cursor to the next element

			base.AfterLoop(context);
		}
	}
}
