using System;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public class LoopBlock : CodeBlock {
		protected virtual bool Loop(ExecutionContext context) {
			return true;
		}

		protected virtual void BeforeLoop(ExecutionContext context) {
		}

		protected virtual void AfterLoop(ExecutionContext context) {
		}

		protected override void Execute(ExecutionContext context) {
			BeforeLoop(context);

			while (Loop(context)) {
				base.Execute(context);
			}

			AfterLoop(context);
		}
	}
}
