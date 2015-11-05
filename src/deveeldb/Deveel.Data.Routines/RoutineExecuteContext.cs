using System;
using System.Collections.Generic;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Routines {
	class RoutineExecuteContext : BlockExecuteContext {
		public RoutineExecuteContext(RoutineBody body, IEnumerable<SqlStatement> statements) 
			: base(body, statements) {
		}

		protected RoutineBody Body {
			get { return Block as RoutineBody; }
		}


		protected RoutineInfo RoutineInfo {
			get { return Body.RoutineInfo; }
		}

		public SqlExpression ReturnExpression { get; private set; }

		public bool HasReturn { get; private set; }

		protected override IQueryContext CreateQueryContext(IQueryContext parentContext) {
			if (RoutineInfo.RoutineType == RoutineType.Function)
				return new FunctionQueryContext(this, parentContext);

			return new RoutineQueryContext(this, parentContext);
		}

		private void SetReturn(SqlExpression expression) {
			if (RoutineInfo.RoutineType != RoutineType.Function)
				throw new InvalidOperationException(String.Format("The routine '{0}' is not a function.", RoutineInfo.RoutineName));

			if (HasReturn)
				throw new InvalidOperationException(string.Format("Function '{0}' is already returned.", RoutineInfo.RoutineName));

			ReturnExpression = expression;
			HasReturn = true;
		}


		#region FunctionQueryContext

		class FunctionQueryContext : RoutineQueryContext, IFunctionQueryContext {
			private RoutineExecuteContext context;

			public FunctionQueryContext(RoutineExecuteContext executeContext, IQueryContext parentContext) 
				: base(executeContext, parentContext) {
				context = executeContext;
			}

			public void SetReturn(SqlExpression expression) {
				context.SetReturn(expression);
			}

			protected override void Dispose(bool disposing) {
				context = null;
				base.Dispose(disposing);
			}
		}

		#endregion

		#region RoutineQueryContext

		class RoutineQueryContext : BlockQueryContext {
			public RoutineQueryContext(BlockExecuteContext executeContext, IQueryContext parentContext) 
				: base(executeContext, parentContext) {
			}
		}

		#endregion
	}
}
