using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Statements {
	public sealed class RaiseStatement : SqlStatement {
		public RaiseStatement() 
			: this(null) {
		}

		public RaiseStatement(string exceptionName) {
			ExceptionName = exceptionName;
		}

		protected override bool IsPreparable {
			get { return false; }
		}

		public string ExceptionName { get; set; }

		protected override ITable ExecuteStatement(IQueryContext context) {
			return base.ExecuteStatement(context);
		}
	}
}
