using System;

namespace Deveel.Data.Sql.Statements {
	public sealed class DropUserStatement : SqlStatement {
		public DropUserStatement(string userName) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");

			UserName = userName;
		}

		public string UserName { get; private set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			base.ExecuteStatement(context);
		}
	}
}