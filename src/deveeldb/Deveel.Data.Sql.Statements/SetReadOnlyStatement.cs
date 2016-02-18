using System;

using Deveel.Data.Serialization;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class SetReadOnlyStatement : SqlStatement {
		public SetReadOnlyStatement() 
			: this(true) {
		}

		public SetReadOnlyStatement(bool status) {
			Status = status;
		}

		private SetReadOnlyStatement(ObjectData data) {
			Status = data.GetBoolean("Status");
		}

		public bool Status { get; private set; }

		protected override void GetData(SerializeData data) {
			data.SetValue("Status", Status);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			context.Query.Context.SessionContext.TransactionContext.ReadOnly(Status);
		}
	}
}
