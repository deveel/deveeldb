using System;

using Deveel.Data.Serialization;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class SetIsolationLevelStatement : SqlStatement {
		public SetIsolationLevelStatement(IsolationLevel isolationLevel) {
			IsolationLevel = isolationLevel;
		}

		private SetIsolationLevelStatement(ObjectData data) {
			IsolationLevel = (IsolationLevel) data.GetValue<int>("IsolationLevel");
		}

		public IsolationLevel IsolationLevel { get; private set; }

		protected override void GetData(SerializeData data) {
			data.SetValue("IsolationLevel", (int)IsolationLevel);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			// TODO: chage the isolation level into the parent transaction
			base.ExecuteStatement(context);
		}
	}
}
