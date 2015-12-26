using System;

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropUserStatement : SqlStatement {
		public DropUserStatement(string userName) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");

			UserName = userName;
		}

		private DropUserStatement(ObjectData data) {
			UserName = data.GetString("UserName");
		}

		public string UserName { get; private set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			base.ExecuteStatement(context);
		}

		protected override void GetData(SerializeData data) {
			data.SetValue("UserName", UserName);
		}
	}
}