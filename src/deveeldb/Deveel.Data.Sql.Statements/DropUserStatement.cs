using System;

using Deveel.Data.Security;
using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DropUserStatement : SqlStatement {
		public DropUserStatement(string userName) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");
			if (String.Equals(userName, User.PublicName, StringComparison.OrdinalIgnoreCase) ||
				String.Equals(userName, User.SystemName, StringComparison.OrdinalIgnoreCase))
				throw new ArgumentException(String.Format("User '{0}' is reserved and cannot be dropped.", userName));

			UserName = userName;
		}

		private DropUserStatement(ObjectData data) {
			UserName = data.GetString("UserName");
		}

		public string UserName { get; private set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.Request.Query.UserCanDropUser(UserName))
				throw new SecurityException(String.Format("The user '{0}' has not enough rights to drop the other user '{1}'",
					context.Request.Query.UserName(), UserName));

			if (!context.Request.Query.UserExists(UserName))
				throw new InvalidOperationException(String.Format("The user '{0}' does not exist: cannot delete.", UserName));

			context.Request.Query.DeleteUser(UserName);
		}

		protected override void GetData(SerializeData data) {
			data.SetValue("UserName", UserName);
		}
	}
}