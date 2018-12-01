using System;
using System.Threading.Tasks;

using Deveel.Data.Security;
using Deveel.Data.Sql.Statements.Security;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateUserStatement : SqlStatement {
		public CreateUserStatement(string userName, IUserIdentificationInfo identificationInfo) {
			UserName = userName ?? throw new ArgumentNullException(nameof(userName));
			IdentificationInfo = identificationInfo ?? throw new ArgumentNullException(nameof(identificationInfo));
		}

		public string UserName { get; }

		public IUserIdentificationInfo IdentificationInfo { get; }

		protected override async Task ExecuteStatementAsync(StatementContext context) {
			if (!User.IsValidName(UserName))
				throw new SqlStatementException($"The specified name '{UserName}' is invalid for a user");

			var securityManager = context.GetService<ISecurityManager>();
			if (securityManager == null)
				throw new SystemException("There is no security manager defined in the system");

			if (await securityManager.UserExistsAsync(UserName))
				throw new SqlStatementException($"A user named '{UserName}' already exists.");

			await securityManager.CreateUserAsync(UserName, IdentificationInfo);
		}

		protected override void Require(IRequirementCollection requirements) {
			requirements.Require(x => x.UserIsAdmin());
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("CREATE USER ");
			builder.Append(UserName);
			builder.Append(" IDENTIFIED ");

			if (IdentificationInfo is PasswordIdentificationInfo) {
				builder.Append("BY '<password>'");
			} else {
				throw new NotSupportedException();
			}
		}
	}
}