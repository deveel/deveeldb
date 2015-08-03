using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateUserStatement : SqlNonPreparableStatement {
		public CreateUserStatement(string userName, SqlExpression password) {
			if (password == null)
				throw new ArgumentNullException("password");
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");

			UserName = userName;
			Password = password;
		}

		public string UserName { get; private set; }

		public SqlExpression Password { get; private set; }


		public override ITable Execute(IQueryContext context) {
			var passwordText = Password.EvaluateToConstant(context, null).Value.ToString();

			try {
				context.CreateUser(UserName, passwordText);
				return FunctionTable.ResultTable(context, 0);
			} catch (Exception ex) {
				throw new StatementException(String.Format("Could not create user '{0}' because of an error", UserName), ex);
			}
		}
	}
}
