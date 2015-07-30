using System;

using Deveel.Data.DbSystem;
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
			throw new NotImplementedException();
		}
	}
}
