using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class SetUserStatusStatement : SqlStatement, IPreparedStatement {
		public SetUserStatusStatement(string userName, UserStatus status) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");

			UserName = userName;
			Status = status;
		}

		public string UserName { get; private set; }

		public UserStatus Status { get; private set; }

		protected override IPreparedStatement PrepareStatement(IExpressionPreparer preparer, IQueryContext context) {
			return this;
		}
		IStatement IPreparedStatement.Source {
			get { return this; }
		}

		public ITable Execute(IQueryContext context) {
			throw new NotImplementedException();
		}
	}
}
