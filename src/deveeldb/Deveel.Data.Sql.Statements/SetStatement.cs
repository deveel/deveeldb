using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Statements {
	public sealed class SetStatement : SqlStatement {
		public SetStatement(string settingName, SqlExpression valueExpression) {
			if (String.IsNullOrEmpty(settingName))
				throw new ArgumentNullException("settingName");
			if (valueExpression == null)
				throw new ArgumentNullException("valueExpression");

			SettingName = settingName;
			ValueExpression = valueExpression;
		}

		public string SettingName { get; private set; }

		public SqlExpression ValueExpression { get; private set; }

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var value = ValueExpression.Prepare(preparer);
			return new SetStatement(SettingName, value);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			context.Request.Query.Session.Transaction.Context.SetVariable(SettingName, ValueExpression);
		}
	}
}
