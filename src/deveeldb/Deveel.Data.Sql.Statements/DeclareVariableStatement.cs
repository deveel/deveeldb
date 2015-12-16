using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Statements {
	public sealed class DeclareVariableStatement : SqlStatement, IPreparable {
		public DeclareVariableStatement(string variableName, SqlType variableType) {
			if (String.IsNullOrEmpty(variableName))
				throw new ArgumentNullException("variableName");
			if (variableType == null)
				throw new ArgumentNullException("variableType");

			VariableName = variableName;
			VariableType = variableType;
		}

		public string VariableName { get; private set; }

		public SqlType VariableType { get; private set; }

		public bool IsConstant { get; set; }

		public SqlExpression DefaultExpression { get; set; }

		public bool IsNotNull { get; set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var statement = new DeclareVariableStatement(VariableName, VariableType);
			if (DefaultExpression != null)
				statement.DefaultExpression = DefaultExpression.Prepare(preparer);

			statement.IsConstant = IsConstant;
			return statement;
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			throw new NotImplementedException();
		}
	}
}
