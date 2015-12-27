using System;

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class AssignVariableStatement : SqlStatement {
		public AssignVariableStatement(SqlExpression varRef, SqlExpression valueExpression) {
			if (varRef == null)
				throw new ArgumentNullException("varRef");
			if (valueExpression == null)
				throw new ArgumentNullException("valueExpression");

			if (!(varRef is SqlReferenceExpression) &&
				!(varRef is SqlVariableReferenceExpression))
				throw new ArgumentException("Reference expression not supported.");

			VariableReference = varRef;
			ValueExpression = valueExpression;
		}

		private AssignVariableStatement(ObjectData data)
			: base(data) {
			VariableReference = data.GetValue<SqlExpression>("Variable");
			ValueExpression = data.GetValue<SqlExpression>("Value");
		}

		public SqlExpression VariableReference { get; private set; }

		public SqlExpression ValueExpression { get; private set; }

		protected override void GetData(SerializeData data) {
			data.SetValue("Variable", VariableReference);
			data.SetValue("Value", ValueExpression);
		}
	}
}
