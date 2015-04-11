using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Expressions {
	public sealed class SqlVariableReferenceExpression : SqlExpression {
		internal SqlVariableReferenceExpression(string variableName) {
			if (String.IsNullOrEmpty(variableName))
				throw new ArgumentNullException("variableName");

			VariableName = variableName;
		}

		public override SqlExpressionType ExpressionType {
			get { return SqlExpressionType.VariableReference; }
		}

		public string VariableName { get; private set; }

		public override bool CanEvaluate {
			get { return true; }
		}

		public override SqlExpression Evaluate(EvaluateContext context) {
			DataObject value;

			try {
				// TODO: check if the context handles variables before using the variable resolver
				value = context.VariableResolver.Resolve(new ObjectName(VariableName));
			} catch (ObjectNotFoundException) {
				value = DataObject.Null();
			}

			return Constant(value);
		}
	}
}
