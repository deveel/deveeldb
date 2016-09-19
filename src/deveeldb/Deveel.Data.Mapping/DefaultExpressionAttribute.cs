using System;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
	public sealed class DefaultExpressionAttribute : DefaultAttribute {
		public DefaultExpressionAttribute(string expression)
			: base(expression, ColumnDefaultType.Expression) {
			if (String.IsNullOrEmpty(expression))
				throw new ArgumentNullException("expression");
		}

		public string Expression {
			get { return (string) Value; }
		}
	}
}
