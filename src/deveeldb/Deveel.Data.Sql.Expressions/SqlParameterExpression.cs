using System;

namespace Deveel.Data.Sql.Expressions {
	public sealed class SqlParameterExpression : SqlExpression {
		public SqlParameterExpression() 
			: this(QueryParameter.Marker) {
		}

		public SqlParameterExpression(string parameterName) {
			if (String.IsNullOrEmpty(parameterName))
				throw new ArgumentNullException("parameterName");

			ParameterName = parameterName;
		}

		public string ParameterName { get; private set; }

		public bool IsMarker {
			get { return String.Equals(ParameterName, QueryParameter.Marker); }
		}

		public override SqlExpressionType ExpressionType {
			get { return SqlExpressionType.Parameter; }
		}
	}
}
