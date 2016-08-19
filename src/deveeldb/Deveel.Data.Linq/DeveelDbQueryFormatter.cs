using System;
using System.Linq.Expressions;

using IQToolkit.Data.Common;

using QueryParameter = Deveel.Data.Sql.QueryParameter;

namespace Deveel.Data.Linq {
	class DeveelDbQueryFormatter : SqlFormatter {
		public DeveelDbQueryFormatter(DeveelDbQueryLanguage language) 
			: base(language) {
		}

		public static string FormatExpression(Expression expression) {
			var formatter = new DeveelDbQueryFormatter(new DeveelDbQueryLanguage());
			formatter.Visit(expression);
			return formatter.ToString();
		}

		protected override Expression VisitSelect(SelectExpression @select) {
			return base.VisitSelect(@select);
		}

		protected override void WriteParameterName(string name) {
			if (name == QueryParameter.Marker) {
				Write(QueryParameter.Marker);
			} else {
				Write(String.Format("{0}{1}", QueryParameter.NamePrefix, name));
			}
		}
	}
}
