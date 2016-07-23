using System;
using System.Linq.Expressions;

using IQToolkit.Data.Common;

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
	}
}
