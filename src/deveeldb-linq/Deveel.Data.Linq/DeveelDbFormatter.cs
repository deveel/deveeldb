using System;
using System.Linq.Expressions;

using IQToolkit.Data.Common;

namespace Deveel.Data.Linq {
	class DeveelDbFormatter : SqlFormatter {
		public DeveelDbFormatter(DeveelDbLanguage language) 
			: base(language) {
		}

		public static new string Format(Expression expression) {
			return Format(expression, new DeveelDbLanguage());
		}

		public static string Format(Expression expression, DeveelDbLanguage language) {
			var formatter = new DeveelDbFormatter(language);
			formatter.Visit(expression);
			return formatter.ToString();
		}

		protected override Expression VisitSelect(SelectExpression @select) {
			// TODO:
			return base.VisitSelect(@select);
		}
	}
}
