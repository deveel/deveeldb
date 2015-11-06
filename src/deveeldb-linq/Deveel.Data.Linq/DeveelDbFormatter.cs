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
			AddAliases(select.From);

			Write("SELECT ");

			if (select.IsDistinct)
				Write("DISTINCT ");

			WriteColumns(select.Columns);

			if (select.From != null) {
				WriteLine(Indentation.Same);
				Write("FROM ");
				VisitSource(select.From);
			}

			if (select.Where != null && 
				(select.GroupBy == null || select.GroupBy.Count ==0)) {
				WriteLine(Indentation.Same);
				Write("WHERE ");
				VisitPredicate(select.Where);
			}

			if (select.GroupBy != null && select.GroupBy.Count > 0) {
				WriteLine(Indentation.Same);
				Write("GROUP BY ");

				for (int i = 0, n = select.GroupBy.Count; i < n; i++) {
					if (i > 0) {
						Write(", ");
					}

					VisitValue(select.GroupBy[i]);
				}

				if (select.Where != null) {
					WriteLine(Indentation.Same);
					Write("HAVING ");
					VisitPredicate(select.Where);
				}
			}
			if (select.OrderBy != null && select.OrderBy.Count > 0) {
				WriteLine(Indentation.Same);
				Write("ORDER BY ");

				for (int i = 0, n = select.OrderBy.Count; i < n; i++) {
					OrderExpression exp = select.OrderBy[i];
					if (i > 0) {
						Write(", ");
					}

					VisitValue(exp.Expression);

					if (exp.OrderType != OrderType.Ascending)
						Write(" DESC");
				}
			}

			/*
			TODO: Implement LIMIT clause in the system...
			if (select.Take != null) {
				WriteLine(Indentation.Same);
				Write("LIMIT ");
				if (select.Skip == null) {
					Write("0");
				} else {
					Write(select.Skip);
				}

				Write(", ");
				Visit(select.Take);
			}
			*/

			return select;
		}

		protected override Expression VisitMemberAccess(MemberExpression m) {
			// TODO: Convert this to functions...
			return base.VisitMemberAccess(m);
		}
	}
}
