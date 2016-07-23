using System;
using System.Linq.Expressions;

using IQToolkit.Data.Common;

namespace Deveel.Data.Linq {
	class DeveelDbLinguist : QueryLinguist {
		public DeveelDbLinguist(DeveelDbQueryLanguage language, QueryTranslator translator) 
			: base(language, translator) {
		}

		public override Expression Translate(Expression expression) {
			// fix up any order-by's
			expression = OrderByRewriter.Rewrite(this.Language, expression);

			expression = base.Translate(expression);

			expression = UnusedColumnRemover.Remove(expression);

			//expression = DistinctOrderByRewriter.Rewrite(expression);

			return expression;
		}

		public override string Format(Expression expression) {
			return DeveelDbQueryFormatter.FormatExpression(expression);
		}
	}
}
