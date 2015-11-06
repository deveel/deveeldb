using System;
using System.Linq.Expressions;
using System.Reflection;

using IQToolkit;
using IQToolkit.Data.Common;

namespace Deveel.Data.Linq {
	class DeveelDbLanguage : QueryLanguage {
		private readonly DeveelDbTypeSystem typeSystem;

		public DeveelDbLanguage() {
			typeSystem = new DeveelDbTypeSystem();
		}

		public override Expression GetGeneratedIdExpression(MemberInfo member) {
			// TODO: Get the mapped table name corresponding to the type reflecting
			//       the member, to invoke the function "LAST_UNIKE_KEY('table_name')"
			string tableName = "";
			var args = new Expression[] {Expression.Constant(tableName, typeof (string))};
            return new FunctionExpression(TypeHelper.GetMemberType(member), "last_unique_key", args);
		}

		public override QueryTypeSystem TypeSystem {
			get { return typeSystem; }
		}

		public override QueryLinguist CreateLinguist(QueryTranslator translator) {
			return new DeveelDbLinguist(this, translator);
		}

		#region DeveelDbLinguist

		class DeveelDbLinguist : QueryLinguist {
			public DeveelDbLinguist(DeveelDbLanguage language, QueryTranslator translator) 
				: base(language, translator) {
			}

			public override Expression Translate(Expression expression) {
				// fix up any order-by's
				expression = OrderByRewriter.Rewrite(Language, expression);
				expression = base.Translate(expression);
				expression = UnusedColumnRemover.Remove(expression);

				return expression;
			}

			public override string Format(Expression expression) {
				return DeveelDbFormatter.Format(expression);
			}
		}

		#endregion
	}
}
