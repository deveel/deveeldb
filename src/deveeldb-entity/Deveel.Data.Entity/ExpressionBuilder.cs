using System;
using System.Collections.Generic;
using System.Text;

namespace Deveel.Data.Entity {
	internal class ExpressionBuilder : Expression {
		public ExpressionBuilder() {
			Expressions = new List<Expression>();
		}

		public List<Expression> Expressions { get; private set; }

		public void Append(string s) {
			Expressions.Add(new LiteralExpression(s));
		}

		public void Append(Expression s) {
			Expressions.Add(s);
		}


		internal override void WriteTo(StringBuilder sb) {
			for (int i = 0; i < Expressions.Count; i++)
				Expressions[i].WriteTo(sb);
		}
	}
}