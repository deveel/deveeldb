using System;
using System.Collections.Generic;

namespace Deveel.Data.Entity {
	internal class ExpressionScope {
		private Dictionary<string, BranchExpression> scopeTable = new Dictionary<string, BranchExpression>();

		public void Add(string name, BranchExpression expression) {
			scopeTable.Add(name, expression);
		}

		public void Remove(BranchExpression expression) {
			if (expression == null) return;
			if (expression.Name != null)
				scopeTable.Remove(expression.Name);

			if (expression is SelectExpression)
				Remove((expression as SelectExpression).From);
			else if (expression is JoinExpression) {
				JoinExpression j = expression as JoinExpression;
				Remove(j.Left);
				Remove(j.Right);
			} else if (expression is CompositeExpression) {
				CompositeExpression u = expression as CompositeExpression;
				Remove(u.Left);
				Remove(u.Right);
			}
		}

		public BranchExpression GetExpression(string name) {
			if (!scopeTable.ContainsKey(name))
				return null;
			return scopeTable[name];
		}
	}
}