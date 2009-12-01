//  
//  Statement.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

using Deveel.Data.Sql;

namespace Deveel.Data {
	/// <summary>
	/// The expression that switches between given <see cref="CaseCondition">conditions</see>
	/// and return the value of the condition if that is evaluated as <c>true</c>.
	/// </summary>
	public sealed class CaseExpression : IStatementTreeObject {
		/// <summary>
		/// Constructs and empty <see cref="CaseExpression"/>.
		/// </summary>
		public CaseExpression() {
			conditions = new ArrayList();
		}

		private ArrayList conditions;
		private Expression elseExpression;

		/// <summary>
		/// Gets or sets the expression to evaluate if all the other 
		/// test conditions evaluate to false.
		/// </summary>
		public Expression Else {
			get { return elseExpression; }
			set { elseExpression = value; }
		}

		public CaseCondition this[int index] {
			get { return conditions[index] as CaseCondition; }
		}

		private bool HasTestCondition(Expression exp) {
			for (int i = 0; i < conditions.Count; i++) {
				CaseCondition condition = (CaseCondition) conditions[i];
				if (condition.TestCondition == exp)
					return true;
			}

			return false;
		}

		public void AddCondition(CaseCondition condition) {
			if (condition == null)
				throw new ArgumentNullException("condition");

			if (HasTestCondition(condition.TestCondition))
				throw new ArgumentException("Another condition with the same test already exists.");

			conditions.Add(condition);
		}

		#region Implementation of ICloneable

		public object Clone() {
			CaseExpression expression = new CaseExpression();
			expression.conditions = (ArrayList) conditions.Clone();
			if (elseExpression != null)
				expression.elseExpression = (Expression) elseExpression.Clone();
			return expression;
		}

		void IStatementTreeObject.PrepareExpressions(IExpressionPreparer preparer) {
			for (int i = 0; i < conditions.Count; i++) {
				CaseCondition condition = (CaseCondition) conditions[i];
				if (condition == null)
					continue;

				(condition as IStatementTreeObject).PrepareExpressions(preparer);
			}

			if (elseExpression != null)
				elseExpression.Prepare(preparer);
		}

		#endregion

		public TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			for (int i = 0; i < conditions.Count; i++) {
				CaseCondition condition = (CaseCondition) conditions[i];
				TObject testResult = condition.TestCondition.Evaluate(group, resolver, context);
				if (testResult.TType is TBooleanType &&
					(!testResult.IsNull && testResult == TObject.BooleanTrue))
					return condition.Result.Evaluate(group, resolver, context);
			}

			if (elseExpression != null)
				return elseExpression.Evaluate(group, resolver, context);

			return TObject.Null;
		}
	}
}