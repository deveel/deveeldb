// 
//  Copyright 2010  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.


using System;
using System.Collections;
using System.Text;

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

		/// <summary>
		/// Gets the <see cref="CaseCondition"/> at the index given
		/// within the expression.
		/// </summary>
		/// <param name="index">The index of the condition to get.</param>
		/// <returns>
		/// Returns a <see cref="CaseCondition"/> object at the given index
		/// within the expresison.
		/// </returns>
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

		public override string ToString() {
			StringBuilder sb = new StringBuilder();
			sb.Append("CASE ");
			for (int i = 0; i < conditions.Count; i++) {
				CaseCondition condition = (CaseCondition) conditions[i];
				sb.Append("WHEN ");
				sb.Append(condition.TestCondition.Text.ToString());
				sb.Append("THEN ");
				sb.Append(condition.Result.Text.ToString());
				sb.Append(" ");
			}

			if (elseExpression != null)
				sb.Append("ELSE ").Append(elseExpression.Text.ToString());

			return sb.ToString();
		}
	}
}