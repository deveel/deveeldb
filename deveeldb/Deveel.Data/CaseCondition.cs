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

using Deveel.Data.Sql;

namespace Deveel.Data {
	/// <summary>
	/// A simple condition in a <c>CASE</c> statement.
	/// </summary>
	public sealed class CaseCondition : IStatementTreeObject {
		/// <summary>
		/// Constructs a <see cref="CaseCondition"/> with a given test
		/// condition and the eventual result.
		/// </summary>
		/// <param name="testCondition">The condition which has to be 
		/// evaluated as <c>true</c> to return the given result.</param>
		/// <param name="result">The result of the condition if the <paramref name="testCondition"/>
		/// is evaluated to <c>true</c>.</param>
		public CaseCondition(Expression testCondition, Expression result) {
			if (testCondition == null)
				throw new ArgumentNullException("testCondition");
			if (result == null)
				throw new ArgumentNullException("result");

			this.testCondition = testCondition;
			this.result = result;
		}

		private readonly Expression testCondition;
		private readonly Expression result;

		/// <summary>
		/// Gets the result expression of the condition if
		/// <see cref="TestCondition"/> is evaluated to <c>true</c>.
		/// </summary>
		public Expression Result {
			get { return result; }
		}

		/// <summary>
		/// Gets the condition to evaluate.
		/// </summary>
		public Expression TestCondition {
			get { return testCondition; }
		}

		#region Implementation of ICloneable

		public object Clone() {
			return new CaseCondition((Expression) testCondition.Clone(), (Expression) result.Clone());
		}

		void IStatementTreeObject.PrepareExpressions(IExpressionPreparer preparer) {
			testCondition.Prepare(preparer);
			result.Prepare(preparer);
		}

		#endregion
	}
}