//  
//  CaseCondition.cs
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