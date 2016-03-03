// 
//  Copyright 2010-2015 Deveel
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
//


using System;

namespace Deveel.Data.Sql.Expressions {
	/// <summary>
	/// An error occurring while evaluating an <see cref="SqlExpression"/>.
	/// </summary>
	/// <seealso cref="SqlExpression.Evaluate(Deveel.Data.Sql.Expressions.EvaluateContext)"/>
	public class ExpressionEvaluateException : SqlExpressionException {
		/// <summary>
		/// Constructs a new exception with no detailed message.
		/// </summary>
		public ExpressionEvaluateException() 
			: this("An error occurred while evaluating a SQL Expression.") {
		}

		/// <summary>
		/// Constructs an exception with a message detailing the error.
		/// </summary>
		/// <param name="message">The details message of the exception.</param>
		public ExpressionEvaluateException(string message) 
			: this(message, null) {
		}

		/// <summary>
		/// Constructs a new exception with a detailed message and the reference
		/// to an originating error.
		/// </summary>
		/// <param name="message">The details message of the exception.</param>
		/// <param name="innerException">The inner error of this exception.</param>
		public ExpressionEvaluateException(string message, Exception innerException) 
			: base(ExpressionErrorCodes.EvaluateError, message, innerException) {
		}
	}
}