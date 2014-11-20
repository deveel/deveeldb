// 
//  Copyright 2010-2014 Deveel
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

namespace Deveel.Data.Sql.Expressions {
	/// <summary>
	/// Lists all the codes of errors in the <c>expressions</c> domain
	/// </summary>
	public enum ExpressionErrorCodes : int {
		/// <summary>
		/// An unknown error that was not handled.
		/// </summary>
		Unknown = 0x450000,

		/// <summary>
		/// An error occurred while evaluating the expression.
		/// </summary>
		EvaluateError = 0x06770440,

		/// <summary>
		/// The evaluator was not able to reduce the expression
		/// to a simpler form.
		/// </summary>
		UnableToReduce = 0x03999400,

		/// <summary>
		/// A variable defined in a <see cref="SqlReferenceExpression"/> was
		/// not found within the execution context.
		/// </summary>
		VariableNotFound = 0x03004003,

		/// <summary>
		/// The expression parser could not parse a given input string
		/// </summary>
		CannotParse = 0x08778290
	}
}