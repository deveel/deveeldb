// 
//  Copyright 2010-2018 Deveel
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

namespace Deveel.Data.Sql.Statements {
	/// <summary>
	/// The result of the execution of a statement that
	/// encapsulates a static value.
	/// </summary>
	public sealed class StatementScalarResult : IStatementResult {
		/// <summary>
		/// Constructs the result with the given value.
		/// </summary>
		/// <param name="value">The value to be returned from the result</param>
		/// <exception cref="ArgumentNullException">If the given <paramref name="value"/>
		/// is <c>null</c>.</exception>
		public StatementScalarResult(SqlObject value) {
			if (value == null)
				throw new ArgumentNullException(nameof(value));

			Value = value;
		}

		/// <summary>
		/// Gets the expression that represents the result
		/// of the execution of a statement
		/// </summary>
		public SqlObject Value { get; }
	}
}