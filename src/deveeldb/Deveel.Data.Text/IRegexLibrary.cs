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
using System.Collections.Generic;

using Deveel.Data.Collections;

namespace Deveel.Data.Text {
	/// <summary>
	/// An interface that links with a Regex library. 
	/// </summary>
	/// <remarks>
	/// This interface allows the database engine to use any regular expression 
	/// library that this interface can be implemented for.
	/// </remarks>
	public interface IRegexLibrary {
		/// <summary>
		/// Matches a regular expression against a string value.
		/// </summary>
		/// <param name="regularExpression">The regular expression to match.</param>
		/// <param name="expressionOps">The expression options string that 
		/// specifies various flags.</param>
		/// <param name="value">The string to test.</param>
		/// <returns>
		/// If the value is a match against the expression then it returns <b>true</b>,
		/// otherwise it returns <b>false</b>.
		/// </returns>
		bool RegexMatch(string regularExpression, string expressionOps, string value);

		/// <summary>
		/// Performs a regular expression search on the given column of the table.
		/// </summary>
		/// <param name="table">The table to search for matching values.</param>
		/// <param name="column">The column of the table to search for matching values.</param>
		/// <param name="regularExpression">The expression to match (eg. "[0-9]+").</param>
		/// <param name="expressionOps">Expression operator string that specifies 
		/// various flags.</param>
		/// <returns>
		/// Returns an <see cref="IntegerVector"/> that contains the list of rows 
		/// in the table that matched the expression, or an empty list if the 
		/// expression matched no rows in the column.
		/// </returns>
		IList<int> RegexSearch(Table table, int column, string regularExpression, string expressionOps);
	}
}