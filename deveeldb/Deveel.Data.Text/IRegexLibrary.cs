//  
//  IRegexLibrary.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
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
		IntegerVector RegexSearch(Table table, int column, string regularExpression, string expressionOps);
	}
}