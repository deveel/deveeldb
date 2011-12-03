//  
//  SystemRegexLibrary.cs
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

using Deveel.Data.Collections;

using Deveel.Text.RegularExpressions;

namespace Deveel.Data.Text {
	internal class DeveelRegexLibrary : IRegexLibrary {
		public bool RegexMatch(string regularExpression, string expressionOps, string value) {
			RegexFlags flags = RegexFlags.None;

			if (expressionOps != null) {
				if (expressionOps.IndexOf('i') != -1) {
					flags |= RegexFlags.IgnoreCase;
				}
				if (expressionOps.IndexOf('s') != -1) {
					flags |= RegexFlags.DotNewLine;
				}
				if (expressionOps.IndexOf('m') != -1) {
					flags |= RegexFlags.MultiLine;
				}
			}

			Regex regex = new Regex(regularExpression, flags);
			return regex.IsMatch(value);
		}

		public IntegerVector RegexSearch(Table table, int column, string regularExpression, string expressionOps) {
			// Get the ordered column,
			IntegerVector row_list = table.SelectAll(column);
			// The result matched rows,
			IntegerVector result_list = new IntegerVector();

			// Make into a new list that matches the pattern,
			Regex regex;

			try {
				RegexFlags flags = RegexFlags.None;
				if (expressionOps != null) {
					if (expressionOps.IndexOf('i') != -1) {
						flags |= RegexFlags.IgnoreCase;
					}
					if (expressionOps.IndexOf('s') != -1) {
						flags |= RegexFlags.DotNewLine;
					}
					if (expressionOps.IndexOf('m') != -1) {
						flags |= RegexFlags.MultiLine;
					}
				}

				regex = new Regex(regularExpression, flags);
			} catch (RegexException) {
				// Incorrect syntax means we always match to an empty list,
				return result_list;
			}

			// For each row in the column, test it against the regular expression.
			int size = row_list.Count;
			for (int i = 0; i < size; ++i) {
				int row_index = row_list[i];
				TObject cell = table.GetCellContents(column, row_index);
				// Only try and match against non-null cells.
				if (!cell.IsNull) {
					Object ob = cell.Object;
					String str = ob.ToString();
					// If the column matches the regular expression then return it,
					if (regex.IsMatch(str)) {
						result_list.AddInt(row_index);
					}
				}
			}

			return result_list;
		}
	}
}