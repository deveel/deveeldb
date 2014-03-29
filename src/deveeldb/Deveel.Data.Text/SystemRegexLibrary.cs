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
using System.Text.RegularExpressions;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Text {
	/// <summary>
	/// The default implementation of the system regular expression library.
	/// </summary>
	class SystemRegexLibrary : IRegexLibrary {
		#region Public Methods
		public bool RegexMatch(string regularExpression, string expressionOps, string value) {
			RegexOptions options = RegexOptions.None;

			if (expressionOps != null) {
				if (expressionOps.IndexOf('i') != -1) {
					options |= RegexOptions.IgnoreCase;
				}
				if (expressionOps.IndexOf('s') != -1) {
					options |= RegexOptions.Singleline;
				}
				if (expressionOps.IndexOf('m') != -1) {
					options |= RegexOptions.Multiline;
				}
			}

			Regex regex = new Regex(regularExpression, options);
			return regex.IsMatch(value);
		}

		public IList<int> RegexSearch(Table table, int column, string regularExpression, string expressionOps) {
			// Get the ordered column,
			IList<int> row_list = table.SelectAll(column);
			// The result matched rows,
			List<int> result_list = new List<int>();

			// Make into a new list that matches the pattern,
			Regex regex;

			try {
				RegexOptions options = RegexOptions.None;
				if (expressionOps != null) {
					if (expressionOps.IndexOf('i') != -1) {
						options |= RegexOptions.IgnoreCase;
					}
					if (expressionOps.IndexOf('s') != -1) {
						options |= RegexOptions.Singleline;
					}
					if (expressionOps.IndexOf('m') != -1) {
						options |= RegexOptions.Multiline;
					}
				}

				regex = new Regex(regularExpression, options);
			} catch (Exception) {
				// Incorrect syntax means we always match to an empty list,
				return result_list;
			}

			// For each row in the column, test it against the regular expression.
			int size = row_list.Count;
			for (int i = 0; i < size; ++i) {
				int row_index = row_list[i];
				TObject cell = table.GetCell(column, row_index);
				// Only try and match against non-null cells.
				if (!cell.IsNull) {
					Object ob = cell.Object;
					String str = ob.ToString();
					// If the column matches the regular expression then return it,
					if (regex.IsMatch(str)) {
						result_list.Add(row_index);
					}
				}
			}

			return result_list;
		}
		#endregion
	}
}