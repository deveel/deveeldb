// 
//  PatternSearch.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Text;

using Deveel.Data.Collections;

namespace Deveel.Data {
	/// <summary>
	/// This is a static class that performs the operations to do a pattern 
	/// search on a given column of a table.
	/// </summary>
	/// <remarks>
	/// The pattern syntax is very simple and follows that of the SQL standard.
	/// <para>
	/// It works as follows:
	/// The '%' character represents any sequence of characters.
	/// The '_' character represents some character.
	/// </para>
	/// <para>
	/// Therefore, the pattern search <c>Anto%</c> will find all rows that 
	/// start with the string <c>Anto</c> and end with any sequence of characters. 
	/// The pattern <c>A% Proven%</c> will find all names starting with <i>A</i> 
	/// and containing <i>Proven</i> somewhere in the end. The pattern <i>_at</i> 
	/// will find all three letter words ending with <i>at</i>.
	/// </para>
	/// <para>
	/// <b>Note</b> A <c>ab%</c> type search is faster than a <c>%bc</c> type 
	/// search. If the start of the search pattern is unknown then the entire 
	/// contents of the column need to be accessed.
	/// </para>
	/// </remarks>
	public sealed class PatternSearch {

		// Statics for the tokens.
		private const char ZERO_OR_MORE_CHARS = '%';
		private const char ONE_CHAR = '_';

		/// <summary>
		/// Returns true if the given character is a wild card (unknown).
		/// </summary>
		/// <param name="ch"></param>
		/// <returns></returns>
		private static bool IsWildCard(char ch) {
			return (ch == ONE_CHAR || ch == ZERO_OR_MORE_CHARS);
		}

		/// <summary>
		/// Matches a pattern against a string and returns true if it matches 
		/// or false otherwise.
		/// </summary>
		/// <remarks>
		/// This matches patterns that do not necessarily start with a wild 
		/// card unlike the <see cref="PatternMatch"/> method.
		/// </remarks>
		public static bool FullPatternMatch(String pattern, String str,
											   char escape_char) {
			StringBuilder start = new StringBuilder();
			String rezt = null;
			int len = pattern.Length;
			int i = 0;
			bool last_escape_char = false;
			for (; i < len && rezt == null; ++i) {
				char c = pattern[i];
				if (last_escape_char) {
					last_escape_char = false;
					start.Append(c);
				} else if (c == escape_char) {
					last_escape_char = true;
				} else if (IsWildCard(c)) {
					rezt = pattern.Substring(i);
				} else {
					start.Append(c);
				}
			}

			if (rezt == null) {
				rezt = "";
			}

			String st = start.ToString();

			//    Console.Out.WriteLine("--");
			//    Console.Out.WriteLine(str);
			//    Console.Out.WriteLine(st);

			if (str.StartsWith(st)) {
				String str_rezt = str.Substring(st.Length); // (i)

				if (rezt.Length > 0)
					return PatternMatch(rezt, str_rezt, escape_char);
				return str_rezt.Length == 0;
			}
			return false;
		}

		/// <summary>
		/// This is the pattern match recurrsive method.
		/// </summary>
		/// <remarks>
		/// It recurses on each wildcard expression in the pattern which makes 
		/// for slightly better efficiency than a character recurse algorithm.
		/// However, patterns such as <c>_%_a</c> will result in many recursive 
		/// calls.
		/// <para>
		/// <b>Note</b> That <c>_%_</c> will be less efficient than <c>__%</c> 
		/// and will produce the same result.
		/// </para>
		/// <para>
		/// <b>Note</b> It requires that a wild card character is the first 
		/// character in the expression.
		/// </para>
		/// <para>
		/// <b>Issue</b> Pattern optimiser, we should optimize wild cards of 
		/// type <c>%__</c> to <c>__%</c>, or <c>%__%_%_%</c> to <c>____%</c>. 
		/// Optimised forms are identical in result and more efficient. This 
		/// optimization could be performed by the client during parsing of 
		/// the <i>LIKE</i> statement.
		/// </para>
		/// <para>
		/// <b>Hacking Issue</b> Badly formed wild cards may result in hogging 
		/// of server side resources.
		/// </para>
		/// </remarks>
		public static bool PatternMatch(String pattern, String expression, char escape_char) {
			// Look at first character in pattern, if it's a ONE_CHAR wildcard then
			// check expression and pattern match until next wild card.

			if (pattern[0] == ONE_CHAR) {

				// Else step through each character in pattern and see if it matches up
				// with the expression until a wild card is found or the end is reached.
				// When the end of the pattern is reached, 'finished' is set to true.

				int i = 1;
				bool finished = (i >= pattern.Length || expression.Length < 1);
				bool last_was_escape = false;
				int checkd = 0;
				while (!finished) {
					char c = pattern[i];
					if (!last_was_escape && c == escape_char) {
						last_was_escape = true;
						if (i >= expression.Length) {
							return false;
						}
						++i;
					} else if (last_was_escape || !IsWildCard(c)) {
						last_was_escape = false;
						// If expression and pattern character doesn't match or end of
						// expression reached, search has failed.
						if (i >= expression.Length || c != expression[i]) {
							return false;
						}
						++i;
						++checkd;
					} else {
						// found a wildcard, so recurse on this wildcard
						return PatternMatch(pattern.Substring(i), expression.Substring(i),
											escape_char);
					}

					finished = (i >= pattern.Length);
				}

				// The pattern length minus any escaped characters
				int real_pattern_length = 0;
				int sz = pattern.Length;
				for (int n = 0; n < sz; ++n) {
					if (pattern[n] != escape_char) {
						++real_pattern_length;
					} else {
						++n;
					}
				}

				// If pattern and expression lengths match then we have walked through
				// the expression and found a match, otherwise no match.

				return real_pattern_length == expression.Length;

			}

			// Therefore we are doing a ZERO_OR_MORE_CHARS wildcard check.

			// If the pattern is '%' (ie. pattern.length() == 1 because it's only 1
			// character in length (the '%' character)) then it doesn't matter what the
			// expression is, we have found a match.

			if (pattern.Length == 1) {
				return true;
			}

			// Look at following character in pattern, and extract all the characters
			// before the next wild card.

			StringBuilder next_string = new StringBuilder();
			int i1 = 1;
			bool finished1 = (i1 >= pattern.Length);
			bool last_was_escape1 = false;
			while (!finished1) {
				char next_char = pattern[i1];
				if (!last_was_escape1 && next_char == escape_char) {
					last_was_escape1 = true;
					++i1;
					if (i1 >= pattern.Length) {
						finished1 = true;
					}
				} else if (last_was_escape1 || !IsWildCard(next_char)) {
					last_was_escape1 = false;
					next_string.Append(next_char);
					++i1;
					if (i1 >= pattern.Length) {
						finished1 = true;
					}
				} else {
					finished1 = true;
				}
			}

			String find_string = next_string.ToString();

			// Special case optimisation if we have found the end of the pattern, all
			// we need to do is check if 'find_string' is on the end of the expression.
			// eg. pattern = "%er", will have a 'find_string' of "er" and it is saying
			// 'does the expression end with 'er''.

			if (i1 >= pattern.Length) {
				return (expression.EndsWith(find_string));
			}

			// Otherwise we must have finished with another wild card.
			// Try and find 'next_string' in the expression.  If its found then
			// recurse over the next pattern.

			int find_str_length = find_string.Length;
			int str_index = expression.IndexOf(find_string, 0);

			while (str_index != -1) {

				bool matched = PatternMatch(
								pattern.Substring(1 + find_str_length),
								expression.Substring(str_index + find_str_length),
								escape_char);

				if (matched) {
					return true;
				}

				str_index = expression.IndexOf(find_string, str_index + 1);
			}

			return false;

		}

		/// <summary>
		/// This is the search method.</summary>
		/// <remarks>
		/// It requires a table to search, a column of the table, and a pattern.
		/// It returns the rows in the table that match the pattern if any. 
		/// Pattern searching only works successfully on columns that are of 
		/// type <see cref="DbTypes.DB_STRING"/>. This works by first reducing the 
		/// search to all cells that contain the first section of text. ie. 
		/// <c>pattern = "Anto% ___ano"</c> will first reduce search to all 
		/// rows between <i>Anto</i> and <i>Anton</i>. This makes for better
		/// efficiency.
		/// </remarks>
		internal static IntegerVector Search(Table table, int column, String pattern) {
			return Search(table, column, pattern, '\\');
		}

		/// <summary>
		/// This is the search method.
		/// </summary>
		/// <remarks>
		/// It requires a table to search, a column of the table, and a pattern.
		/// It returns the rows in the table that match the pattern if any. Pattern searching 
		/// only works successfully on columns that are of type DbTypes.String.
		/// This works by first reducing the search to all cells that contain the
		/// first section of text. ie. pattern = "Anto% ___ano" will first reduce
		/// search to all rows between "Anto" and "Anton".  This makes for better
		/// efficiency.
		/// </remarks>
		internal static IntegerVector Search(Table table, int column, String pattern, char escape_char) {
			// Get the type for the column
			TType col_type = table.DataTableDef[column].TType;

			// If the column type is not a string type then report an error.
			if (!(col_type is TStringType)) {
				throw new ApplicationException("Unable to perform a pattern search " +
								"on a non-String type column.");
			}
			TStringType col_string_type = (TStringType)col_type;

			// ---------- Pre Search ----------

			// First perform a 'pre-search' on the head of the pattern.  Note that
			// there may be no head in which case the entire column is searched which
			// has more potential to be expensive than if there is a head.

			StringBuilder pre_pattern = new StringBuilder();
			int i = 0;
			bool finished = i >= pattern.Length;
			bool last_is_escape = false;

			while (!finished) {
				char c = pattern[i];
				if (last_is_escape) {
					last_is_escape = true;
					pre_pattern.Append(c);
				} else if (c == escape_char) {
					last_is_escape = true;
				} else if (!IsWildCard(c)) {
					pre_pattern.Append(c);

					++i;
					if (i >= pattern.Length) {
						finished = true;
					}

				} else {
					finished = true;
				}
			}

			// This is set with the remaining search.
			String post_pattern;

			// This is our initial search row set.  In the second stage, rows are
			// eliminated from this vector.
			IntegerVector search_case;

			if (i >= pattern.Length) {
				// If the pattern has no 'wildcards' then just perform an EQUALS
				// operation on the column and return the results.

				TObject cell = new TObject(col_type, pattern);
				return table.SelectRows(column, Operator.Get("="), cell);

				// RETURN
			} else if (pre_pattern.Length == 0 ||
					 col_string_type.Locale != null) {

				// No pre-pattern easy search :-(.  This is either because there is no
				// pre pattern (it starts with a wild-card) or the locale of the string
				// is non-lexicographical.  In either case, we need to select all from
				// the column and brute force the search space.

				search_case = table.SelectAll(column);
				post_pattern = pattern;

			} else {

				// Criteria met: There is a pre_pattern, and the column locale is
				// lexicographical.

				// Great, we can do an upper and lower bound search on our pre-search
				// set.  eg. search between 'Geoff' and 'Geofg' or 'Geoff ' and
				// 'Geoff\33'

				String lower_bounds = pre_pattern.ToString();
				int next_char = pre_pattern[i - 1] + 1;
				pre_pattern[i - 1] = (char)next_char;
				String upper_bounds = pre_pattern.ToString();

				post_pattern = pattern.Substring(i);

				TObject cell_lower = new TObject(col_type, lower_bounds);
				TObject cell_upper = new TObject(col_type, upper_bounds);

				// Select rows between these two points.

				search_case = table.SelectRows(column, cell_lower, cell_upper);

			}

			// ---------- Post search ----------

			//  [This optimization assumes that (NULL like '%' = true) which is incorrect]
			//    // EFFICIENCY: This is a special case efficiency case.
			//    // If 'post_pattern' is '%' then we have already found all the records in
			//    // our pattern.
			//
			//    if (post_pattern.Equals("%")) {
			//      return search_case;
			//    }

			int pre_index = i;

			// Now eliminate from our 'search_case' any cells that don't match our
			// search pattern.
			// Note that by this point 'post_pattern' will start with a wild card.
			// This follows the specification for the 'PatternMatch' method.
			// EFFICIENCY: This is a brute force iterative search.  Perhaps there is
			//   a faster way of handling this?

			BlockIntegerList i_list = new BlockIntegerList(search_case);
			IIntegerIterator iterator = i_list.GetIterator(0, i_list.Count - 1);

			while (iterator.MoveNext()) {

				// Get the expression (the contents of the cell at the given column, row)

				bool pattern_matches = false;
				TObject cell = table.GetCellContents(column, iterator.next());
				// Null values doesn't match with anything
				if (!cell.IsNull) {
					String expression = cell.Object.ToString();
					// We must remove the head of the string, which has already been
					// found from the pre-search section.
					expression = expression.Substring(pre_index);
					pattern_matches = PatternMatch(post_pattern, expression, escape_char);
				}
				if (!pattern_matches) {
					// If pattern does not match then remove this row from the search.
					iterator.Remove();
				}

			}

			return new IntegerVector(i_list);

		}

		// ---------- Matching against a regular expression ----------

		/// <summary>
		/// Matches a string against a regular expression pattern.
		/// </summary>
		/// <remarks>
		/// We use the regex library as specified in the DatabaseSystem 
		/// configuration.
		/// </remarks>
		internal static bool RegexMatch(TransactionSystem system, String pattern, String value) {
			// If the first character is a '/' then we assume it's a Perl style regular
			// expression (eg. "/.*[0-9]+\/$/i")
			if (pattern.StartsWith("/")) {
				int end = pattern.LastIndexOf('/');
				String pat = pattern.Substring(1, end);
				String ops = pattern.Substring(end + 1);
				return system.RegexLibrary.RegexMatch(pat, ops, value);
			} else {
				// Otherwise it's a regular expression with no operators
				return system.RegexLibrary.RegexMatch(pattern, "", value);
			}
		}

		/// <summary>
		/// Matches a column of a table against a constant regular expression
		/// pattern.
		/// </summary>
		/// <remarks>
		/// We use the regex library as specified in the DatabaseSystem
		/// configuration.
		/// </remarks>
		internal static IntegerVector RegexSearch(Table table, int column, String pattern) {
			// If the first character is a '/' then we assume it's a Perl style regular
			// expression (eg. "/.*[0-9]+\/$/i")
			if (pattern.StartsWith("/")) {
				int end = pattern.LastIndexOf('/');
				String pat = pattern.Substring(1, end);
				String ops = pattern.Substring(end + 1);
				return table.Database.System.RegexLibrary.RegexSearch(
														  table, column, pat, ops);
			} else {
				// Otherwise it's a regular expression with no operators
				return table.Database.System.RegexLibrary.RegexSearch(
														  table, column, pattern, "");
			}
		}
	}
}