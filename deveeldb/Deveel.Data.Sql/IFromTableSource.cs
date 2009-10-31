//  
//  IFromTableSource.cs
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

namespace Deveel.Data.Sql {
	/// <summary>
	/// A single table resource item in a query which handles the behaviour
	/// of resolving references to columns as well as providing various base
	/// utility methods for resolving general variable names.
	/// </summary>
	/// <remarks>
	/// Each instance of this interface represents a single <i>FROM</i>
	/// resource.
	/// </remarks>
	public interface IFromTableSource {
		/// <summary>
		/// Gets a unique name given to this table source.
		/// </summary>
		/// <remarks>
		/// No other sources will share this identifier string.
		/// </remarks>
		string UniqueName { get; }

		/// <summary>
		/// Checks if the table matches the given catalog, schema and table.
		/// </summary>
		/// <param name="catalog">The catalog name used for the matching.</param>
		/// <param name="schema">The schema name used for the matching.</param>
		/// <param name="table">The table name used for the maching.</param>
		/// <remarks>
		/// If any arguments are <b>null</b> then it is not included in 
		/// the match.
		/// <para>
		/// Used for 'Part.*' type glob searches.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if this source matches the given catalog, 
		/// schema and table, otherwise <b>false</b>.
		/// </returns>
		bool MatchesReference(String catalog, String schema, String table);

		/// <summary>
		/// Returns the number of instances we can resolve the given 
		/// catalog, schema, table and column name to a column or columns 
		/// within the current source.
		/// </summary>
		/// <param name="catalog"></param>
		/// <param name="schema"></param>
		/// <param name="table"></param>
		/// <param name="column"></param>
		/// <remarks>
		/// Note that if catalog, schema, table or column is <b>null</b> 
		/// then it means it doesn't matter.
		/// <para>
		/// Note that parameters of <i>null, null, null, null</i>,
		/// <i>null, null, null, not null</i>, <i>null, null, not null, not null</i>,
		/// <i>null, not null, not null, not null</i>, and 
		/// <i>not null, not null, not null, not null</i> are only accepted.
		/// </para>
		/// </remarks>
		/// <example>
		/// For example, say we need to resolve the column 'id' the arguments 
		/// are  <c>null, null, null, "id"</c>. This may resolve to multiple 
		/// columns if there is a mixture of tables with "id" as a column.
		/// </example>
		/// <returns></returns>
		int ResolveColumnCount(string catalog, string schema, string table, string column);

		/// <summary>
		/// Resolves a variable within the current source.
		/// </summary>
		/// <param name="catalog"></param>
		/// <param name="schema"></param>
		/// <param name="table"></param>
		/// <param name="column"></param>
		/// <remarks>
		/// This method does not have to check whether the parameters 
		/// reference more than one column. If more than one column is 
		/// referenced, the actual column returned is implementation 
		/// specific.
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="Variable"/> that is a fully resolved form of 
		/// the given <paramref name="column"/> in the current table set.
		/// </returns>
		Variable ResolveColumn(string catalog, string schema, string table, string column);

		/// <summary>
		/// Returns an array of <see cref="Variable"/> objects that references 
		/// each column available in the table set item in order from left 
		/// column to right column.
		/// </summary>
		Variable[] AllColumns { get; }
	}
}