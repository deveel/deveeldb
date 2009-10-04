//  
//  IQueryContext.cs
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

using Deveel.Data.Functions;

namespace Deveel.Data {
	/// <summary>
	/// Facts about a particular query including the root table sources, user name
	/// of the controlling context, sequence state, etc.
	/// </summary>
	public interface IQueryContext {
		/// <summary>
		/// Returns a TransactionSystem object that is used to determine information
		/// about the transactional system.
		/// </summary>
		TransactionSystem System { get; }

		/// <summary>
		/// Returns the user name of the connection.
		/// </summary>
		string UserName { get; }

		/// <summary>
		/// Returns a <see cref="FunctionLookup"/> object used to convert 
		/// <see cref="FunctionDef"/> objects to <see cref="IFunction"/> objects 
		/// when evaluating an expression.
		/// </summary>
		IFunctionLookup FunctionLookup { get; }

		// ---------- Sequences ----------

		/// <summary>
		/// Increments the sequence generator and returns the next unique key.
		/// </summary>
		/// <param name="generator_name"></param>
		/// <returns></returns>
		long NextSequenceValue(String generator_name);

		/// <summary>
		/// Returns the current sequence value returned for the given sequence
		/// generator within the connection defined by this context.
		/// </summary>
		/// <param name="generator_name"></param>
		/// <returns></returns>
		/// <exception cref="StatementException">
		/// If a value was not returned for this connection.
		/// </exception>
		long CurrentSequenceValue(String generator_name);

		/// <summary>
		/// Sets the current sequence value for the given sequence generator.
		/// </summary>
		/// <param name="generator_name"></param>
		/// <param name="value"></param>
		void SetSequenceValue(String generator_name, long value);

		// ---------- Caching ----------

		/// <summary>
		/// Marks a table in a query plan.
		/// </summary>
		/// <param name="mark_name"></param>
		/// <param name="table"></param>
		/// <seealso cref="GetMarkedTable"/>
		void AddMarkedTable(String mark_name, Table table);

		/// <summary>
		/// Returns a table that was marked in a query plan or null if no 
		/// mark was found.
		/// </summary>
		/// <param name="mark_name"></param>
		/// <returns></returns>
		/// <seealso cref="AddMarkedTable"/>
		Table GetMarkedTable(String mark_name);

		/// <summary>
		/// Put a <see cref="Table"/> into the cache.
		/// </summary>
		/// <param name="id"></param>
		/// <param name="table"></param>
		/// <seealso cref="GetCachedNode"/>
		void PutCachedNode(long id, Table table);

		/// <summary>
		/// Returns a cached table or null if it isn't cached.
		/// </summary>
		/// <param name="id"></param>
		/// <returns></returns>
		/// <seealso cref="PutCachedNode"/>
		Table GetCachedNode(long id);

		/// <summary>
		/// Clears the cache of any cached tables.
		/// </summary>
		/// <seealso cref="PutCachedNode"/>
		/// <seealso cref="GetCachedNode"/>
		void ClearCache();

	}
}