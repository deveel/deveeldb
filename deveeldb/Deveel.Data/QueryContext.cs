//  
//  QueryContext.cs
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
using System.Collections;

using Deveel.Data.Functions;

namespace Deveel.Data {
	/// <summary>
	/// An abstract implementation of <see cref="IQueryContext"/>
	/// </summary>
	public abstract class QueryContext : IQueryContext {
		/// <summary>
		/// Any marked tables that are made during the evaluation of a query plan. (String) -> (Table)
		/// </summary>
		private Hashtable marked_tables;

		/// <inheritdoc/>
		public abstract TransactionSystem System { get; }

		/// <inheritdoc/>
		public abstract string UserName { get; }

		/// <inheritdoc/>
		public abstract IFunctionLookup FunctionLookup { get; }

		/// <inheritdoc/>
		public abstract long NextSequenceValue(string generator_name);

		/// <inheritdoc/>
		public abstract long CurrentSequenceValue(string generator_name);

		/// <inheritdoc/>
		public abstract void SetSequenceValue(string generator_name, long value);

		/// <inheritdoc/>
		public void AddMarkedTable(String mark_name, Table table) {
			if (marked_tables == null)
				marked_tables = new Hashtable();
			marked_tables.Add(mark_name, table);
		}

		/// <inheritdoc/>
		public Table GetMarkedTable(String mark_name) {
			if (marked_tables == null)
				return null;
			return (Table)marked_tables[mark_name];
		}

		/// <inheritdoc/>
		public void PutCachedNode(long id, Table table) {
			if (marked_tables == null)
				marked_tables = new Hashtable();
			marked_tables.Add(id, table);
		}

		/// <inheritdoc/>
		public Table GetCachedNode(long id) {
			if (marked_tables == null)
				return null;
			return (Table)marked_tables[id];
		}

		/// <inheritdoc/>
		public void ClearCache() {
			if (marked_tables != null)
				marked_tables.Clear();
		}
	}
}