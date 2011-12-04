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
using System.Collections;

using Deveel.Data.Functions;
using Deveel.Data.QueryPlanning;

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

		public virtual Variable DeclareVariable(string name, TType type, bool constant, bool notNull) {
			return null;
		}

		public virtual Variable GetVariable(string name) {
			return null;
		}

		public virtual void SetVariable(string name, Expression value) {
			// nothing to do by default...
		}

		/// <inheritdoc/>
		public virtual Cursor DeclareCursor(TableName name, IQueryPlanNode planNode, CursorAttributes attributes) {
			return null;
		}

		/// <inheritdoc/>
		public virtual Cursor GetCursror(TableName name) {
			return null;
		}

		/// <inheritdoc/>
		public virtual void OpenCursor(TableName name) {
		}

		/// <inheritdoc/>
		public virtual void CloseCursor(TableName name) {
		}
	}
}