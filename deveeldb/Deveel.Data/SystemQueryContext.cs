//  
//  SystemQueryContext.cs
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
	/// A <see cref="IQueryContext"/> that only wraps around a 
	/// <see cref="TransactionSystem"/> and does not provide implementations 
	/// for the <see cref="IQueryContext.GetMarkedTable"/>, and <see cref="Database"/> methods.
	/// </summary>
	sealed class SystemQueryContext : QueryContext {
		/// <summary>
		/// The wrapped TransactionSystem object.
		/// </summary>
		private readonly TransactionSystem system;

		/// <summary>
		/// The Transaction this is a part of.
		/// </summary>
		private readonly SimpleTransaction transaction;

		/// <summary>
		/// The context schema of this context.
		/// </summary>
		private readonly String current_schema;



		internal SystemQueryContext(SimpleTransaction transaction, String current_schema) {
			this.transaction = transaction;
			system = transaction.System;
			this.current_schema = current_schema;
		}

		/// <inheritdoc/>
		public override TransactionSystem System {
			get { return system; }
		}

		/// <inheritdoc/>
		public override IFunctionLookup FunctionLookup {
			get { return System.FunctionLookup; }
		}

		/// <inheritdoc/>
		public override long NextSequenceValue(String name) {
			TableName tn = transaction.ResolveToTableName(current_schema, name,
														system.IgnoreIdentifierCase);
			return transaction.NextSequenceValue(tn);
		}

		/// <inheritdoc/>
		public override long CurrentSequenceValue(String name) {
			TableName tn = transaction.ResolveToTableName(current_schema, name,
														system.IgnoreIdentifierCase);
			return transaction.LastSequenceValue(tn);
		}

		/// <inheritdoc/>
		public override void SetSequenceValue(String name, long value) {
			TableName tn = transaction.ResolveToTableName(current_schema, name,
														system.IgnoreIdentifierCase);
			transaction.SetSequenceValue(tn, value);
		}

		/// <summary>
		/// Returns a unique key for the given table source in the database.
		/// </summary>
		/// <param name="table_name"></param>
		/// <returns></returns>
		public long NextUniqueID(String table_name) {
			TableName tname = TableName.Resolve(current_schema, table_name);
			return transaction.NextUniqueID(tname);
		}

		/// <inheritdoc/>
		public override string UserName {
			get { return "@SYSTEM"; }
		}

		public override Variable GetVariable(string name) {
			return transaction.Variables.GetVariable(name);
		}

		public override void SetVariable(string name, Expression value) {
			transaction.Variables.SetVariable(name, value, this);
		}

		public override Cursor DeclareCursor(TableName name, IQueryPlanNode planNode, CursorAttributes attributes) {
			throw new NotSupportedException();
		}

		public override Cursor GetCursror(TableName name) {
			throw new NotSupportedException();
		}

		public override void OpenCursor(TableName name) {
			throw new NotSupportedException();
		}

		public override void CloseCursor(TableName name) {
			throw new NotSupportedException();
		}
	}
}