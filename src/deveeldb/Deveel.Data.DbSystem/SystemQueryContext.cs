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

using Deveel.Data.Routines;
using Deveel.Data.Query;
using Deveel.Data.Security;
using Deveel.Data.Transactions;
using Deveel.Diagnostics;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// A <see cref="IQueryContext"/> that only wraps around a 
	/// <see cref="SystemContext"/> and does not provide implementations 
	/// for the <see cref="IQueryContext.GetMarkedTable"/>, and <see cref="Database"/> methods.
	/// </summary>
	sealed class SystemQueryContext : QueryContext {
		/// <summary>
		/// The wrapped TransactionSystem object.
		/// </summary>
		private readonly SystemContext context;

		/// <summary>
		/// The Transaction this is a part of.
		/// </summary>
		private readonly SimpleTransaction transaction;

		/// <summary>
		/// The context schema of this context.
		/// </summary>
		private readonly String currentSchema;



		public SystemQueryContext(SimpleTransaction transaction, string currentSchema) {
			this.transaction = transaction;
			context = transaction.Context;
			this.currentSchema = currentSchema;
		}

		/// <inheritdoc/>
		public override SystemContext Context {
			get { return context; }
		}

		public override ILogger Logger {
			get { return context.Logger; }
		}

		/// <inheritdoc/>
		public override IRoutineResolver RoutineResolver {
			get { return Context.RoutineResolver; }
		}

		/// <inheritdoc/>
		public override long NextSequenceValue(String generatorName) {
			TableName tn = transaction.ResolveToTableName(currentSchema, generatorName, context.IgnoreIdentifierCase);
			return transaction.NextSequenceValue(tn);
		}

		/// <inheritdoc/>
		public override long CurrentSequenceValue(String generatorName) {
			TableName tn = transaction.ResolveToTableName(currentSchema, generatorName, context.IgnoreIdentifierCase);
			return transaction.LastSequenceValue(tn);
		}

		/// <inheritdoc/>
		public override void SetSequenceValue(string generatorName, long value) {
			TableName tn = transaction.ResolveToTableName(currentSchema, generatorName, context.IgnoreIdentifierCase);
			transaction.SetSequenceValue(tn, value);
		}

		/// <summary>
		/// Returns a unique key for the given table source in the database.
		/// </summary>
		/// <param name="tableName"></param>
		/// <returns></returns>
		public long NextUniqueID(string tableName) {
			TableName tname = TableName.Resolve(currentSchema, tableName);
			return transaction.NextUniqueID(tname);
		}

		/// <inheritdoc/>
		public override string UserName {
			get { return "@SYSTEM"; }
		}

		public override Privileges GetUserGrants(GrantObject objType, string objName) {
			return Privileges.TableAll;
		}

		public override Table GetTable(TableName tableName) {
			throw new NotSupportedException();
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

		public override Cursor GetCursor(TableName name) {
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