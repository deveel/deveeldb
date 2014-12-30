// 
//  Copyright 2010-2014 Deveel
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

using Deveel.Data.DbSystem;
using Deveel.Data.Query;

namespace Deveel.Data.Transactions {
	public interface ITransaction : IDisposable {
		ITransactionContext Context { get; }

		VariablesManager Variables { get; }


		TableName[] GetTables();

		bool TableExists(TableName tableName);

		bool RealTableExists(TableName tableName);

		TableName ResolveToTableName(string currentSchema, string name, bool caseInsensitive);

		TableName TryResolveCase(TableName tableName);

		DataTableInfo GetTableInfo(TableName tableName);

		ITableDataSource GetTable(TableName tableName);

		string GetTableType(TableName tableName);

		// Sequences

		long NextSequenceValue(TableName name);

		long LastSequenceValue(TableName name);

		void SetSequenceValue(TableName name, long value);

		// Table IDs

		long CurrentUniqueId(TableName tableName);

		long NextUniqueId(TableName tableName);

		void SetUniqueId(TableName tableName, long uniqueId);

		// Cursors

		Cursor DeclareCursor(TableName name, IQueryPlanNode queryPlan, CursorAttributes attributes);

		void DropCursor(TableName name);

		Cursor GetCursor(TableName name);

		bool CursorExists(TableName name);
	}
}