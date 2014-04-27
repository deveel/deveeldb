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
using System.Data;

using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Threading;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public interface IDatabaseConnection : IDisposable {
		IDatabase Database { get; }

		User User { get; }

		string CurrentSchema { get; set; }

		OldNewTableState OldNewState { get;}

		LockingMechanism LockingMechanism { get; }

		ITransaction Transaction { get; }

		GrantManager GrantManager { get; }

		ViewManager ViewManager { get; }

		// TODO: Make these ones session variables ...
		bool IsInCaseInsensitiveMode { get; set; }

		bool AutoCommit { get; set; }

		bool ErrorOnDirtySelect { get; set; }

		IsolationLevel TransactionIsolation { get; set; }


		void CacheTable(TableName tableName, ITableDataSource table);

		ITableDataSource GetCachedTable(TableName tableName);

		void CreateCallbackTrigger(string triggerName, TableName triggerSource, TriggerEventType type);

		void CreateTableTrigger(string schema, string name, TriggerEventType type, TableName onTable, string procedureName, TObject[] parameters);

		void DeleteCallbackTrigger(string triggerName);

		void DropTrigger(string schema, string name);

		void OnTriggerEvent(TriggerEventArgs args);

		void FireTableEvent(TriggerEventArgs args);

		IDbConnection GetDbConnection();

		IProcedureConnection CreateProcedureConnection(User user);

		void DisposeProcedureConnection(IProcedureConnection connection);

		IRef CreateLargeObject(ReferenceType referenceType, long size);

		void Commit();

		void Rollback();

		void Close();
	}
}