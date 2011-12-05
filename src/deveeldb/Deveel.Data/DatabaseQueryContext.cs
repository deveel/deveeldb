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

using Deveel.Data.Functions;
using Deveel.Data.QueryPlanning;

namespace Deveel.Data {
	///<summary>
	/// An implementation of a <see cref="IQueryContext"/> based on a 
	/// <see cref="DatabaseConnection"/> object.
	///</summary>
	public class DatabaseQueryContext : QueryContext {
		private readonly DatabaseConnection database;

		///<summary>
		/// Constructs the IQueryContext on the <see cref="DatabaseConnection"/> given.
		///</summary>
		///<param name="database"></param>
		public DatabaseQueryContext(DatabaseConnection database) {
			this.database = database;
		}

		///<summary>
		/// Returns the Database object that this context is a child of.
		///</summary>
		public Database Database {
			get { return database.Database; }
		}

		public override TransactionSystem System {
			get { return Database.System; }
		}

		public override IFunctionLookup FunctionLookup {
			get { return System.FunctionLookup; }
		}

		internal DatabaseConnection Connection {
			get { return database; }
		}

		///<summary>
		/// Returns the GrantManager object that is used to determine grant 
		/// information for the database.
		///</summary>
		public GrantManager GrantManager {
			get { return database.GrantManager; }
		}

		///<summary>
		/// Returns a DataTable from the database with the given table name.
		///</summary>
		///<param name="name"></param>
		///<returns></returns>
		public DataTable GetTable(TableName name) {
			database.AddSelectedFromTable(name);
			return database.GetTable(name);
		}

		///<summary>
		/// Returns a DataTableInfo for the given table name.
		///</summary>
		///<param name="name"></param>
		///<returns></returns>
		public DataTableInfo GetDataTableDef(TableName name) {
			return database.GetDataTableDef(name);
		}

		///<summary>
		/// Creates a IQueryPlanNode for the view with the given name.
		///</summary>
		///<param name="name"></param>
		///<returns></returns>
		public IQueryPlanNode CreateViewQueryPlanNode(TableName name) {
			return database.CreateViewQueryPlanNode(name);
		}

		public override long NextSequenceValue(String name) {
			return database.NextSequenceValue(name);
		}

		public override long CurrentSequenceValue(String name) {
			return database.LastSequenceValue(name);
		}

		public override void SetSequenceValue(String name, long value) {
			database.SetSequenceValue(name, value);
		}

		public override string UserName {
			get { return database.User.UserName; }
		}

		public override Variable DeclareVariable(string name, TType type, bool constant, bool notNull) {
			return database.DeclareVariable(name, type, constant, notNull);
		}

		public override Variable GetVariable(string name) {
			return database.GetVariable(name);
		}

		public override void SetVariable(string name, Expression value) {
			database.SetVariable(name, value, this);
		}

		public override Cursor DeclareCursor(TableName name, IQueryPlanNode planNode, CursorAttributes attributes) {
			return database.DeclareCursor(name, planNode, attributes);
		}

		public override Cursor GetCursror(TableName name) {
			return database.GetCursor(name);
		}

		public override void OpenCursor(TableName name) {
			Cursor cursor = GetCursror(name);
			if (cursor == null)
				throw new DatabaseException("The cursor '" + name + "' was not defined within the current context.");
			cursor.Open(this);
		}

		public override void CloseCursor(TableName name) {
			Cursor cursor = GetCursror(name);
			if (cursor == null)
				throw new DatabaseException("The cursor '" + name + "' was not defined within the current context.");
			cursor.Close();
		}
	}
}