// 
//  DatabaseQueryContext.cs
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

using Deveel.Data.Functions;

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
		/// Returns a DataTableDef for the given table name.
		///</summary>
		///<param name="name"></param>
		///<returns></returns>
		public DataTableDef GetDataTableDef(TableName name) {
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
	}
}