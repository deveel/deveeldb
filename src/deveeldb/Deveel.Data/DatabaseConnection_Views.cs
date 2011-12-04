// 
//  Copyright 2010-2011  Deveel
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

namespace Deveel.Data {
	public sealed partial class DatabaseConnection {
		/// <summary>
		/// The connection view manager that handles view information through this
		/// connection.
		/// </summary>
		private ViewManager view_manager;

		/// <summary>
		/// Creates a new view.
		/// </summary>
		/// <param name="query"></param>
		/// <param name="view">View meta informations used to create the view.</param>
		/// <remarks>
		/// Note that this is a transactional operation. You need to commit for 
		/// the view to be visible to other transactions.
		/// </remarks>
		/// <exception cref="DatabaseException"/>
		public void CreateView(SqlQuery query, ViewDef view) {
			CheckAllowCreate(view.DataTableDef.TableName);

			try {
				view_manager.DefineView(view, query, User);
			} catch (DatabaseException e) {
				Debug.WriteException(e);
				throw new Exception("Database Exception: " + e.Message);
			}

		}

		/// <summary>
		/// Drops a view with the given name.
		/// </summary>
		/// <param name="view_name">Name of the view to drop.</param>
		/// <remarks>
		/// Note that this is a transactional operation. You need to commit 
		/// for the change to be visible to other transactions.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if the drop succeeded, otherwise <b>false</b> if 
		/// the view was not found.
		/// </returns>
		public bool DropView(TableName view_name) {
			try {
				return view_manager.DeleteView(view_name);
			} catch (DatabaseException e) {
				Debug.WriteException(e);
				throw new Exception("Database Exception: " + e.Message);
			}

		}
	}
}