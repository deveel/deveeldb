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
		/// Declares a cursor identified by the given name and on
		/// the specified query.
		/// </summary>
		/// <param name="name">The name of the cursor to create.</param>
		/// <param name="queryPlan">The query used by the cursor to iterate
		/// through the results.</param>
		/// <param name="attributes">The attributes to define a cursor.</param>
		/// <returns>
		/// Returns the newly created <see cref="Cursor"/> instance.
		/// </returns>
		public Cursor DeclareCursor(TableName name, IQueryPlanNode queryPlan, CursorAttributes attributes) {
			return Transaction.DeclareCursor(name, queryPlan, attributes);
		}

		/// <summary>
		/// Declares a scrollable cursor identified by the given name and on
		/// the specified query.
		/// </summary>
		/// <param name="name">The name of the cursor to create.</param>
		/// <param name="queryPlan">The query used by the cursor to iterate
		/// through the results.</param>
		/// <returns>
		/// Returns the newly created <see cref="Cursor"/> instance.
		/// </returns>
		public Cursor DeclareCursor(TableName name, IQueryPlanNode queryPlan) {
			return DeclareCursor(name, queryPlan, CursorAttributes.ReadOnly);
		}

		/// <summary>
		/// Gets the instance of a cursor name.
		/// </summary>
		/// <param name="name">The name of the cursor to get.</param>
		/// <returns>
		/// Returns the instance of the <see cref="Cursor"/> identified by
		/// the given name, or <c>null</c> if it was not found.
		/// </returns>
		public Cursor GetCursor(TableName name) {
			return Transaction.GetCursor(name);
		}

		public bool CursorExists(TableName name) {
			return Transaction.CursorExists(name);
		} 
	}
}