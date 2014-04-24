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
using Deveel.Data.Types;
using Deveel.Diagnostics;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// Facts about a particular query including the root table sources, user name
	/// of the controlling context, sequence state, etc.
	/// </summary>
	public interface IQueryContext {
		/// <summary>
		/// Returns a TransactionSystem object that is used to determine information
		/// about the transactional system.
		/// </summary>
		SystemContext Context { get; }

		/// <summary>
		/// Returns the user name of the connection.
		/// </summary>
		string UserName { get; }

		IRoutineResolver RoutineResolver { get; }

		/// <summary>
		/// Optionally gets the connection to the database, if the context
		/// is within a connection.
		/// </summary>
		DatabaseConnection Connection { get; }

		/// <summary>
		/// Gets an object used to log diagnostic information about
		/// events occurred withing the context.
		/// </summary>
		Logger Logger { get; }

		/// <summary>
		/// Gets a value that indicates if the current context is in
		/// an exception state or not.
		/// </summary>
		/// <seealso cref="SetExceptionState"/>
		bool IsExceptionState { get; }

		/// <summary>
		/// Gets a table with the given name from within the underlying
		/// database context.
		/// </summary>
		/// <param name="tableName">The name of the table to return.</param>
		/// <returns>
		/// Returns a <see cref="Table"/> having the name specified or
		/// <b>null</b> if no table with the given name was found in the
		/// underlying database context.
		/// </returns>
		Table GetTable(TableName tableName);

		/// <summary>
		/// Gets the privileges of the current user on the given database object
		/// </summary>
		/// <param name="objType">The type of the object.</param>
		/// <param name="objName">The name of the object.</param>
		/// <returns></returns>
		Privileges GetUserGrants(GrantObject objType, string objName);

		/// <summary>
		/// Gets only the <c>GRANT</c> options on the given database object for the
		/// current user in the session
		/// </summary>
		/// <param name="objType"></param>
		/// <param name="objName"></param>
		/// <returns></returns>
		/// <seealso cref="GrantManager.GetUserGrantOptions"/>
		Privileges GetUserGrantOptions(GrantObject objType, string objName);

		/// <summary>
		/// Marks the execution context as in an exception state.
		/// </summary>
		/// <param name="exception">The exception that causes the change of
		/// state of the context.</param>
		/// <seealso cref="IsExceptionState"/>
		/// <seealso cref="GetException"/>
		void SetExceptionState(Exception exception);

		/// <summary>
		/// If this context is in an exception state, this method
		/// gets the exception that caused the change of state.
		/// </summary>
		/// <returns>
		/// Returns an <see cref="Exception"/> that is the origin
		/// of the context change state, or <b>null</b> if the context
		/// is not in an exception state.
		/// </returns>
		/// <seealso cref="IsExceptionState"/>
		/// <seealso cref="SetExceptionState"/>
		Exception GetException();

		// ---------- Sequences ----------

		/// <summary>
		/// Increments the sequence generator and returns the next unique key.
		/// </summary>
		/// <param name="generatorName"></param>
		/// <returns></returns>
		long NextSequenceValue(String generatorName);

		/// <summary>
		/// Returns the current sequence value returned for the given sequence
		/// generator within the connection defined by this context.
		/// </summary>
		/// <param name="generatorName"></param>
		/// <returns></returns>
		/// <exception cref="StatementException">
		/// If a value was not returned for this connection.
		/// </exception>
		long CurrentSequenceValue(String generatorName);

		/// <summary>
		/// Sets the current sequence value for the given sequence generator.
		/// </summary>
		/// <param name="generatorName"></param>
		/// <param name="value"></param>
		void SetSequenceValue(String generatorName, long value);

		// ---------- Caching ----------

		/// <summary>
		/// Marks a table in a query plan.
		/// </summary>
		/// <param name="markName"></param>
		/// <param name="table"></param>
		/// <seealso cref="GetMarkedTable"/>
		void AddMarkedTable(String markName, Table table);

		/// <summary>
		/// Returns a table that was marked in a query plan or null if no 
		/// mark was found.
		/// </summary>
		/// <param name="markName"></param>
		/// <returns></returns>
		/// <seealso cref="AddMarkedTable"/>
		Table GetMarkedTable(String markName);

		/// <summary>
		/// Put a <see cref="Table"/> into the cache.
		/// </summary>
		/// <param name="id"></param>
		/// <param name="table"></param>
		/// <seealso cref="GetCachedNode"/>
		void PutCachedNode(long id, Table table);

		/// <summary>
		/// Returns a cached table or null if it isn't cached.
		/// </summary>
		/// <param name="id"></param>
		/// <returns></returns>
		/// <seealso cref="PutCachedNode"/>
		Table GetCachedNode(long id);

		/// <summary>
		/// Clears the cache of any cached tables.
		/// </summary>
		/// <seealso cref="PutCachedNode"/>
		/// <seealso cref="GetCachedNode"/>
		void ClearCache();

		// -------------- Variables ----------------

		/// <summary>
		/// Declares a variable, identified by the given name,
		/// within the current query context.
		/// </summary>
		/// <param name="name">The name of the variable to declare.</param>
		/// <param name="type">The type of the variable.</param>
		/// <param name="constant">A flag indicating whether the variable
		/// is <c>constant</c> (default value is required and cannot be 
		/// changed).</param>
		/// <param name="notNull">A flag indicating whether the variable
		/// can be set to <c>null</c>.</param>
		/// <returns>
		/// Returns a reference to the <see cref="Variable"/> object created.
		/// </returns>
		Variable DeclareVariable(string name, TType type, bool constant, bool notNull);

		/// <summary>
		/// Gets a declared variable with the given name.
		/// </summary>
		/// <param name="name">The name of the variable
		/// to return.</param>
		/// <returns>
		/// Returns a declared <see cref="Variable"/> having
		/// the given name or <c>null</c> if no variables were 
		/// found for the given name.
		/// </returns>
		Variable GetVariable(string name);

		/// <summary>
		/// Sets the value of a variable identified by the
		/// given name.
		/// </summary>
		/// <param name="name">The name of the variable to set.</param>
		/// <param name="value">The value to set for the variable.</param>
		void SetVariable(string name, Expression value);

		/// <summary>
		/// Explicitly removes the given variable from the
		/// current query context.
		/// </summary>
		/// <param name="name">The name of the variable to remove.</param>
		void RemoveVariable(string name);

		// -------------- Cursors ----------------

		Cursor DeclareCursor(TableName name, IQueryPlanNode planNode, CursorAttributes attributes);

		Cursor GetCursor(TableName name);

		void OpenCursor(TableName name);

		void CloseCursor(TableName name);
	}
}