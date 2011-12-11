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

using Deveel.Diagnostics;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Provides a set of useful utility functions to use by all the
	/// interpretted statements.
	/// </summary>
	public abstract class Statement {
		protected Statement() {
			context = new StatementContext(this);
		}

		private readonly StatementContext context;
		private bool prepared;
		private bool evaluated;

		public StatementContext Context {
			get { return context; }
		}

		protected DatabaseConnection Connection {
			get { return context.Connection; }
		}

		protected User User {
			get { return context.User; }
		}

		protected SqlQuery Query {
			get { return context.Query; }
		}

		/// <summary>
		/// Gets an <see cref="IDebugLogger"/> used to log _queries.
		/// </summary>
		protected IDebugLogger Debug {
			get { return context.Connection.Debug; }
		}

		protected string GetString(string key) {
			return (string)context.StatementTree.GetValue(key);
		}

		protected object GetValue(string key) {
			return context.StatementTree.GetValue(key);
		}

		protected object GetValue(string key, Type type) {
			object value = context.StatementTree.GetValue(key);
			if (value == null)
				return null;

			if (!type.IsInstanceOfType(value))
				value = Convert.ChangeType(value, type);

			return value;
		}

		protected T GetValue<T>(string key) {
			return (T) GetValue(key, typeof (T));
		}

		protected int GetInt32(string key) {
			return GetValue<int>(key);
		}

		protected bool GetBoolean(string key) {
			return GetValue<bool>(key);
		}

		protected Expression GetExpression(string key) {
			return (Expression) GetValue(key);
		}

		protected bool IsEmpty(string key) {
			IList list = GetList(key);
			return (list == null || list.Count == 0);
		}

		protected IList GetList(string key) {
			return GetList(key, false);
		}

		protected IList GetList(string key, bool safe) {
			IList list = (IList) GetValue(key);
			if (list == null && safe) {
				list = new BackedList(this, key);
				SetValue(key, list);
			}

			return list;
		}

		protected void SetValue(string key, object value) {
			context.StatementTree.SetValue(key, value);
		}

		protected void SetValue(string key, int value) {
			SetValue(key, (object)value);
		}

		protected void SetValue(string key, bool value) {
			SetValue(key, (object)value);
		}

		protected void SetValue(string key, string value) {
			SetValue(key, (object)value);
		}

		protected void SetValue(string key, Expression value) {
			SetValue(key, (object)value);
		}

		/// <summary>
		/// Resets this statement so it may be re-prepared and evaluated again.
		/// </summary>
		/// <remarks>
		/// Useful for repeating a query multiple times.
		/// </remarks>
		internal void Reset() {
			evaluated = false;
			prepared = false;

			OnReset();
		}

		protected virtual void OnReset() {
		}

		/// <summary>
		/// Resolves table name over the current context.
		/// </summary>
		/// <param name="name"></param>
		/// <remarks>
		/// If the schema part of the table name is not present then it 
		/// is set to the current schema of the database session. If the
		/// database is ignoring the case then this will correctly resolve 
		/// the table to the cased version of the table name.
		/// </remarks>
		/// <returns></returns>
		protected TableName ResolveTableName(string name) {
			return context.Connection.ResolveTableName(name);
		}

		internal void PrepareStatement() {
			if (prepared)
				throw new StatementException("The statement has been already prepared.");
			if (evaluated)
				throw new StatementException("The statement has been already executed.");

			Prepare();

			prepared = true;
		}

		/// <summary>
		/// Prepares the statement with the given database object.
		/// </summary>
		/// <remarks>
		/// This is called before the statement is evaluated. The prepare 
		/// statement queries the database and resolves information about 
		/// the statement (for example, it resolves column names and aliases 
		/// and determines the tables that are touched by this statement 
		/// so we can lock the appropriate tables before we evaluate).
		/// <para>
		/// <b>Note:</b> Care must be taken to ensure that all methods 
		/// called here are safe in as far as modifications to the data 
		/// occuring. 
		/// The rules for safety should be as follows: 
		/// <list type="bullet">
		/// <item>
		/// If the database is in <see cref="LockingMode.Exclusive"/> mode, 
		/// then we need to wait until it's switched back to 
		/// <see cref="LockingMode.Shared"/> mode before this method is called.
		/// </item>
		/// <item>
		/// All collection of information done here should not involve 
		/// any table state info. Except for column count, column names, 
		/// column types, etc.
		/// </item>
		/// <item>
		/// Queries such as obtaining the row count, selectable scheme 
		/// information, and certainly 'GetCellValue' must never be called 
		/// during prepare.
		/// </item>
		/// <item>
		/// When prepare finishes, the affected tables are locked and 
		/// the query is safe to call <see cref="Evaluate"/> at which 
		/// time table state is safe to inspect.
		/// </item>
		/// </list>
		/// </para>
		/// </remarks>
		/// <exception cref="DatabaseException"/>
		protected abstract void Prepare();

		internal Table EvaluateStatement() {
			if (!prepared)
				throw new StatementException("The statement is in an invalid state: must be prepared.");
			if (evaluated)
				throw new StatementException("The statement has already been evaluated.");

			Table result = Evaluate();

			evaluated = true;

			return result;
		}

		/// <summary>
		/// Evaluates the statement after it is prepared.
		/// </summary>
		/// <remarks>
		/// The method must be called after <see cref="Prepare"/>.
		/// </remarks>
		/// <returns>
		/// Returns a table that represents the result set.
		/// </returns>
		/// <exception cref="DatabaseException"/>
		/// <exception cref="TransactionException"/>
		protected abstract Table Evaluate();

		protected virtual bool OnListAdd(string key, object value, ref object newValue) {
			return true;
		}

		protected virtual void OnListAdded(string key, object value, int index) {
		}

		protected virtual bool OnListRemove(string key, object value) {
			return true;
		}

		protected virtual bool OnListRemoved(string key, object value) {
			return true;
		}

		protected virtual bool OnListRemoveAt(string key, int index) {
			IList list = GetList(key);
			return OnListRemove(key, list[index]);
		}

		protected virtual bool OnListClear(string key) {
			return true;
		}

		protected virtual bool OnListInsert(string key, int index, object value, ref object newValue) {
			return true;
		}

		protected virtual void OnListInserted(string key, int index, object value) {
		}

		protected virtual bool OnListSet(string key, int index, object value, ref object newValue) {
			return true;
		}


		#region BackedList

		private class BackedList : IList {
			public BackedList(Statement statement, string key) {
				this.statement = statement;
				this.key = key;
				list = new ArrayList();
			}

			private readonly Statement statement;
			private readonly string key;
			private readonly ArrayList list;

			#region Implementation of IEnumerable

			public IEnumerator GetEnumerator() {
				return list.GetEnumerator();
			}

			#endregion

			#region Implementation of ICollection

			public void CopyTo(Array array, int index) {
				list.CopyTo(array, index);
			}

			public int Count {
				get { return list.Count; }
			}

			public object SyncRoot {
				get { return list.SyncRoot; }
			}

			public bool IsSynchronized {
				get { return list.IsSynchronized; }
			}

			#endregion

			#region Implementation of IList

			public int Add(object value) {
				int index = -1;
				object newValue = value;
				if (statement.OnListAdd(key, value, ref newValue)) {
					index = list.Add(newValue);
					statement.OnListAdded(key, newValue, index);
				}

				return index;
			}

			public bool Contains(object value) {
				return list.Contains(value);
			}

			public void Clear() {
				if (statement.OnListClear(key))
					list.Clear();
			}

			public int IndexOf(object value) {
				return list.IndexOf(value);
			}

			public void Insert(int index, object value) {
				object newValue = value;
				if (statement.OnListInsert(key, index, value, ref newValue)) {
					list.Insert(index, newValue);
					statement.OnListInserted(key, index, newValue);
				}
			}

			public void Remove(object value) {
				if (statement.OnListRemove(key, value)) {
					list.Remove(value);
					statement.OnListRemoved(key, value);
				}
			}

			public void RemoveAt(int index) {
				if (statement.OnListRemoveAt(key, index)) {
					list.RemoveAt(index);
					statement.OnListRemoved(key, index);
				}
			}

			public object this[int index] {
				get { return list[index]; }
				set {
					object newValue = value;
					if (statement.OnListSet(key, index, value, ref newValue)) {
						list[index] = newValue;
					}
				}
			}

			public bool IsReadOnly {
				get { return list.IsReadOnly; }
			}

			public bool IsFixedSize {
				get { return list.IsFixedSize; }
			}

			#endregion
		}

		#endregion
	}
}