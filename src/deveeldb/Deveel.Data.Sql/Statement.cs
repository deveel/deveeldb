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
using System.Collections.Generic;
using System.Runtime.Serialization;

using Deveel.Data.QueryPlanning;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Provides a set of useful utility functions to use by all the
	/// interpretted statements.
	/// </summary>
	[Serializable]
	public abstract class Statement : ISerializable {
		protected Statement() {
		}

		protected Statement(SerializationInfo info, StreamingContext context) {
			prepared = info.GetBoolean("Prepared");

			statementTree = (StatementTree) info.GetValue("StatementTree", typeof (StatementTree));
			query = (SqlQuery) info.GetValue("Query", typeof (SqlQuery));

			parent = (Statement) info.GetValue("Parent", typeof (Statement));
			preparedStatements =
				(Dictionary<string, List<Statement>>)
				info.GetValue("PreparedStatements", typeof (Dictionary<string, List<Statement>>));
		}

		private StatementTree statementTree;
		private Statement parent;
		private SqlQuery query;
		private bool prepared;
		private bool evaluated;
		private Dictionary<string, List<Statement> >preparedStatements;

		public SqlQuery Query {
			get { return query; }
			internal set { query = value; }
		}

		protected Statement Root {
			get { 
				Statement root = this;
				Statement s;
				while ((s = root.parent) != null) {
					root = s;
				}

				return root;
			}
		}

		internal StatementTree StatementTree {
			get { return statementTree; }
			set { statementTree = value; }
		}

		private void PrepareChildStatementTree(IQueryContext context, string key, StatementTree tree) {
			// we assume the statement as a public constructor
			Statement statement = Activator.CreateInstance(tree.StatementType, true) as Statement;
			if (statement == null)
				return;

			statement.parent = this;
			statement.Query = query;
			statement.statementTree = tree;
			statement.PrepareStatement(context);

			if (preparedStatements == null)
				preparedStatements = new Dictionary<string, List<Statement>>();

			List<Statement> statements;
			if (!preparedStatements.TryGetValue(key, out statements)) {
				statements = new List<Statement>();
				preparedStatements[key] = statements;
			}

			statements.Add(statement);
		}

		/// <summary>
		/// Scans the tree to find for child statements to prepare
		/// </summary>
		private void PrepareStatements(IQueryContext context) {
			foreach (KeyValuePair<string, object> pair in statementTree) {
				object value = pair.Value;
				if (value is StatementTree) {
					StatementTree childTree = (StatementTree) value;
					PrepareChildStatementTree(context, pair.Key, childTree);
				} else if (value is IList<StatementTree>) {
					IList<StatementTree> list = (IList<StatementTree>) value;
					foreach (StatementTree childTree in list) {
						PrepareChildStatementTree(context, pair.Key, childTree);
					}
				}
			}
		}

		/// <summary>
		/// Checks the permissions for the current user to determine if they are 
		/// allowed to select (read) from tables in the given plan.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="plan"></param>
		/// <exception cref="UserAccessException">
		/// If the user is not allowed to select from a table in the 
		/// given plan.
		/// </exception>
		protected void CheckUserSelectPermissions(IQueryContext context, IQueryPlanNode plan) {
			// Discover the list of TableName objects this command touches,
			IList<TableName> touchedTables = plan.DiscoverTableNames(new List<TableName>());
			Database dbase = context.Connection.Database;

			// Check that the user is allowed to select from these tables.
			foreach (TableName table in touchedTables) {
				if (!dbase.CanUserSelectFromTableObject(context, table, null))
					throw new UserAccessException("User not permitted to select from table: " + table);
			}
		}

		protected IList<Statement> GetPreparedStatements(string key) {
			if (preparedStatements == null)
				return null;

			List<Statement> statements;
			if (!preparedStatements.TryGetValue(key, out statements))
				return null;

			return statements.AsReadOnly();
		}

		protected Statement GetPreparedStatement(string key) {
			IList<Statement> statements = GetPreparedStatements(key);
			if (statements == null)
				return null;
			if (statements.Count > 0)
				throw new InvalidOperationException("Ambigous reference for statements with key '" + key + "'.");

			return statements[0];
		}

		protected string GetString(string key) {
			return (string)statementTree.GetValue(key);
		}

		protected object GetValue(string key) {
			return statementTree.GetValue(key);
		}

		protected object GetValue(string key, Type type) {
			object value = statementTree.GetValue(key);
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
			return GetList(key, null);
		}

		protected IList GetList(string key, Type type) {
			return GetList(key, type, false);
		}

		protected IList GetList(string key, bool safe) {
			return GetList(key, null, safe);
		}

		protected IList GetList(string key, Type type, bool safe) {
			IList list = (IList) GetValue(key);
			if (list == null && safe) {
				if (type == null)
					type = typeof (object);

				Type listType = typeof (BackedList<>).MakeGenericType(type);
				list = Activator.CreateInstance(listType, new object[] { this, key}) as IList;
				SetValue(key, list);
			}

			return list;
		}

		protected void SetValue(string key, object value) {
			statementTree.SetValue(key, value);
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

			if (preparedStatements != null)
				preparedStatements.Clear();

			preparedStatements = null;

			OnReset();
		}

		protected virtual void OnReset() {
		}

		/// <summary>
		/// Resolves table name over the current context.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="name"></param>
		/// <remarks>
		/// If the schema part of the table name is not present then it 
		/// is set to the current schema of the database session. If the
		/// database is ignoring the case then this will correctly resolve 
		/// the table to the cased version of the table name.
		/// </remarks>
		/// <returns></returns>
		protected static TableName ResolveTableName(IQueryContext context, string name) {
			return context.Connection.ResolveTableName(name);
		}

		protected static SchemaDef ResolveSchemaName(IQueryContext context, string name) {
			return context.Connection.ResolveSchemaCase(name, context.Connection.IsInCaseInsensitiveMode);
		}

		internal void PrepareStatement(IQueryContext context) {
			if (!prepared) {
				if (evaluated)
					throw new StatementException("The statement has been already executed.");


				PrepareStatements(context);

				Prepare(context);

				prepared = true;
			}
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
		protected virtual void Prepare(IQueryContext context) {
		}

		internal Table EvaluateStatement(IQueryContext context) {
			if (!prepared)
				throw new StatementException("The statement is in an invalid state: must be prepared.");
			if (evaluated)
				throw new StatementException("The statement has already been evaluated.");

			Table result = Evaluate(context);

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
		protected abstract Table Evaluate(IQueryContext context);

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

		private class BackedList<T> : IList<T>, IList {
			public BackedList(Statement statement, string key) {
				this.statement = statement;
				this.key = key;
				list = new List<T>();
			}

			private readonly Statement statement;
			private readonly string key;
			private readonly List<T> list;

			#region Implementation of IEnumerable

			public IEnumerator<T> GetEnumerator() {
				return list.GetEnumerator();
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			#endregion

			#region Implementation of ICollection

			public void CopyTo(Array array, int index) {
				list.CopyTo((T[])array, index);
			}

			public bool Remove(T item) {
				return list.Remove(item);
			}

			public int Count {
				get { return list.Count; }
			}

			public object SyncRoot {
				get { return this; }
			}

			public bool IsSynchronized {
				get { return false; }
			}

			#endregion

			#region Implementation of IList

			public int Add(object value) {
				int index = -1;
				object newValue = value;
				if (statement.OnListAdd(key, value, ref newValue)) {
					index = list.Count;
					list.Add((T)newValue);
					statement.OnListAdded(key, newValue, index);
				}

				return index;
			}

			public bool Contains(object value) {
				return list.Contains((T)value);
			}

			public void Add(T item) {
				object newValue = item;
				if (statement.OnListAdd(key, item, ref newValue)) {
					list.Add((T)newValue);
					statement.OnListAdded(key, newValue, -1);
				}
			}

			public void Clear() {
				if (statement.OnListClear(key))
					list.Clear();
			}

			public bool Contains(T item) {
				return list.Contains(item);
			}

			public void CopyTo(T[] array, int arrayIndex) {
				list.CopyTo(array, arrayIndex);
			}

			public int IndexOf(object value) {
				return list.IndexOf((T)value);
			}

			public void Insert(int index, object value) {
				object newValue = value;
				if (statement.OnListInsert(key, index, value, ref newValue)) {
					list.Insert(index, (T)newValue);
					statement.OnListInserted(key, index, newValue);
				}
			}

			public void Remove(object value) {
				if (statement.OnListRemove(key, value)) {
					list.Remove((T)value);
					statement.OnListRemoved(key, value);
				}
			}

			public int IndexOf(T item) {
				return list.IndexOf(item);
			}

			public void Insert(int index, T item) {
				object newValue = item;
				if (statement.OnListInsert(key, index, item, ref newValue)) {
					list.Insert(index, (T)newValue);
					statement.OnListInserted(key, index, newValue);
				}
			}

			public void RemoveAt(int index) {
				if (statement.OnListRemoveAt(key, index)) {
					list.RemoveAt(index);
					statement.OnListRemoved(key, index);
				}
			}

			public T this[int index] {
				get { return list[index]; }
				set {
					object newValue = value;
					if (statement.OnListSet(key, index, value, ref newValue)) {
						list[index] = (T) newValue;
					}
				}
			}

			object IList.this[int index] {
				get { return this[index]; }
				set { this[index] = (T) value; }
			}

			public bool IsReadOnly {
				get { return false; }
			}

			public bool IsFixedSize {
				get { return false; }
			}

			#endregion
		}

		#endregion

		protected virtual void GetObjectData(SerializationInfo info, StreamingContext streamingContext) {
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext streamingContext) {
			// we don't let this part to be controlled by statement implementations
			info.AddValue("StatementTree", statementTree, typeof(StatementTree));
			info.AddValue("Query", query, typeof(SqlQuery));
			info.AddValue("Prepared", prepared);
			info.AddValue("PreparedStatements", preparedStatements);

			// but we provide a hook for them
			GetObjectData(info, streamingContext);
		}
	}
}