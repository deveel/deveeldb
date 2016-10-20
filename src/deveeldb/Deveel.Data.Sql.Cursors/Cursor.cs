// 
//  Copyright 2010-2016 Deveel
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
//


using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Cursors {
	public sealed class Cursor : ICursor {
		internal Cursor(CursorInfo cursorInfo) {
			if (cursorInfo == null)
				throw new ArgumentNullException("cursorInfo");

			CursorInfo = cursorInfo;

			State = new CursorState();
		}

		~Cursor() {
			Dispose(false);
		}

		public CursorInfo CursorInfo { get; private set; }

		public IRequest Context { get; private set; }

		IObjectInfo IDbObject.ObjectInfo {
			get { return CursorInfo; }
		}

		public CursorStatus Status {
			get { return State.Status; }
		}

		private CursorState State { get; set; }

		public ITable Source {
			get {
				lock (this) {
					AssertNotDisposed();
					return State.Result;
				}
			}
		}

		public SqlQueryExpression QueryExpression {
			get { return CursorInfo.QueryExpression; }
		}

		private void AssertNotDisposed() {
			if (Status == CursorStatus.Disposed)
				throw new ObjectDisposedException("Cursor");
		}

		private void AssertNotClosed() {
			if (Status == CursorStatus.Closed)
				throw new CursorClosedException(CursorInfo.CursorName);
		}

		private void AssertNotOpen() {
			if (Status != CursorStatus.Closed)
				throw new CursorOpenException(CursorInfo.CursorName);
		}

		private SqlQueryExpression PrepareQuery(SqlExpression[] args) {
			SqlQueryExpression query = CursorInfo.QueryExpression;
			if (CursorInfo.Parameters.Count > 0) {
				var cursorArgs = BuildArgs(CursorInfo.Parameters, args);
				var preparer = new CursorArgumentPreparer(cursorArgs);
				query = query.Prepare(preparer) as SqlQueryExpression;
			}

			return query;
		}

		private Dictionary<string, SqlExpression> BuildArgs(IEnumerable<CursorParameter> parameters, SqlExpression[] args) {
			var orderedParams = parameters.OrderBy(x => x.Offset).ToArray();
			if (args == null || args.Length != orderedParams.Length)
				throw new ArgumentException();

			var result = new Dictionary<string, SqlExpression>();
			for (int i = 0; i < orderedParams.Length; i++) {
				var param = orderedParams[i];
				var arg = args[i];
				result[param.ParameterName] = arg;
			}

			return result;
		}

		private ITable Evaluate(IRequest context, SqlExpression[] args, out IList<IDbObject> refs) {
			try {
				var prepared = PrepareQuery(args);
				var queryPlan = context.Query.Context.QueryPlanner().PlanQuery(new QueryInfo(context, prepared));
				var refNames = queryPlan.DiscoverAccessedResources();

				refs = refNames.Select(x => context.Access().FindObject(x.ResourceName)).ToArray();
				context.Query.Session.Enter(refs, AccessType.Read);

				var tables = refs.Where(x => x.ObjectInfo.ObjectType == DbObjectType.Table).Select(x => x.ObjectInfo.FullName);
				foreach (var table in tables) {
					context.Query.Session.Transaction.GetTableManager().SelectTable(table);
				}

				return queryPlan.Evaluate(context);
			} catch(CursorException) {
				throw;
			} catch (Exception ex) {
				throw new CursorException(CursorInfo.CursorName,ex);
			}
		}

		public void Open(IRequest request, params SqlExpression[] args) {
			lock (this) {
				AssertNotDisposed();
				AssertNotOpen();

				IList<IDbObject> refs = new List<IDbObject>();

				ITable result = null;
				if (CursorInfo.IsInsensitive)
					result = Evaluate(request, args, out refs);

				State.Open(refs.ToArray(), result, args);
				Context = request;
			}
		}

		public void Close() {
			lock (this) {
				AssertNotDisposed();

				if (State.IsClosed)
					return;

				Context.Query.Session.Exit(State.References, AccessType.Read);

				State.Close();
			}
		}

		void ICursor.Reset() {
			Close();
		}

		public Row Fetch(FetchDirection direction) {
			return Fetch(direction, -1);
		}

		public Row Fetch(FetchDirection direction, int offset) {
			lock (this) {
				AssertNotClosed();

				if (!CursorInfo.IsScroll &&
				    direction != FetchDirection.Next)
					throw new ScrollCursorFetchException(CursorInfo.CursorName);

				var table = State.Result;
				if (!CursorInfo.IsInsensitive) {
					if (Context == null)
						throw new CursorOutOfContextException(CursorInfo.CursorName);

					IList<IDbObject> refs;
					table = Evaluate(Context, State.OpenArguments, out refs);
				}

				return State.FetchRowFrom(table, direction, offset);
			}
		}

		public void FetchInto(FetchContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			var fetchRow = Fetch(context.Direction, context.Offset);

			if (context.IsGlobalReference) {
				var reference = ((SqlReferenceExpression) context.Reference).ReferenceName;

				FetchIntoReference(fetchRow, reference);
			} else if (context.IsVariableReference) {
				var varNames = context.VariableNames;
				FetchIntoVatiable(fetchRow, varNames);
			}
		}

		private void FetchIntoVatiable(Row row, string[] varNames) {
			if (row.ColumnCount != varNames.Length)
				throw new FetchException(CursorInfo.CursorName, "The destination number of variables does not match the source number of columns.");

			for (int i = 0; i < varNames.Length; i++) {
				var varName = varNames[i];

				var variable = Context.Context.FindVariable(varName);
				if (variable == null)
					throw new ObjectNotFoundException(new ObjectName(varName));

				if (variable.Type is TabularType) {
					var tabular = (TabularType)variable.Type;
					// TODO: check if the table info is compatible with the row info

					throw new NotImplementedException();
				}

				var columnInfo = row.Table.TableInfo[i];
				var columnType = columnInfo.ColumnType;
				if (!variable.Type.CanCastTo(columnType))
					throw new FetchException(CursorInfo.CursorName, 
						String.Format("The value of column '{0}' is not compatible with the type of the destination variable '{1}'.",
							columnInfo.FullColumnName, varName));

				var sourceValue = row.GetValue(i);
				var destValue = sourceValue.CastTo(variable.Type);

				variable.SetValue(destValue);
			}
		}

		private void FetchIntoReference(Row row, ObjectName reference) {
			if (reference == null)
				throw new ArgumentNullException("reference");

			var table = Context.Access().GetMutableTable(reference);
			if (table == null)
				throw new ObjectNotFoundException(reference);

			try {
				Context.Query.Session.Enter(table, AccessType.Write);

				var newRow = table.NewRow();

				for (int i = 0; i < row.ColumnCount; i++) {
					var sourceValue = row.GetValue(i);
					newRow.SetValue(i, sourceValue);
				}

				newRow.SetDefault(Context);
				table.AddRow(newRow);
			} catch (CursorException) {
				throw;
			} catch (Exception ex) {
				throw new FetchException(CursorInfo.CursorName, ex);
			} finally {
				Context.Query.Session.Exit(new[] {table}, AccessType.Write);
			}

		}

		public void DeleteCurrent(IMutableTable table, IRequest request) {
			AssertNotDisposed();
			AssertNotClosed();

			var pkey = request.Query.Session.Transaction.QueryTablePrimaryKey(table.TableInfo.TableName);
			if (pkey == null)
				throw new MissingPrimaryKeyException(table.TableInfo.TableName);

			var colIndexes = new int[pkey.ColumnNames.Length];
			for (int i = 0; i < colIndexes.Length; i++) {
				var colIdx = table.TableInfo.IndexOfColumn(pkey.ColumnNames[i]);
				colIndexes[i] = colIdx;
			}

			var currentRow = State.CurrentRow;

			var values = new Field[colIndexes.Length];
			for (int i = 0; i < colIndexes.Length; i++) {
				values[i] = currentRow.GetValue(colIndexes[i]);
			}

			ITable deleteSet = table;
			for (int i = 0; i < colIndexes.Length; i++) {
				deleteSet = deleteSet.SelectEqual(colIndexes[i], values[i]);
			}

			if (deleteSet.RowCount == 0)
				return;

			table.Delete(deleteSet, 1);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (State != null)
					State.Dispose();
			}

			State = null;
		}

		public IEnumerator<Row> GetEnumerator() {
			if (Context == null &&
				!CursorInfo.IsInsensitive)
				throw new ArgumentNullException("context", "A context is required for a SCROLL cursor.");

			if (Status == CursorStatus.Closed)
				throw new InvalidOperationException(String.Format("The cursor '{0}' is closed.", CursorInfo.CursorName));

			if (Status == CursorStatus.Fetching)
				throw new InvalidOperationException(String.Format("Another enumeration is currently going on cursor '{0}': cannot double enumerate.", CursorInfo.CursorName));

			return new CursorEnumerator(this);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		#region CursorArgumentPreparer

		class CursorArgumentPreparer : IExpressionPreparer {
			private readonly Dictionary<string, SqlExpression> args;

			public CursorArgumentPreparer(Dictionary<string, SqlExpression> args) {
				this.args = args;
			}

			public bool CanPrepare(SqlExpression expression) {
				return expression is SqlVariableReferenceExpression;
			}

			public SqlExpression Prepare(SqlExpression expression) {
				var varRef = ((SqlVariableReferenceExpression) expression).VariableName;
				SqlExpression exp;
				if (!args.TryGetValue(varRef, out exp))
					throw new ArgumentException(String.Format("Variable '{0}' was not found in the cursor arguments", varRef));

				return exp;
			}
		}

		#endregion

		#region CursorEnumerator

		class CursorEnumerator : IEnumerator<Row> {
			private Cursor cursor;
			private Row currentRow;
			private int offset = -1;

			private bool disposed;

			public CursorEnumerator(Cursor cursor) {
				this.cursor = cursor;
			}

			public void Dispose() {
				currentRow = null;
				cursor = null;
				disposed = true;
			}

			private void AssertNotDisposed() {
				if (disposed)
					throw new ObjectDisposedException("CursorEnumerator");
				if (cursor.Status == CursorStatus.Closed)
					throw new InvalidOperationException(String.Format("The cursor '{0}' was closed.", cursor.CursorInfo.CursorName));
				if (cursor.Status == CursorStatus.Disposed)
					throw new ObjectDisposedException("Cursor", String.Format("The cursor '{0}' was disposed", cursor.CursorInfo.CursorName));
			}

			public bool MoveNext() {
				AssertNotDisposed();

				currentRow = cursor.Fetch(FetchDirection.Absolute, ++offset);
				return cursor.Status == CursorStatus.Fetching;
			}

			public void Reset() {
				offset = -1;
			}

			public Row Current {
				get {
					AssertNotDisposed();
					return currentRow;
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}
		}

		#endregion
	}
}
