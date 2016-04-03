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
		internal Cursor(CursorInfo cursorInfo, IRequest context) {
			if (cursorInfo == null)
				throw new ArgumentNullException("cursorInfo");
			if (context == null)
				throw new ArgumentNullException("context");

			CursorInfo = cursorInfo;

			Context = context;
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

		private ITable Evaluate(SqlExpression[] args, out IList<IDbObject> refs) {
			try {
				var prepared = PrepareQuery(args);
				var queryPlan = Context.Query.Context.QueryPlanner().PlanQuery(new QueryInfo(Context, prepared));
				var refNames = queryPlan.DiscoverTableNames();

				refs = refNames.Select(x => Context.Access().FindObject(x)).ToArray();
				Context.Query.Session.Enter(refs, AccessType.Read);

				return queryPlan.Evaluate(Context);
			} catch (Exception) {

				throw;
			}
		}

		public void Open(params SqlExpression[] args) {
			lock (this) {
				AssertNotDisposed();

				IList<IDbObject> refs = new List<IDbObject>();

				ITable result = null;
				if (CursorInfo.IsInsensitive)
					result = Evaluate(args, out refs);

				State.Open(refs.ToArray(), result, args);
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

		public Row Fetch(FetchDirection direction) {
			return Fetch(Context, direction);
		}

		public Row Fetch(IRequest request, FetchDirection direction) {
			return Fetch(request, direction, -1);
		}

		public Row Fetch(FetchDirection direction, int offset) {
			return Fetch(Context, direction, offset);
		}

		public Row Fetch(IRequest request, FetchDirection direction, int offset) {
			lock (this) {
				if (!CursorInfo.IsScroll &&
				    direction != FetchDirection.Next)
					throw new ArgumentException(String.Format("Cursor '{0}' is not SCROLL: can fetch only NEXT value.",
						CursorInfo.CursorName));

				var table = State.Result;
				if (!CursorInfo.IsInsensitive) {
					if (request == null)
						throw new ArgumentNullException("request", String.Format("The sensitive cursor '{0}' requires an active context.", CursorInfo.CursorName));

					IList<IDbObject> refs;
					table = Evaluate(State.OpenArguments, out refs);
				}

				return State.FetchRowFrom(table, direction, offset);
			}
		}

		public void FetchInto(FetchContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			var fetchRow = Fetch(context.Request, context.Direction, context.Offset);

			if (context.IsGlobalReference) {
				var reference = ((SqlReferenceExpression) context.Reference).ReferenceName;

				FetchIntoReference(fetchRow, reference);
			} else if (context.IsVariableReference) {
				var varName = ((SqlVariableReferenceExpression) context.Reference).VariableName;
				FetchIntoVatiable(fetchRow, varName);
			}
		}

		private void FetchIntoVatiable(Row row, string varName) {
			var variable = Context.Context.FindVariable(varName);
			if (variable == null)
				throw new InvalidOperationException(String.Format("Variable '{0}' was not found in current scope.", varName));

			if (variable.Type is TabularType) {
				var tabular = (TabularType) variable.Type;
				// TODO: check if the table info is compatible with the row info
			} else {
				if (row.ColumnCount != 1)
					throw new NotSupportedException();

				// TODO: find the variable type and cast the source type
				// TODO: set the value from the row into the variable
			}

			throw new NotImplementedException();
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
			} finally {
				Context.Query.Session.Exit(new []{table}, AccessType.Write);
			}

		}

		public void DeleteCurrent(IMutableTable table, IRequest request) {
			throw new NotImplementedException();
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

		public IEnumerator<Row> GetEnumerator(IRequest context) {
			if (context == null &&
				!CursorInfo.IsInsensitive)
				throw new ArgumentNullException("context", "A context is required for a SCROLL cursor.");

			if (Status == CursorStatus.Closed)
				throw new InvalidOperationException(String.Format("The cursor '{0}' is closed.", CursorInfo.CursorName));

			if (Status == CursorStatus.Fetching)
				throw new InvalidOperationException(String.Format("Another enumeration is currently going on cursor '{0}': cannot double enumerate.", CursorInfo.CursorName));

			return new CursorEnumerator(this, context);
		}

		public IEnumerator<Row> GetEnumerator() {
			return GetEnumerator(null);
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
			private IRequest context;
			private Row currentRow;
			private int offset = -1;

			private bool disposed;

			public CursorEnumerator(Cursor cursor, IRequest context) {
				this.cursor = cursor;
				this.context = context;
			}

			public void Dispose() {
				context = null;
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

				currentRow = cursor.Fetch(context, FetchDirection.Absolute, ++offset);
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
