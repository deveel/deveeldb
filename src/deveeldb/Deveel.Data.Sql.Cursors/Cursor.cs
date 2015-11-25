// 
//  Copyright 2010-2015 Deveel
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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Cursors {
	public sealed class Cursor : IDbObject, IDisposable {
		internal Cursor(CursorInfo cursorInfo) {
			if (cursorInfo == null)
				throw new ArgumentNullException("cursorInfo");

			CursorInfo = cursorInfo;
			State = new CursorState(this);
		}

		~Cursor() {
			Dispose(false);
		}

		public CursorInfo CursorInfo { get; private set; }

		public CursorStatus Status {
			get { return State.Status; }
		}

		public CursorState State { get; private set; }

		ObjectName IDbObject.FullName {
			get { return new ObjectName(CursorInfo.CursorName); }
		}

		DbObjectType IDbObject.ObjectType {
			get { return DbObjectType.Cursor; }
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

		private ITable Evaluate(IRequest context, SqlExpression[] args) {
			try {
				var prepared = PrepareQuery(args);
				var queryPlan = context.Query.Context.QueryPlanner().PlanQuery(context, prepared, null, null);
				return queryPlan.Evaluate(context);
			} catch (Exception) {

				throw;
			}
		}

		public void Open(IRequest context, params SqlExpression[] args) {
			lock (this) {
				AssertNotDisposed();

				ITable result = null;
				if (CursorInfo.IsInsensitive)
					result = Evaluate(context, args);

				State.Open(result, args);
			}
		}

		public void Close() {
			lock (this) {
				AssertNotDisposed();
				State.Close();
			}
		}

		public void FetchInto(FetchContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			if (!CursorInfo.IsScroll &&
				context.Direction != FetchDirection.Next)
				throw new ArgumentException(String.Format("Cursor '{0}' is not SCROLL: can fetch only NEXT value.", CursorInfo.CursorName));

			var table = State.Result;
			if (!CursorInfo.IsInsensitive)
				table = Evaluate(context.Query, State.OpenArguments);

			var fetchRow = State.FetchRowFrom(table, context.Direction, context.Offset);

			if (context.IsGlobalReference) {
				var reference = ((SqlReferenceExpression) context.Reference).ReferenceName;
				FetchIntoReference(context.Query, fetchRow, reference);
			} else if (context.IsVariableReference) {
				var varName = ((SqlVariableReferenceExpression) context.Reference).VariableName;
				FetchIntoVatiable(context.Query, fetchRow, varName);
			}
		}

		private void FetchIntoVatiable(IQuery query, Row row, string varName) {
			throw new NotImplementedException();
		}

		private void FetchIntoReference(IQuery query, Row row, ObjectName reference) {
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
	}
}
