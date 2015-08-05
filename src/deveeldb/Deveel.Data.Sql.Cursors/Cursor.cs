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

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Cursors {
	public sealed class Cursor : IDbObject, IDisposable {
		internal Cursor(CursorManager manager, CursorInfo cursorInfo) {
			if (manager == null)
				throw new ArgumentNullException("manager");
			if (cursorInfo == null)
				throw new ArgumentNullException("cursorInfo");

			Manager = manager;
			CursorInfo = cursorInfo;
		}

		~Cursor() {
			Dispose(false);
		}

		public CursorInfo CursorInfo { get; private set; }

		public ITable Result { get; private set; }

		public CursorState State { get; private set; }

		public int CurrentOffset { get; private set; }

		public CursorManager Manager { get; private set; }

		ObjectName IDbObject.FullName {
			get { return CursorInfo.CursorName; }
		}

		DbObjectType IDbObject.ObjectType {
			get { return DbObjectType.Cursor; }
		}

		private void AssertNotDisposed() {
			if (State == CursorState.Disposed)
				throw new ObjectDisposedException("Cursor");
		}

		private SqlExpression PrepareQuery(SqlExpression[] args) {
			SqlExpression query = CursorInfo.QueryExpression;
			if (CursorInfo.Parameters.Count > 0) {
				var cursorArgs = BuildArgs(CursorInfo.Parameters, args);
				var preparer = new CursorArgumentPreparer(cursorArgs);
				query = query.Prepare(preparer);
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

		public void Open(IQueryContext context, params SqlExpression[] args) {
			lock (this) {
				AssertNotDisposed();

				try {
					var prepared = PrepareQuery(args);
					var evalQuery = prepared.Evaluate(context, null);
					if (evalQuery.ExpressionType != SqlExpressionType.Constant ||
						!(((SqlConstantExpression)evalQuery).Value.Type is QueryType))
						throw new InvalidOperationException();

					var queryPlan = ((SqlQueryObject) ((SqlConstantExpression) evalQuery).Value.Value).QueryPlan;
					Result = queryPlan.Evaluate(context);
				} catch (Exception) {
					
					throw;
				}
			}
		}

		public void Close() {
			lock (this) {
				AssertNotDisposed();
				CurrentOffset = -1;
				State = CursorState.Closed;
				Result = null;
			}
		}

		public void FetchInto(IQueryContext context, ObjectName refName) {
			throw new NotImplementedException();
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (Result != null)
					Result.Dispose();

				if (Manager != null)
					Manager.DisposeCursor(this);
			}

			Manager = null;
			Result = null;
			State = CursorState.Disposed;
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
