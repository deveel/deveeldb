using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Design;
using Deveel.Data.Sql.Statements;

using Remotion.Linq;

namespace Deveel.Data.Linq {
	class QueryExecutor : IQueryExecutor {
		private ISession session;
		private DbCompiledModel model;

		public QueryExecutor(ISession session, DbCompiledModel model) {
			this.session = session;
			this.model = model;
		}

		private SelectStatement BuildSelectStatement(IQuery context, QueryModel queryModel) {
			return SqlSelectGeneratorVisitor.GenerateSelect(context, model, queryModel);
		}

		public T ExecuteScalar<T>(QueryModel queryModel) {
			return ExecuteCollection<T>(queryModel).First();
		}

		public T ExecuteSingle<T>(QueryModel queryModel, bool returnDefaultWhenEmpty) {
			return returnDefaultWhenEmpty
				? ExecuteCollection<T>(queryModel).FirstOrDefault()
				: ExecuteCollection<T>(queryModel).First();
		}


		public IEnumerable<T> ExecuteCollection<T>(QueryModel queryModel) {
			var query = session.CreateQuery();
			var statement = BuildSelectStatement(query, queryModel);

			var result = query.ExecuteStatement(statement);

			return new QueryResultEnumerable<T>(query, model, result);
		}

		#region QueryResultEnumerable

		class QueryResultEnumerable<T> : IEnumerable<T>, IDisposable {
			private IQuery query;
			private StatementResult result;
			private DbCompiledModel model;

			public QueryResultEnumerable(IQuery query, DbCompiledModel model, StatementResult result) {
				this.query = query;
				this.result = result;
				this.model = model;
			}

			~QueryResultEnumerable() {
				Dispose(false);
			}

			public IEnumerator<T> GetEnumerator() {
				if (result.Type == StatementResultType.Exception)
					throw result.Error;

				return new QueryResultEnumerator(this);
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			private void Dispose(bool disposing) {
				if (disposing) {
					if (result != null)
						result.Dispose();

					if (query != null)
						query.Dispose();
				}

				result = null;
				query = null;
				model = null;
			}

			public void Dispose() {
				Dispose(true);
				GC.SuppressFinalize(this);
			}

			#region QueryResultEnumerator

			class QueryResultEnumerator : IEnumerator<T> {
				private QueryResultEnumerable<T> parent;
				private IEnumerator<T> enumerator;
				private bool disposed;

				public QueryResultEnumerator(QueryResultEnumerable<T> parent) {
					this.parent = parent;

					IEnumerable<T> enumerable;

					if (parent.result.Type == StatementResultType.CursorRef) {
						enumerable = parent.result.Cursor.Select(x => x.ToObject<T>(parent.query, parent.model));
					} else if (parent.result.Type == StatementResultType.Result) {
						enumerable = parent.result.Result.Select(x => x.ToObject<T>(parent.query, parent.model));
					} else {
						enumerable = new T[0];
					}

					enumerator = enumerable.GetEnumerator();
				}

				public void Dispose() {
					if (!disposed) {
						if (enumerator != null)
							enumerator.Dispose();

						if (parent != null)
							parent.Dispose();
					}

					parent = null;
					enumerator = null;
					disposed = true;
				}

				public bool MoveNext() {
					return enumerator.MoveNext();
				}

				public void Reset() {
					enumerator.Reset();
				}

				public T Current {
					get { return enumerator.Current; }
				}

				object IEnumerator.Current {
					get { return Current; }
				}
			}

			#endregion
		}

		#endregion
	}
}
