using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

using IQToolkit;

namespace Deveel.Data.Linq {
	public sealed class DbTable<T> : IDbTable<T> where T : class {
		private QueryProvider provider;
		private TableQuery query;

		internal DbTable(QueryProvider provider) {
			this.provider = provider;
			query = new TableQuery(provider);
		}

		~DbTable() {
			Dispose(false);
		}


		IEnumerator<T> IEnumerable<T>.GetEnumerator() {
			return query.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return (this as IEnumerable<T>).GetEnumerator();
		}

		Expression IQueryable.Expression {
			get { return query.Expression; }
		}

		Type IQueryable.ElementType {
			get { return query.ElementType; }
		}

		IQueryProvider IQueryable.Provider {
			get { return provider; }
		}

		public T FindByKey(object key) {
			throw new NotImplementedException();
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			provider = null;
			query = null;
		}

		#region TableQuery

		class TableQuery : Query<T> {
			public TableQuery(QueryProvider provider) 
				: base(provider) {
			}
		}

		#endregion
	}
}
