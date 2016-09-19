using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Deveel.Data.Linq {
	public sealed class QueryTable<TType> : IQueryTable<TType>, IUpdateQueryTable where TType : class {
		private IQueryable<TType> queryable;

		internal QueryTable(SessionQueryContext context, IQueryable<TType> queryable) {
			this.queryable = queryable;
			Context = context;
		}

		~QueryTable() {
			Dispose(false);
		}

		private SessionQueryContext Context { get; set; }

		IEnumerator<TType> IEnumerable<TType>.GetEnumerator() {
			return queryable.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return queryable.GetEnumerator();
		}

		Expression IQueryable.Expression {
			get { return queryable.Expression; }
		}

		Type IQueryable.ElementType {
			get { return queryable.ElementType; }
		}

		IQueryProvider IQueryable.Provider {
			get { return queryable.Provider; }
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (queryable != null &&
					(queryable is IDisposable))
					(queryable as IDisposable).Dispose();
			}

			Context = null;
			queryable = null;
		}

		Type IQueryTable.Type {
			get { return queryable.ElementType; }
		}

		public TType FindByKey(object key) {
			throw new NotImplementedException();
		}

		object IQueryTable.FindByKey(object key) {
			return FindByKey(key);
		}

		object IUpdateQueryTable.Insert(object obj) {
			return Insert((TType) obj);
		}

		public TType Insert(TType obj) {
			throw new NotImplementedException();
		}

		bool IUpdateQueryTable.Update(object obj) {
			return Update((TType) obj);
		}

		public bool Update(TType obj) {
			throw new NotImplementedException();
		}

		bool IUpdateQueryTable.Delete(object obj) {
			return Delete((TType) obj);
		}

		public bool Delete(object obj) {
			throw new NotImplementedException();
		}
	}
}
