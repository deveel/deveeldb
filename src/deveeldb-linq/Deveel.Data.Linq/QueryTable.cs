using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

using IQToolkit;

namespace Deveel.Data.Linq {
	public sealed class QueryTable<T> : IQueryable<T> where T : class {
		internal QueryTable(QueryContext context) {
			QueryContext = context;
		}

		public QueryContext QueryContext { get; private set; }

		private IEntityTable<T> EntityTable {
			get { return QueryContext.GetTable<T>(); }
		}

		IEnumerator<T> IEnumerable<T>.GetEnumerator() {
			return EntityTable.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return EntityTable.GetEnumerator();
		}

		Expression IQueryable.Expression {
			get { return EntityTable.Expression; }
		}

		Type IQueryable.ElementType {
			get { return EntityTable.ElementType; }
		}

		IQueryProvider IQueryable.Provider {
			get { return EntityTable.Provider; }
		}

		public int Remove(T entity) {
			return EntityTable.Delete(entity);
		}

		public T FindById(object id) {
			if (id == null)
				throw new ArgumentNullException("id");

			try {
				return EntityTable.GetById(id);
			} catch (Exception ex) {
				throw new QueryException(String.Format("Error while querying type '{0}' by id.", typeof(T)), ex);
			}
		}
		
		public int Update(T entity) {
			return EntityTable.Update(entity);
		}

		public int Add(T entity) {
			return EntityTable.Insert(entity);
		}
	}
}
