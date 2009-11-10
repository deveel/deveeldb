using System;

namespace Deveel.Data {
	public delegate void QueryEventHandler(object sender, QueryEventArgs args);

	public sealed class QueryEventArgs : EventArgs {
		internal QueryEventArgs(SqlQuery query, int index, int count) {
			this.query = query;
			this.index = index;
			this.count = count;
		}

		private readonly SqlQuery query;
		private readonly int index;
		private readonly int count;

		public int Count {
			get { return count; }
		}

		public int Index {
			get { return index; }
		}

		public SqlQuery Query {
			get { return query; }
		}
	}
}