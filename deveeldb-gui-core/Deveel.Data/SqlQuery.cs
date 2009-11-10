using System;
using System.Data;

using Deveel.Data.Client;

namespace Deveel.Data {
	public sealed class SqlQuery {
		public SqlQuery(string text) {
			this.text = text;
		}

		private readonly string text;
		private DateTime startTime;
		private DateTime endTime;
		private DataSet result;

		public string Text {
			get { return text; }
		}

		public DateTime EndTime {
			get { return endTime; }
		}

		public DateTime StartTime {
			get { return startTime; }
		}

		public DataSet Result {
			get { return result; }
		}

		internal void Execute(DeveelDbConnection connection, int index) {
			startTime = DateTime.Now;

			DeveelDbDataAdapter adapter = new DeveelDbDataAdapter(connection.CreateCommand(text));
			result = new DataSet("Query " + index);
			adapter.Fill(result);

			endTime = DateTime.Now;
		}
	}
}