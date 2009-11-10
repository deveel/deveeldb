using System;
using System.Collections;
using System.Text.RegularExpressions;

namespace Deveel.Data {
	public sealed class SqlQueryBatch {
		public SqlQueryBatch() {
			queries = new ArrayList();
		}

		public SqlQueryBatch(string text)
			: this() {

			if (text == null || text.Trim().Length == 0)
				return;

			string[] st = Split(text, ";");
			for (int i = 0; i < st.Length; i++) {
				AddQuery(st[i]);
			}
		}

		private readonly ArrayList queries;
		private DateTime startTime;
		private DateTime endTime;
		private bool busy;

		public SqlQuery this[int index] {
			get { return queries[index] as SqlQuery; }
		}

		public DateTime EndTime {
			get { return endTime; }
		}

		public DateTime StartTime {
			get { return startTime; }
		}

		public TimeSpan Elapsed {
			get { return endTime - startTime; }
		}

		public int QueryCount {
			get { return queries.Count; }
		}

		public bool IsBusy {
			get { return busy; }
		}

		internal void Start() {
			startTime = DateTime.Now;
			busy = true;
		}

		internal void End() {
			endTime = DateTime.Now;
			busy = false;
		}

		private static string[] Split(string script, string indicator) {
			string pattern = string.Concat("^\\s*", indicator, "\\s*$");
			const RegexOptions options = RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Multiline;

			ArrayList list = new ArrayList();
			foreach (string batch in Regex.Split(script, pattern, options)) {
				list.Add(batch.Trim());
			}

			return (string[]) list.ToArray(typeof (string));
		}

		public void AddQuery(string text) {
			if (busy)
				throw new InvalidOperationException();

			queries.Add(new SqlQuery(text));
		}

		public void Clear() {
			if (busy)
				throw new InvalidOperationException();

			queries.Clear();
		}
	}
}