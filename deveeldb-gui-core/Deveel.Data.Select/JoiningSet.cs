using System;
using System.Collections;

namespace Deveel.Data.Select {
	public sealed class JoiningSet {
		public JoiningSet() {
			joinSet = new ArrayList();
		}

		private readonly ArrayList joinSet;

		public void AddTable(string tableName) {
			joinSet.Add(tableName);
		}

		public void AddPreviousJoin(JoinType type, string onExpression) {
			joinSet.Insert(joinSet.Count - 1, new JoinPart(type, onExpression));
		}

		public void AddJoin(JoinType type, string onExpression) {
			joinSet.Add(new JoinPart(type, onExpression));
		}

		public void AddJoin(JoinType type) {
			joinSet.Add(new JoinPart(type));
		}

		public int TableCount {
			get { return (joinSet.Count + 1) / 2; }
		}

		public string FirstTable {
			get { return this[0]; }
		}

		public string this[int n] {
			get { return (string)joinSet[n * 2]; }
		}

		public JoinType GetJoinType(int n) {
			return ((JoinPart)joinSet[(n * 2) + 1]).Type;
		}

		public string GetOnExpression(int n) {
			return ((JoinPart)joinSet[(n * 2) + 1]).OnExpression;
		}
	}
}