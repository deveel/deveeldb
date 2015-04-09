using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Transactions {
	public sealed class TableCommitInfo {
		public TableCommitInfo(int commitId, ObjectName tableName, IEnumerable<int> addedRows, IEnumerable<int> removedRows) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			CommitId = commitId;
			TableName = tableName;

			if (addedRows != null)
				AddedRows = addedRows.ToList();
			if (removedRows != null)
				RemovedRows = removedRows.ToList();
		}

		public int CommitId { get; private set; }

		public ObjectName TableName { get; private set; }

		public IEnumerable<int> AddedRows { get; private set; }

		public IEnumerable<int> RemovedRows { get; private set; } 
	}
}