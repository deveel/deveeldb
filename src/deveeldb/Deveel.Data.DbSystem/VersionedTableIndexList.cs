using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql;

namespace Deveel.Data.DbSystem {
	class VersionedTableIndexList {
		private readonly List<TableEventRegistry> eventRegistries;
 
		public VersionedTableIndexList(TableSource tableSource) {
			TableSource = tableSource;

			eventRegistries = new List<TableEventRegistry>();
		}

		public IDatabaseContext DatabaseContext {
			get { return TableSource.DatabaseContext; }
		}

		public TableSource TableSource { get; private set; }

		public bool HasChangesPending {
			get { return eventRegistries.Any(); }
		}

		public void AddRegistry(TableEventRegistry registry) {
			eventRegistries.Add(registry);
		}

		public bool MergeChanges(long commitId) {
			// TODO: report the stat to the system

			while (eventRegistries.Count > 0) {
				var registry = eventRegistries[0];

				if (commitId > registry.CommitId) {
					// Remove the top registry from the list.
					eventRegistries.RemoveAt(0);
				} else {
					return false;
				}
			}

			return true;
		}

		public IEnumerable<TableEventRegistry> FindSinceCommit(long commitId) {
			return eventRegistries.Where(x => x.CommitId >= commitId);
		}
	}
}