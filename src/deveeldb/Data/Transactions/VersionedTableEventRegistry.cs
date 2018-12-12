// 
//  Copyright 2010-2018 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Transactions {
	public sealed class VersionedTableEventRegistry : IEnumerable<ITableEventRegistry>, IDisposable {
		private List<ITableEventRegistry> eventRegistries;

		public VersionedTableEventRegistry() {
			eventRegistries = new List<ITableEventRegistry>();
		}

		public bool HasChanges => eventRegistries.Any();

		private void Dispose(bool disposing) {
			if (disposing) {
				if (eventRegistries != null)
					eventRegistries.Clear();
			}

			eventRegistries = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public void AddRegistry(ITableEventRegistry registry) {
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

		public IEnumerable<ITableEventRegistry> FindSinceCommit(long commitId) {
			return eventRegistries.Where(x => x.CommitId >= commitId);
		}

		public IEnumerator<ITableEventRegistry> GetEnumerator() {
			return eventRegistries.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}
	}
}