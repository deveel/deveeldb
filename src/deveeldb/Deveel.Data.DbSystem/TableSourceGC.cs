// 
//  Copyright 2010-2015 Deveel
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
using System.IO;

using Deveel.Data.Index;

namespace Deveel.Data.DbSystem {
	class TableSourceGC {
		private readonly TableSource tableSource;
		private BlockIndex<int> deletedRows;

		private DateTime lastSuccess;
		private DateTime? lastTry;

		private bool fullSweep;

		public TableSourceGC(TableSource tableSource) {
			this.tableSource = tableSource;
			deletedRows = new BlockIndex<int>();

			lastSuccess = DateTime.UtcNow;
			lastTry = null;
		}

		public void DeleteRow(int rowIndex) {
			if (!fullSweep) {
				if (!deletedRows.UniqueInsertSort(rowIndex))
					throw new InvalidOperationException("Row marked twice for deletion.");
			}
		}

		public void Collect(bool force) {
			try {
				int checkCount = 0;
				int deleteCount = 0;

				// Synchronize over the master data table source so no other threads
				// can interfere when we collect this information.
				lock (tableSource) {
					if (tableSource.IsClosed)
						return;

					// If root is locked, or has transaction changes pending, then we
					// can't delete any rows marked as deleted because they could be
					// referenced by transactions or result sets.
					if (force ||
						(!tableSource.IsRootLocked &&
						 !tableSource.HasChangesPending)) {

						lastSuccess = DateTime.Now;
						lastTry = null;

						// Are we due a full sweep?
						if (fullSweep) {
							int rawRowCount = tableSource.RawRowCount;
							for (int i = 0; i < rawRowCount; ++i) {
								// Synchronized in dataSource.
								if (tableSource.HardCheckAndReclaimRow(i))
									++deleteCount;

								++checkCount;
							}

							fullSweep = false;
						} else {
							// Are there any rows marked as deleted?
							int size = deletedRows.Count;
							if (size > 0) {
								// Go remove all rows marked as deleted.
								foreach (int rowIndex in deletedRows) {
									// Synchronized in dataSource.
									tableSource.HardRemoveRow(rowIndex);
									++deleteCount;
									++checkCount;
								}
							}

							deletedRows = new BlockIndex<int>();
						}

						if (checkCount > 0) {
							// TODO: Emit the information to the system
						}

					} // if not roots locked and not transactions pending

				} // lock
			} catch (IOException e) {
				// TODO: Log the error to the system
			}
		}
	}
}