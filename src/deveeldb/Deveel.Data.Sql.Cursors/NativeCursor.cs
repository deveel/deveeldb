// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Cursors {
	class NativeCursor : ICursor {
		public NativeCursor(NativeCursorInfo cursorInfo, IRequest context) {
			if (cursorInfo == null)
				throw new ArgumentNullException("cursorInfo");

			CursorInfo = cursorInfo;
			Context = context;

			Status = CursorStatus.Open;
		}

		~NativeCursor() {
			Dispose(false);
		}

		public const string NativeCursorName = "##NATIVE##";

		public NativeCursorInfo CursorInfo { get; private set; }

		public IRequest Context { get; private set; }

		public ITable Source {
			get { return Result; }
		}

		private IEnumerable<IDbObject> References { get; set; }
		 
		private ITable Result { get; set; }

		private bool evaluated;
		private int currentOffset;

		private void Evaluate() {
			if (!evaluated) {
				try {
					AcquireReferences();
					Result = CursorInfo.QueryPlan.Evaluate(Context);
				} finally {
					evaluated = true;
				}
			}
		}

		private void AcquireReferences() {
			var refNames = CursorInfo.QueryPlan.DiscoverTableNames();
			var refs = refNames.Select(x => Context.Access().FindObject(x)).ToArray();

			var accessType = AccessType.Read;
			if (CursorInfo.ForUpdate)
				accessType |= AccessType.Write;

			Context.Query.Session.Enter(refs, accessType);
			References = refs;
		}

		private void ReleaseReferences() {
			if (References != null) {
				Context.Query.Session.Exit(References, AccessType.Read);
			}
		}

		IObjectInfo IDbObject.ObjectInfo {
			get { return CursorInfo; }
		}

		public IEnumerator<Row> GetEnumerator() {
			return new CursorEnumerator(this);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (Status != CursorStatus.Disposed) {
				if (disposing) {
					ReleaseReferences();

					if (Result != null)
						Result.Dispose();
				}

				Result = null;
				Context = null;
				CursorInfo = null;
				Status = CursorStatus.Disposed;
			}
		}

		public CursorStatus Status { get; private set; }

		public Row Fetch(FetchDirection direction, int offset) {
			if (direction != FetchDirection.Absolute &&
				direction != FetchDirection.Relative &&
				offset > -1)
				throw new ArgumentException("Cannot set the offset for a non-relative and non-absolute fetch direction.");

			int realOffset;
			if (direction == FetchDirection.Next) {
				realOffset = currentOffset + 1;
			} else if (direction == FetchDirection.Prior) {
				realOffset = currentOffset - 1;
			} else if (direction == FetchDirection.First) {
				realOffset = 0;
			} else if (direction == FetchDirection.Last) {
				realOffset = Result.RowCount - 1;
			} else if (direction == FetchDirection.Absolute) {
				realOffset = offset;
			} else if (direction == FetchDirection.Relative) {
				realOffset = offset + currentOffset;
			} else {
				throw new ArgumentException();
			}

			if (realOffset >= Result.RowCount || realOffset < 0) {
				Status = CursorStatus.Closed;
				return null;
			}

			var row = Result.GetRow(realOffset);
			currentOffset = realOffset;
			Status = CursorStatus.Fetching;

			return row;
		}

		#region CursorEnumerator

		class CursorEnumerator : IEnumerator<Row> {
			private int offset = -1;
			private NativeCursor cursor;
			private Row currentRow;

			public CursorEnumerator(NativeCursor cursor) {
				this.cursor = cursor;
			}

			public Row Current {
				get {
					AssertIsEnumerable();
					return currentRow;
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			private void AssertIsEnumerable() {
				if (cursor == null)
					throw new ObjectDisposedException("CursorEnumerator");
				if (cursor.Status == CursorStatus.Disposed)
					throw new ObjectDisposedException("Cursor");				
			}

			public bool MoveNext() {
				AssertIsEnumerable();
				cursor.Evaluate();

				currentRow = cursor.Fetch(FetchDirection.Absolute, ++offset);
				return cursor.Status == CursorStatus.Fetching;
			}

			public void Reset() {
				offset = -1;
			}

			public void Dispose() {
				cursor = null;
			}
		}

		#endregion
	}
}
