using System;
using System.Collections;
using System.Collections.Generic;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Cursors {
	class NativeCursor : ICursor {
		public NativeCursor(NativeCursorInfo cursorInfo, IRequest context) {
			if (cursorInfo == null)
				throw new ArgumentNullException("cursorInfo");

			CursorInfo = cursorInfo;
			Context = context;
		}

		public const string NativeCursorName = "##NATIVE##";

		public NativeCursorInfo CursorInfo { get; private set; }

		public IRequest Context { get; private set; }

		IObjectInfo IDbObject.ObjectInfo {
			get { return CursorInfo; }
		}

		public IEnumerator<Row> GetEnumerator() {
			throw new NotImplementedException();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public void Dispose() {
			throw new NotImplementedException();
		}

		public CursorStatus Status { get; }

		public Row Fetch(FetchDirection direction, int offset) {
			throw new NotImplementedException();
		}
	}
}
