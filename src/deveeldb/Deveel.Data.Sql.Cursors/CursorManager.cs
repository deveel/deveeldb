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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Cursors {
	public sealed class CursorManager : IObjectManager {
		private List<Cursor> cursors;

		public CursorManager(IQueryContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			Context = context;
			cursors = new List<Cursor>();
		}

		~CursorManager() {
			Dispose(false);
		}

		public IQueryContext Context { get; private set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (cursors != null) {
					foreach (var cursor in cursors) {
						cursor.Dispose();
					}

					cursors.Clear();
				}
			}

			cursors = null;
			Context = null;
		}

		DbObjectType IObjectManager.ObjectType {
			get { return DbObjectType.Cursor; }
		}

		void IObjectManager.Create() {
		}

		void IObjectManager.CreateObject(IObjectInfo objInfo) {
			DeclareCursor((CursorInfo)objInfo);
		}

		bool IObjectManager.RealObjectExists(ObjectName objName) {
			return CursorExists(objName);
		}

		bool IObjectManager.ObjectExists(ObjectName objName) {
			return CursorExists(objName);
		}

		IDbObject IObjectManager.GetObject(ObjectName objName) {
			return GetCursor(objName.Name);
		}

		bool IObjectManager.AlterObject(IObjectInfo objInfo) {
			throw new NotSupportedException("Cannot alter a cursor");
		}

		bool IObjectManager.DropObject(ObjectName objName) {
			return DropCursor(objName);
		}

		public ObjectName ResolveName(ObjectName objName, bool ignoreCase) {
			var ojectName = objName.Name;
			var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
			foreach (var cursor in cursors) {
				var cursorName = cursor.CursorInfo.CursorName;
				if (cursorName.Equals(ojectName, comparison))
					return new ObjectName(cursorName);
			}

			return null;
		}

		public void DeclareCursor(CursorInfo cursorInfo) {
			if (cursorInfo == null)
				throw new ArgumentNullException("cursorInfo");


			lock (this) {
				var cursorName = cursorInfo.CursorName;
				if (cursors.Any(x => x.CursorInfo.CursorName.Equals(cursorName, StringComparison.OrdinalIgnoreCase)))
					throw new ArgumentException(String.Format("Cursor '{0}' was already declared.", cursorName));

				var cursor = new Cursor(cursorInfo);
				cursors.Add(cursor);
			}
		}

		public bool CursorExists(ObjectName cursorName) {
			var ignoreCase = Context.IgnoreIdentifiersCase();
			var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
			return cursors.Any(x => x.CursorInfo.CursorName.Equals(cursorName.Name, comparison));
		}

		public Cursor GetCursor(string cursorName) {
			var ignoreCase = Context.IgnoreIdentifiersCase();
			var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
			return cursors.FirstOrDefault(x => x.CursorInfo.CursorName.Equals(cursorName, comparison));
		}

		internal void DisposeCursor(Cursor cursor) {
			var name = cursor.CursorInfo.CursorName;
			for (int i = cursors.Count - 1; i >= 0; i--) {
				var cursorName = cursors[i].CursorInfo.CursorName;
				if (cursorName.Equals(name, StringComparison.OrdinalIgnoreCase))
					cursors.RemoveAt(i);
			}
		}

		public bool DropCursor(ObjectName cursorName) {
			for (int i = cursors.Count - 1; i >= 0; i--) {
				var cursor = cursors[i];
				if (cursor.CursorInfo.CursorName.Equals(cursorName.Name, StringComparison.OrdinalIgnoreCase)) {
					cursors.RemoveAt(i);
					cursor.Dispose();
					return true;
				}
			}

			return false;
		}
	}
}
