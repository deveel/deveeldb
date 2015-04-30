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
using System.Resources;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public sealed class ViewManager : IObjectManager {
		private Dictionary<long, View> viewCache;
 
		public ViewManager(ITransaction transaction) {
			if (transaction == null)
				throw new ArgumentNullException("transaction");

			Transaction = transaction;
			viewCache = new Dictionary<long, View>();
		}

		public ITransaction Transaction { get; private set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			Transaction = null;
		}

		DbObjectType IObjectManager.ObjectType {
			get { return DbObjectType.View; }
		}

		void IObjectManager.CreateObject(IObjectInfo objInfo) {
			var viewInfo = objInfo as ViewInfo;
			if (viewInfo == null)
				throw new ArgumentException();

			DefineView(viewInfo);
		}

		bool IObjectManager.RealObjectExists(ObjectName objName) {
			return ViewExists(objName);
		}

		bool IObjectManager.ObjectExists(ObjectName objName) {
			return ViewExists(objName);
		}

		IDbObject IObjectManager.GetObject(ObjectName objName) {
			return GetView(objName);
		}

		bool IObjectManager.AlterObject(IObjectInfo objInfo) {
			throw new NotSupportedException();
		}

		bool IObjectManager.DropObject(ObjectName objName) {
			return DropView(objName);
		}

		public ObjectName ResolveName(ObjectName objName, bool ignoreCase) {
			throw new NotImplementedException();
		}

		public void DefineView(ViewInfo viewInfo) {
			throw new NotImplementedException();
		}

		public View GetView(ObjectName viewName) {
			throw new NotImplementedException();
		}

		public bool ViewExists(ObjectName viewName) {
			throw new NotImplementedException();
		}

		public bool DropView(ObjectName viewName) {
			throw new NotImplementedException();
		}

		public ITableContainer CreateInternalTableInfo() {
			return new ViewTableContainer(this);
		}

		private View GetViewAt(int offset) {
			var table = Transaction.GetTable(SystemSchema.ViewTableName);
			if (table == null)
				throw new DatabaseSystemException(String.Format("System table '{0}' was not defined.", SystemSchema.ViewTableName));

			var e = table.GetEnumerator();
			int i = 0;
			while (e.MoveNext()) {
				var row = e.Current.RowId.RowNumber;

				if (i == offset) {
					View view;
					if (!viewCache.TryGetValue(row, out view)) {
						// Not in the cache, so deserialize it and write it in the cache.
						var binary = (ISqlBinary)table.GetValue(row, 3).Value;
						// Derserialize the binary
						view = View.Deserialize(binary);
						// Put this in the cache....
						viewCache[row] = view;
					}

					return view;
				}

				++i;
			}

			throw new ArgumentOutOfRangeException("offset");
		}

		#region ViewTableContainer

		class ViewTableContainer : SystemTableContainer {
			private readonly ViewManager viewManager;

			public ViewTableContainer(ViewManager viewManager)
				: base(viewManager.Transaction, SystemSchema.ViewTableName) {
				this.viewManager = viewManager;
			}

			public override TableInfo GetTableInfo(int offset) {
				var view = viewManager.GetViewAt(offset);
				if (view == null)
					return null;

				return view.ViewInfo.TableInfo;
			}

			public override string GetTableType(int offset) {
				return TableTypes.View;
			}

			public override ITable GetTable(int offset) {
				throw new NotSupportedException();
			}
		}

		#endregion
	}
}
