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

using Deveel.Data.Sql;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public sealed class ViewManager : IObjectManager {
		public ViewManager(ITransaction transaction) {
			if (transaction == null)
				throw new ArgumentNullException("transaction");

			Transaction = transaction;
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
	}
}
