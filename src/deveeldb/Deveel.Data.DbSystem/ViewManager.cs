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
