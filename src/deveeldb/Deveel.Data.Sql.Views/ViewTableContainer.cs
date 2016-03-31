using System;

using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Views {
	class ViewTableContainer : SystemTableContainer {

		public ViewTableContainer(ITransaction transaction)
			: base(transaction, ViewManager.ViewTableName) {
		}

		private ViewManager ViewManager {
			get {
				var manager = Transaction.GetObjectManager(DbObjectType.View) as ViewManager;
				if (manager == null)
					throw new InvalidOperationException("Invalid view manager in context.");

				return manager;
			}
		}

		public override TableInfo GetTableInfo(int offset) {
			var view = ViewManager.GetViewAt(offset);
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
}