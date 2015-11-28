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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Views {
	public sealed class ViewManager : IObjectManager {
		private Dictionary<long, ViewInfo> viewCache;
		private bool viewTableChanged;

		public ViewManager(ITransaction transaction) {
			if (transaction == null)
				throw new ArgumentNullException("transaction");

			Transaction = transaction;
			viewCache = new Dictionary<long, ViewInfo>();

			transaction.RegisterOnCommit(OnCommit);
		}

		private void OnCommit(TableCommitInfo obj) {
			if (!obj.TableName.Equals(SystemSchema.ViewTableName))
				return;

			// If there were changed then invalidate the cache
			if (viewTableChanged) {
				InvalidateViewCache();
				viewTableChanged = false;
			} else if ((obj.AddedRows != null && obj.AddedRows.Any()) ||
					 (obj.RemovedRows != null && obj.RemovedRows.Any())) {
				// Otherwise, if there were committed added or removed changes also
				// invalidate the cache,
				InvalidateViewCache();
			}
		}

		private void InvalidateViewCache() {
			viewCache.Clear();
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

		private ITable FindViewEntry(ObjectName viewName) {
			var table = Transaction.GetTable(SystemSchema.ViewTableName);

			var schemav = table.GetResolvedColumnName(0);
			var namev = table.GetResolvedColumnName(1);

			using (var session = new SystemSession(Transaction, SystemSchema.Name)) {
				using (var query =session.CreateQuery()) {
					var t = table.SimpleSelect(query, namev, SqlExpressionType.Equal,
						SqlExpression.Constant(DataObject.String(viewName.Name)));
					t = t.ExhaustiveSelect(query,
						SqlExpression.Equal(SqlExpression.Reference(schemav), SqlExpression.Constant(viewName.ParentName)));

					// This should be at most 1 row in size
					if (t.RowCount > 1)
						throw new ArgumentException(String.Format("Multiple view entries for name '{0}' in the system.", viewName));

					// Return the entries found.
					return t;
				}
			}
		}

		public void Create() {
			var tableInfo = new TableInfo(SystemSchema.ViewTableName);
			tableInfo.AddColumn("schema", PrimitiveTypes.String());
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("query", PrimitiveTypes.String());
			tableInfo.AddColumn("plan", PrimitiveTypes.Binary());

			// TODO: Columns...

			Transaction.CreateTable(tableInfo);
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
			if (viewInfo == null)
				throw new ArgumentNullException("viewInfo");

			var dataTableInfo = viewInfo.TableInfo;
			var viewTable = Transaction.GetMutableTable(SystemSchema.ViewTableName);

			var viewName = dataTableInfo.TableName;
			var query = viewInfo.QueryExpression;
			var viewInfoData = viewInfo.AsBinary();

			// Create the view record
			var rdat = viewTable.NewRow();
			rdat.SetValue(0, dataTableInfo.SchemaName.Name);
			rdat.SetValue(1, dataTableInfo.Name);
			rdat.SetValue(2, query.ToString());
			rdat.SetValue(3, DataObject.Binary(viewInfoData));

			// Find the entry from the view that equals this name
			var t = FindViewEntry(viewName);

			// Delete the entry if it already exists.
			if (t.RowCount == 1) {
				viewTable.Delete(t);
			}

			// Insert the new view entry in the system view table
			viewTable.AddRow(rdat);

			// Notify that this database object has been successfully created.
			Transaction.Registry.RegisterEvent(new ObjectCreatedEvent(viewName, DbObjectType.View));

			// Change to the view table
			viewTableChanged = true;
		}

		public View GetView(ObjectName viewName) {
			var viewTable = Transaction.GetTable(SystemSchema.ViewTableName);
			var e = viewTable.GetEnumerator();
			while (e.MoveNext()) {
				int row = e.Current.RowId.RowNumber;

				var cSchema = viewTable.GetValue(row, 0).Value.ToString();
				var cName = viewTable.GetValue(row, 1).Value.ToString();

				if (viewName.ParentName.Equals(cSchema) &&
					viewName.Name.Equals(cName)) {

					ViewInfo viewInfo;
					if (!viewCache.TryGetValue(row, out viewInfo)) { 
						var blob = (SqlBinary)viewTable.GetValue(row, 3).Value;
						using (var session = new SystemSession(Transaction, SystemSchema.Name)) {
							using (var context = session.CreateQuery()) {

								viewInfo = ViewInfo.Deserialize(blob.GetInput());
							}
						}

						viewCache[row] = viewInfo;

					}

					return new View(viewInfo);
				}

			}

			return null;
		}

		public bool ViewExists(ObjectName viewName) {
			return FindViewEntry(viewName).RowCount > 0;
		}

		public bool DropView(ObjectName viewName) {
			var table = Transaction.GetMutableTable(SystemSchema.ViewTableName);

			var t = FindViewEntry(viewName);

			if (t.RowCount == 0)
				return false;

			table.Delete(t);

			// Notify that this database object has been successfully dropped.
			Transaction.Registry.RegisterEvent(new ObjectDroppedEvent(DbObjectType.View, viewName));

			viewTableChanged = true;

			return true;
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
					ViewInfo viewInfo;
					if (!viewCache.TryGetValue(row, out viewInfo)) {
						var binary = (ISqlBinary)table.GetValue(row, 3).Value;

						using (var session = new SystemSession(Transaction, SystemSchema.Name)) {
							using (var context = session.CreateQuery()) {
								viewInfo = ViewInfo.Deserialize(binary.GetInput());
							}
						}

						viewCache[row] = viewInfo;
					}

					return new View(viewInfo);
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
