using System;

using Deveel.Data.Index;
using Deveel.Data.Sql;
using Deveel.Data.Store;

namespace Deveel.Data.DbSystem {
	class TableSource {
		internal TableSource(TableSourceComposite composite, IStoreSystem storeSystem, int tableId, string tableName) {
			if (composite == null)
				throw new ArgumentNullException("composite");

			Composite = composite;
			StoreSystem = storeSystem;
			TableId = tableId;
			TableName = tableName;
		}

		public IIndexSet CreateIndexSet() {
			throw new NotImplementedException();
		}

		public TableSourceComposite Composite { get; private set; }

		private IStoreSystem StoreSystem { get; set; }

		public int TableId { get; private set; }

		public string TableName { get; private set; }

		public bool IsRootLocked { get; private set; }

		public TableInfo TableInfo { get; private set; }

		public bool CanCompact { get; private set; }

		public bool IsReadOnly { get; private set; }

		public bool Exists() {
			return StoreSystem.StoreExists(TableName);
		}

		public void Close(bool dropPending) {
			throw new NotImplementedException();
		}

		public bool Drop() {
			throw new NotImplementedException();
		}

		public void Open() {
			throw new NotImplementedException();
		}

		public void Create(TableInfo tableInfo) {
			throw new NotImplementedException();
		}

		public long GetNextUniqueId() {
			throw new NotImplementedException();
		}

		public void SetUniqueId(long uniqueId) {
			throw new NotImplementedException();
		}

		public ITable CreateTableAtCommit(TableManager tableManager) {
			throw new NotImplementedException();
		}

		public void WriteRecordType(int rowNumber, TableRecordState state) {
			throw new NotImplementedException();
		}

		public int AddRow(Row dataRow) {
			throw new NotImplementedException();
		}

		public void BuildIndexes() {
			throw new NotImplementedException();
		}

		public void CopyFrom(int tableId, TableSource tableSource, IIndexSet indexSet) {
			throw new NotImplementedException();
		}

		public void AddLock() {
			throw new NotImplementedException();
		}

		public void RemoveLock() {
			throw new NotImplementedException();
		}
	}
}
