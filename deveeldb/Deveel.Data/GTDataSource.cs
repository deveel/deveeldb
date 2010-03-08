// 
//  Copyright 2010  Deveel
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

using System;

namespace Deveel.Data {
	/// <summary>
	/// A base class for a dynamically generated data source (GT == Generated Table).
	/// </summary>
	/// <remarks>
	/// While this inherits <see cref="IMutableTableDataSource"/> 
	/// (so we can make a <see cref="DataTable"/> out of it) a derived class 
	/// may not be mutable.
	/// For example, an implementation of this class may produce a list of a 
	/// columns in all tables. 
	/// It is not suggested let a user to change this information unless he 
	/// runs a DML command.
	/// </remarks>
	internal abstract class GTDataSource : IMutableTableDataSource {
		/// <summary>
		/// The TransactionSystem object for this table.
		/// </summary>
		private readonly TransactionSystem system;

		private bool disposed;

		protected GTDataSource(TransactionSystem system) {
			this.system = system;
		}

		#region IMutableTableDataSource Members

		void IDisposable.Dispose() {
			GC.SuppressFinalize(this);
			Dispose(true);
		}

		// ---------- Implemented from ITableDataSource ----------

		/// <inheritdoc/>
		public TransactionSystem System {
			get { return system; }
		}

		/// <inheritdoc/>
		public abstract DataTableDef DataTableDef { get; }

		/// <inheritdoc/>
		public abstract int RowCount { get; }

		/// <inheritdoc/>
		public IRowEnumerator GetRowEnumerator() {
			return new SimpleRowEnumerator(RowCount);
		}

		/// <inheritdoc/>
		public virtual SelectableScheme GetColumnScheme(int column) {
			return new BlindSearch(this, column);
		}

		/// <inheritdoc/>
		public abstract TObject GetCellContents(int column, int row);

		// ---------- Implemented from IMutableTableDataSource ----------

		/// <inheritdoc/>
		public virtual int AddRow(DataRow dataRow) {
			throw new Exception("Functionality not available.");
		}

		/// <inheritdoc/>
		public virtual void RemoveRow(int row_index) {
			throw new Exception("Functionality not available.");
		}

		/// <inheritdoc/>
		public virtual int UpdateRow(int row_index, DataRow dataRow) {
			throw new Exception("Functionality not available.");
		}

		/// <inheritdoc/>
		public virtual MasterTableJournal Journal {
			get { throw new Exception("Functionality not available."); }
		}

		/// <inheritdoc/>
		public virtual void FlushIndexChanges() {
			throw new Exception("Functionality not available.");
		}

		/// <inheritdoc/>
		public virtual void ConstraintIntegrityCheck() {
			throw new Exception("Functionality not available.");
		}

		/// <inheritdoc/>
		public virtual void AddRootLock() {
			// No need to Lock roots
		}

		/// <inheritdoc/>
		public virtual void RemoveRootLock() {
			// No need to Lock roots
		}

		#endregion

		~GTDataSource() {
			Dispose(false);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposed)
				return;

			if (disposing) {
				Dispose();
			}

			disposed = true;
		}

		/// <summary>
		/// Converts the given value to a value coresponding for the column
		/// at the given index of the table.
		/// </summary>
		/// <param name="column">Index of the column in the table.</param>
		/// <param name="ob">Value to convert.</param>
		/// <remarks>
		/// <paramref name="ob"/> must be of a compatible type to store in 
		/// the column defined.
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="TObject"/> representation of the given value 
		/// compatible with the column <see cref="TType"/> of the column at 
		/// the given <paramref name="column"/>.
		/// </returns>
		protected TObject GetColumnValue(int column, Object ob) {
			TType type = DataTableDef[column].TType;
			return new TObject(type, ob);
		}

		/// <inheritdoc/>
		protected virtual void Dispose() {
		}

		// Convenience methods for constructing a DataTableDef for the dynamically
		// generated table.

		protected static DataTableColumnDef GetStringColumn(String name) {
			DataTableColumnDef column = new DataTableColumnDef();
			column.Name = name;
			column.IsNotNull = true;
			column.SqlType = SqlType.VarChar;
			column.Size = Int32.MaxValue;
			column.Scale = -1;
			column.IndexScheme = "BlindSearch";
			column.InitTTypeInfo();
			return column;
		}

		protected static DataTableColumnDef GetBooleanColumn(String name) {
			DataTableColumnDef column = new DataTableColumnDef();
			column.Name = name;
			column.IsNotNull = true;
			column.SqlType = SqlType.Bit;
			column.Size = -1;
			column.Scale = -1;
			column.IndexScheme = "BlindSearch";
			column.InitTTypeInfo();
			return column;
		}

		protected static DataTableColumnDef GetNumericColumn(String name) {
			DataTableColumnDef column = new DataTableColumnDef();
			column.Name = name;
			column.IsNotNull = true;
			column.SqlType = SqlType.Numeric;
			column.Size = -1;
			column.Scale = -1;
			column.IndexScheme = "BlindSearch";
			column.InitTTypeInfo();
			return column;
		}

		protected static DataTableColumnDef GetDateColumn(String name) {
			DataTableColumnDef column = new DataTableColumnDef();
			column.Name = name;
			column.IsNotNull = true;
			column.SqlType = SqlType.TimeStamp;
			column.Size = -1;
			column.Scale = -1;
			column.IndexScheme = "BlindSearch";
			column.InitTTypeInfo();
			return column;
		}
	}
}