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

using Deveel.Data.Index;
using Deveel.Data.Types;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// A base class for a dynamically generated data source (GT == Generated Table).
	/// </summary>
	/// <remarks>
	/// It is not suggested let a user to change this information unless he 
	/// runs a DML command.
	/// </remarks>
	abstract class GTDataSource : ITableDataSource {
		/// <summary>
		/// The TransactionSystem object for this table.
		/// </summary>
		private readonly ISystemContext context;

		private bool disposed;

		protected GTDataSource(ISystemContext context) {
			this.context = context;
		}


		~GTDataSource() {
			Dispose(false);
		}

		public void Dispose() {
			if (!disposed) {
				Dispose(true);
				GC.SuppressFinalize(this);
				disposed = true;
			}
		}

		// ---------- Implemented from ITableDataSource ----------

		/// <inheritdoc/>
		public ISystemContext Context {
			get { return context; }
		}

		/// <inheritdoc/>
		public abstract DataTableInfo TableInfo { get; }

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
		public abstract TObject GetCell(int column, int row);


		protected virtual void Dispose(bool disposing) {
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
		protected TObject GetColumnValue(int column, object ob) {
			TType type = TableInfo[column].TType;
			return new TObject(type, ob);
		}
	}
}