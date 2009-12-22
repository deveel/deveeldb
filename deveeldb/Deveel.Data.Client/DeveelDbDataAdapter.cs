//  
//  DeveelDbDataAdapter.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;
using System.Data;
using System.Data.Common;

namespace Deveel.Data.Client {
	public sealed class DeveelDbDataAdapter : DbDataAdapter {
		public DeveelDbDataAdapter() {
		}

		public DeveelDbDataAdapter(DeveelDbCommand selectCommand)
			: this() {
			SelectCommand = selectCommand;
		}

		public DeveelDbDataAdapter(string selectCommandText, DeveelDbConnection connection)
			: this() {
			SelectCommand = new DeveelDbCommand(selectCommandText, connection);
		}

		public DeveelDbDataAdapter(string selectCommandText, string connectionString)
			: this() {
			SelectCommand = new DeveelDbCommand(selectCommandText, new DeveelDbConnection(connectionString));
		}

		private int updateBatchSize;
		private ArrayList batch;

		public event DeveelDbRowUpdatingEventHandler RowUpdating;
		public event DeveelDbRowUpdatedEventHandler RowUpdated;

		public override int UpdateBatchSize {
			get { return updateBatchSize; }
			set { updateBatchSize = value; }
		}

		public new DeveelDbCommand SelectCommand {
			get { return (DeveelDbCommand) base.SelectCommand; }
			set { base.SelectCommand = value; }
		}

		public new DeveelDbCommand UpdateCommand {
			get { return (DeveelDbCommand) base.UpdateCommand; }
			set { base.UpdateCommand = value; }
		}

		public new DeveelDbCommand DeleteCommand {
			get { return (DeveelDbCommand) base.DeleteCommand; }
			set { base.DeleteCommand = value; }
		}

		public new DeveelDbCommand InsertCommand {
			get { return (DeveelDbCommand) base.InsertCommand; }
			set { base.InsertCommand = value; }
		}

		protected override void InitializeBatching() {
			batch = new ArrayList();
		}

		protected override int AddToBatch(IDbCommand command) {
			command.Prepare();
			//TODO: further checks?
			return batch.Add(command);
		}

		protected override int ExecuteBatch() {
			int count = 0;

			for (int i = 0; i < batch.Count; i++) {
				DeveelDbCommand command = (DeveelDbCommand) batch[i];
				count += command.ExecuteNonQuery();
			}

			return count;
		}

		protected override void ClearBatch() {
			if (batch.Count > 0)
				batch.Clear();
		}

		protected override void TerminateBatching() {
			ClearBatch();
			batch = null;
		}

		protected override IDataParameter GetBatchedParameter(int commandIdentifier, int parameterIndex) {
			DeveelDbCommand command = (DeveelDbCommand) batch[commandIdentifier];
			return (command == null ? null : command.Parameters[parameterIndex]);
		}

		protected override RowUpdatedEventArgs CreateRowUpdatedEvent(System.Data.DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping) {
			return new DeveelDbRowUpdatedEventArgs(dataRow, (DeveelDbCommand) command, statementType, tableMapping);
		}

		protected override RowUpdatingEventArgs CreateRowUpdatingEvent(System.Data.DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping) {
			return new DeveelDbRowUpdatingEventArgs(dataRow, (DeveelDbCommand) command, statementType, tableMapping);
		}

		protected override void OnRowUpdating(RowUpdatingEventArgs value) {
			if (RowUpdating != null)
				RowUpdating(this, (DeveelDbRowUpdatingEventArgs) value);
		}

		protected override void OnRowUpdated(RowUpdatedEventArgs value) {
			if (RowUpdated != null)
				RowUpdated(this, (DeveelDbRowUpdatedEventArgs) value);
		}
	}
}