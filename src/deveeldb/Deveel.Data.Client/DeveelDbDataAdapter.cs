// 
//  Copyright 2010-2014 Deveel
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