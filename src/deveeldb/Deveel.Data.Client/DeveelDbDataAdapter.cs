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
using System.Data.Common;

namespace Deveel.Data.Client {
	public sealed class DeveelDbDataAdapter : DbDataAdapter {
		public DeveelDbDataAdapter() {
		}

		public DeveelDbDataAdapter(DeveelDbCommand selectCommand) {
			SelectCommand = selectCommand;
		}

		public DeveelDbDataAdapter(string commandText) {
			SelectCommand = new DeveelDbCommand(commandText);
		}

		public DeveelDbDataAdapter(DeveelDbConnection connection, string commandText) {
			SelectCommand = new DeveelDbCommand(connection, commandText);
		}

		public DeveelDbDataAdapter(string connectionString, string commandText) {
			var connection = new DeveelDbConnection(connectionString);
			SelectCommand = new DeveelDbCommand(connection, commandText);
		}

		private readonly object updatingEventKey = new object();
		private readonly object updatedEventKey = new object();

		public event EventHandler<RowUpdatingEventArgs> RowUpdating {
			add { Events.AddHandler(updatingEventKey, value); }
			remove { Events.RemoveHandler(updatingEventKey, value); }
		}

		public event EventHandler<RowUpdatedEventArgs> RowUpdated {
			add { Events.AddHandler(updatedEventKey, value); }
			remove { Events.RemoveHandler(updatedEventKey, value); }
		}

		public new DeveelDbCommand SelectCommand {
			get { return (DeveelDbCommand) base.SelectCommand; }
			set { base.SelectCommand = value; }
		}

		public new DeveelDbCommand DeleteCommand {
			get { return (DeveelDbCommand) base.DeleteCommand; }
			set { base.DeleteCommand = value; }
		}

		public new DeveelDbCommand UpdateCommand {
			get { return (DeveelDbCommand) base.UpdateCommand; }
			set { base.UpdateCommand = value; }
		}

		public new DeveelDbCommand InsertCommand {
			get { return (DeveelDbCommand) base.InsertCommand; }
			set { base.InsertCommand = value; }
		}

		protected override void OnRowUpdated(RowUpdatedEventArgs value) {
			var handler = Events[updatedEventKey] as EventHandler<RowUpdatedEventArgs>;
			if (handler != null)
				handler(this, value);
		}

		protected override void OnRowUpdating(RowUpdatingEventArgs value) {
			var handler = Events[updatingEventKey] as EventHandler<RowUpdatingEventArgs>;
			if (handler != null)
				handler(this, value);
		}
	}
}
