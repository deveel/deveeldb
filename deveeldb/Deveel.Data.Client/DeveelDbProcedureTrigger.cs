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
using System.Text;

namespace Deveel.Data.Client {
	public class DeveelDbProcedureTrigger : DeveelDbTrigger {
		public DeveelDbProcedureTrigger(DeveelDbConnection connection, string triggerName, string objectName, string procedureName) 
			: base(connection, triggerName, objectName) {
			this.procedureName = procedureName;
		}

		private readonly string procedureName;

		public string ProcedureName {
			get { return procedureName; }
		}

		internal override DeveelDbCommand GetCreateStatement() {
			ParameterStyle paramStyle = Connection.Settings.ParameterStyle;

			StringBuilder sb = new StringBuilder();
			sb.Append("CREATE TRIGGER ");
			sb.Append(Name);
			sb.Append(" ");
			sb.Append(FormatEventType(EventType));
			sb.Append(" ON ");
			sb.Append(ObjectName);
			sb.Append(" FOR EACH ROW EXECUTE PROCEDURE ");
			sb.Append(procedureName);

			//TODO: support parameters...

			DeveelDbCommand command = Connection.CreateCommand(sb.ToString());
			return command;
		}

		internal override DeveelDbCommand GetDropStatement() {
			StringBuilder sb = new StringBuilder();
			sb.Append("DROP TRIGGER ");
			sb.Append(Name);

			return Connection.CreateCommand(sb.ToString());
		}
	}
}