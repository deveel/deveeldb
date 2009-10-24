//  
//  DeveelDbProcedureTrigger.cs
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