//  
//  DbProcedureTrigger.cs
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
	public class DbProcedureTrigger : DbTrigger {
		public DbProcedureTrigger(DbConnection connection, string triggerName, string objectName, string procedureName) 
			: base(connection, triggerName, objectName) {
			this.procedureName = procedureName;
		}

		private readonly string procedureName;
		private TriggerEventMoment moment = TriggerEventMoment.After;

		public string ProcedureName {
			get { return procedureName; }
		}

		public TriggerEventMoment EventMoment {
			get { return moment; }
			set {
				CheckExisting();
				moment = value;
			}
		}

		internal override DbCommand GetCreateStatement() {
			ParameterStyle paramStyle = Connection.ConnectionString.ParameterStyle;

			StringBuilder sb = new StringBuilder();
			sb.Append("CREATE TRIGGER ");
			if (paramStyle == ParameterStyle.Marker) {
				sb.Append("?");
			} else {
				sb.Append("@TriggerName");
			}
			if (moment == TriggerEventMoment.Before) {
				sb.Append(" BEFORE ");
			} else {
				sb.Append(" AFTER ");
			}
			sb.Append(" ");
			sb.Append(FormatEventType(EventTypes));
			sb.Append(" ON ");
			if (paramStyle == ParameterStyle.Marker) {
				sb.Append("?");
			} else {
				sb.Append("@TableName");
			}

			sb.Append(" FOR EACH ROW EXECUTE PROCEDURE ");

			if (paramStyle == ParameterStyle.Marker) {
				sb.Append("?");
			} else {
				sb.Append("@ProcedureName");
			}

			//TODO: support parameters...

			sb.Append(";");

			DbCommand command = Connection.CreateCommand(sb.ToString());
			if (paramStyle == ParameterStyle.Marker) {
				command.Parameters.Add(Name);
				command.Parameters.Add(ObjectName);
				command.Parameters.Add(procedureName);
			} else {
				command.Parameters.Add("@TriggerName", Name);
				command.Parameters.Add("@TableName", ObjectName);
				command.Parameters.Add("@ProcedureName", procedureName);
			}

			command.Prepare();

			return command;
		}

		internal override DbCommand GetDropStatement() {
			ParameterStyle paramStyle = Connection.ConnectionString.ParameterStyle;

			StringBuilder sb = new StringBuilder();
			sb.Append("DROP CALLBACK TRIGGER ");
			if (paramStyle == ParameterStyle.Marker)
				sb.Append("?");
			else
				sb.Append("@TriggerName");
			sb.Append(";");

			DbCommand command = Connection.CreateCommand(sb.ToString());

			if (paramStyle == ParameterStyle.Marker)
				command.Parameters.Add(Name);
			else
				command.Parameters.Add("@TriggerName", Name);

			command.Prepare();

			return command;
		}
	}
}