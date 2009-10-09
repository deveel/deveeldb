//  
//  ViewDataConvert.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
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
using System.Text;

using Deveel.Data.Client;

namespace Deveel.Data.Functions {
	// Used to convert view data in the system view table to forms that are
	// human understandable.  Useful function for debugging or inspecting views.
	internal sealed class ViewDataConvert : Function {

		public ViewDataConvert(Expression[] parameters)
			: base("i_view_data", parameters) {

			if (ParameterCount != 2) {
				throw new Exception(
					"i_sql_type function must have two arguments.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			// Get the parameters.  The first is a string describing the operation.
			// The second is the binary data to process and output the information
			// for.
			TObject commandObj = this[0].Evaluate(group, resolver, context);
			TObject data = this[1].Evaluate(group, resolver, context);

			String command_str = commandObj.Object.ToString();
			ByteLongObject blob = (ByteLongObject)data.Object;

			if (String.Compare(command_str, "referenced tables", true) == 0) {
				ViewDef view_def = ViewDef.DeserializeFromBlob(blob);
				IQueryPlanNode node = view_def.QueryPlanNode;
				ArrayList touched_tables = node.DiscoverTableNames(new ArrayList());
				StringBuilder buf = new StringBuilder();
				int sz = touched_tables.Count;
				for (int i = 0; i < sz; ++i) {
					buf.Append(touched_tables[i]);
					if (i < sz - 1) {
						buf.Append(", ");
					}
				}
				return TObject.GetString(buf.ToString());
			} else if (String.Compare(command_str, "plan dump", true) == 0) {
				ViewDef view_def = ViewDef.DeserializeFromBlob(blob);
				IQueryPlanNode node = view_def.QueryPlanNode;
				StringBuilder buf = new StringBuilder();
				node.DebugString(0, buf);
				return TObject.GetString(buf.ToString());
			} else if (String.Compare(command_str, "query string", true) == 0) {
				SqlCommand command = SqlCommand.DeserializeFromBlob(blob);
				return TObject.GetString(command.ToString());
			}

			return TObject.Null;

		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			return TType.StringType;
		}

	}
}