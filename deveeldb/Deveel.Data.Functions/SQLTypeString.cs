// 
//  SQLTypeString.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Text;

namespace Deveel.Data.Functions {
	// Used to form an SQL type string that describes the SQL type and any
	// size/scale information together with it.
	internal sealed class SQLTypeString : Function {
		public SQLTypeString(Expression[] parameters)
			: base("i_sql_type", parameters) {

			if (ParameterCount != 3) {
				throw new Exception(
					"i_sql_type function must have three arguments.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			// The parameter should be a variable reference that is resolved
			TObject type_string = this[0].Evaluate(group, resolver, context);
			TObject type_size = this[1].Evaluate(group, resolver, context);
			TObject type_scale = this[2].Evaluate(group, resolver, context);

			StringBuilder result_str = new StringBuilder();
			result_str.Append(type_string.ToString());
			long size = -1;
			long scale = -1;
			if (!type_size.IsNull) {
				size = type_size.ToBigNumber().ToInt64();
			}
			if (!type_scale.IsNull) {
				scale = type_scale.ToBigNumber().ToInt64();
			}

			if (size != -1) {
				result_str.Append('(');
				result_str.Append(size);
				if (scale != -1) {
					result_str.Append(',');
					result_str.Append(scale);
				}
				result_str.Append(')');
			}

			return TObject.GetString(result_str.ToString());
		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			return TType.StringType;
		}

	}
}