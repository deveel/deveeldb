//  
//  DateFormatFunction.cs
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

namespace Deveel.Data.Functions {
	// A function that formats an input DateTime object to the format
	// given using the string format.
	sealed class DateFormatFunction : Function {
		public DateFormatFunction(Expression[] parameters)
			: base("dateformat", parameters) {

			if (ParameterCount != 2) {
				throw new Exception("'dateformat' function must have exactly two parameters.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			TObject datein = this[0].Evaluate(group, resolver, context);
			TObject format = this[1].Evaluate(group, resolver, context);
			// If expression resolves to 'null' then return null
			if (datein.IsNull) {
				return datein;
			}

			DateTime d;
			if (!(datein.TType is TDateType)) {
				throw new Exception("Date to format must be DATE, TIME or TIMESTAMP");
			} else {
				d = (DateTime)datein.Object;
			}

			String format_string = format.ToString();
			return TObject.GetString(d.ToString(format_string));
		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			return TType.StringType;
		}

	}
}