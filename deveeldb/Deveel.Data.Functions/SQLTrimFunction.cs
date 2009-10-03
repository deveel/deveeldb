// 
//  SQLTrimFunction.cs
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

namespace Deveel.Data.Functions {
	internal sealed class SQLTrimFunction : Function {

		public SQLTrimFunction(Expression[] parameters)
			: base("sql_trim", parameters) {

			//      Console.Out.WriteLine(parameterCount());
			if (ParameterCount != 3) {
				throw new Exception(
					"SQL Trim function must have three parameters.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			// The type of trim (leading, both, trailing)
			TObject ttype = this[0].Evaluate(group, resolver, context);
			// Characters to trim
			TObject cob = this[1].Evaluate(group, resolver, context);
			if (cob.IsNull) {
				return cob;
			} else if (ttype.IsNull) {
				return TObject.GetString((StringObject)null);
			}
			String characters = cob.Object.ToString();
			String ttype_str = ttype.Object.ToString();
			// The content to trim.
			TObject ob = this[2].Evaluate(group, resolver, context);
			if (ob.IsNull) {
				return ob;
			}
			String str = ob.Object.ToString();

			int skip = characters.Length;
			// Do the trim,
			if (ttype_str.Equals("leading") || ttype_str.Equals("both")) {
				// Trim from the start.
				int scan = 0;
				while (scan < str.Length &&
				       str.IndexOf(characters, scan) == scan) {
					scan += skip;
				}
				str = str.Substring(System.Math.Min(scan, str.Length));
			}
			if (ttype_str.Equals("trailing") || ttype_str.Equals("both")) {
				// Trim from the end.
				int scan = str.Length - 1;
				int i = str.LastIndexOf(characters, scan);
				while (scan >= 0 && i != -1 && i == scan - skip + 1) {
					scan -= skip;
					i = str.LastIndexOf(characters, scan);
				}
				str = str.Substring(0, System.Math.Max(0, scan + 1));
			}

			return TObject.GetString(str);
		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			return TType.StringType;
		}

	}
}