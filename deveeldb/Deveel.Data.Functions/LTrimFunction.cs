//  
//  LTrimFunction.cs
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
	internal sealed class LTrimFunction : Function {

		public LTrimFunction(Expression[] parameters)
			: base("ltrim", parameters) {

			if (ParameterCount != 1) {
				throw new Exception(
					"ltrim function may only have 1 parameter.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
		                                 IQueryContext context) {
			TObject ob = this[0].Evaluate(group, resolver, context);
			if (ob.IsNull) {
				return ob;
			}
			String str = ob.Object.ToString();

			// Do the trim,
			// Trim from the start.
			int scan = 0;
			while (scan < str.Length &&
			       str.IndexOf(' ', scan) == scan) {
				scan += 1;
			}
			str = str.Substring(System.Math.Min(scan, str.Length));

			return TObject.GetString(str);
		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			return TType.StringType;
		}

	}
}