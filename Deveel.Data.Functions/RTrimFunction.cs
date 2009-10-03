// 
//  RTrimFunction.cs
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
	internal sealed class RTrimFunction : Function {

		public RTrimFunction(Expression[] parameters)
			: base("rtrim", parameters) {

			if (ParameterCount != 1) {
				throw new Exception("rtrim function may only have 1 parameter.");
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
			// Trim from the end.
			int scan = str.Length - 1;
			int i = str.LastIndexOf(" ", scan);
			while (scan >= 0 && i != -1 && i == scan - 2) {
				scan -= 1;
				i = str.LastIndexOf(" ", scan);
			}
			str = str.Substring(0, System.Math.Max(0, scan + 1));

			return TObject.GetString(str);
		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			return TType.StringType;
		}

	}
}