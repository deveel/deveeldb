//  
//  SignFunction.cs
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

using Deveel.Math;

namespace Deveel.Data.Functions {
	internal sealed class SignFunction : Function {

		public SignFunction(Expression[] parameters)
			: base("sign", parameters) {

			if (ParameterCount != 1) {
				throw new Exception("Sign function must have one argument.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			TObject ob = this[0].Evaluate(group, resolver, context);
			if (ob.IsNull) {
				return ob;
			}
			BigNumber num = ob.ToBigNumber();
			return TObject.GetInt4(num.Signum());
		}

	}
}