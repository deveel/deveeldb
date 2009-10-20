//  
//  SinHFunction.cs
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

namespace Deveel.Data.Functions {
	[Serializable]
	public sealed class SinHFunction : Function {
		public SinHFunction(Expression[] parameters)
			: base("sinh", parameters) {
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			TObject ob = this[0].Evaluate(group, resolver, context);
			if (ob.IsNull)
				return ob;

			if (ob.TType is TNumericType)
				ob = ob.CastTo(TType.NumericType);

			return TObject.GetBigNumber(Math.BigNumber.fromDouble(System.Math.Sinh(ob.ToBigNumber().ToDouble())));
		}
	}
}