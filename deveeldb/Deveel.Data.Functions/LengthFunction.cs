// 
//  LengthFunction.cs
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
	internal sealed class LengthFunction : Function {

		public LengthFunction(Expression[] parameters)
			: base("length", parameters) {

			if (ParameterCount != 1) {
				throw new Exception("Length function must have one argument.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
		                                 IQueryContext context) {
			TObject ob = this[0].Evaluate(group, resolver, context);
			if (ob.IsNull) {
				return ob;
			}
			if (ob.TType is TBinaryType) {
				IBlobAccessor blob = (IBlobAccessor)ob.Object;
				return TObject.GetInt4(blob.Length);
			}
			if (ob.TType is TStringType) {
				IStringAccessor str = (IStringAccessor)ob.Object;
				return TObject.GetInt4(str.Length);
			}
			return TObject.GetInt4(ob.Object.ToString().Length);
		}

	}
}