//  
//  SoundexFunction.cs
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

using Deveel.Data.Text;

namespace Deveel.Data.Functions {
	public sealed class SoundexFunction : Function {
		public SoundexFunction(Expression[] parameters) 
			: base("soundex", parameters) {
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			TObject obj = this[0].Evaluate(group, resolver, context);

			if (!(obj.TType is TStringType))
				obj = obj.CastTo(TType.StringType);

			return TObject.GetString(Soundex.UsEnglish.Compute(obj.ToStringValue()));
		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			return TType.StringType;
		}
	}
}