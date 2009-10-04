//  
//  ForeignRuleConvert.cs
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
	// Used in the 'getxxxKeys' methods in DatabaseMetaData to convert the
	// update delete rule of a foreign key to the JDBC short enum.
	internal sealed class ForeignRuleConvert : Function {

		public ForeignRuleConvert(Expression[] parameters)
			: base("i_frule_convert", parameters) {

			if (ParameterCount != 1) {
				throw new Exception(
					"i_frule_convert function must have one argument.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			// The parameter should be a variable reference that is resolved
			TObject ob = this[0].Evaluate(group, resolver, context);
			String str = null;
			if (!ob.IsNull) {
				str = ob.Object.ToString();
			}
			int v;
			if (str == null || str.Equals("") || str.Equals("NO ACTION")) {
				v = ImportedKey.NoAction;
			} else if (str.Equals("CASCADE")) {
				v = ImportedKey.Cascade;
			} else if (str.Equals("SET NULL")) {
				v = ImportedKey.SetNull;
			} else if (str.Equals("SET DEFAULT")) {
				v = ImportedKey.SetDefault;
			} else if (str.Equals("RESTRICT")) {
				v = ImportedKey.Restrict;
			} else {
				throw new ApplicationException("Unrecognised foreign key rule: " + str);
			}
			// Return the correct enumeration
			return TObject.GetInt4(v);
		}

	}
}