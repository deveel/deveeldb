//
//  This file is part of DeveelDB.
//
//    DeveelDB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of the 
//    License, or (at your option) any later version.
//
//    DeveelDB is distributed in the hope that it will be useful,
//    but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public 
//    License along with DeveelDB.  If not, see <http://www.gnu.org/licenses/>.
//
//  Authors:
//    Antonello Provenzano <antonello@deveel.com>
//    Tobias Downer <toby@mckoi.com>
//

using System;

namespace Deveel.Data {
	/// <summary>
	/// A wrapper for a variable in a sub-query that references a 
	/// column outside of the current query.
	/// </summary>
	/// <remarks>
	/// A correlated variable differs from a regular variable because 
	/// its value is constant in an operation, but may vary over future 
	/// iterations of the operation.
	/// <para>
	/// This object is NOT immutable.
	/// </para>
	/// </remarks>
	[Serializable]
	public class CorrelatedVariable : ICloneable {
		/// <summary>
		/// The <see cref="Variable"/> reference itself.
		/// </summary>
		private Variable variable;

		/// <summary>
		/// The number of sub-query branches back that the reference for this
		/// variable can be found.
		/// </summary>
		private readonly int query_level_offset;

		/// <summary>
		/// The temporary value this variable has been set to evaluate to.
		/// </summary>
		private TObject eval_result;


		public CorrelatedVariable(Variable variable, int level_offset) {
			this.variable = variable;
			this.query_level_offset = level_offset;
		}

		/// <summary>
		/// Returns the wrapped Variable.
		/// </summary>
		public Variable Variable {
			get { return variable; }
		}

		/// <summary>
		/// Returns the number of sub-query branches back that the reference 
		/// for this variable can be found.
		/// </summary>
		/// <remarks>
		/// For example, if the correlated variable references the direct 
		/// descendant this will return 1.
		/// </remarks>
		public int QueryLevelOffset {
			get { return query_level_offset; }
		}

		/// <summary>
		/// Given a <see cref="IVariableResolver"/> this will set the value 
		/// of the correlated variable.
		/// </summary>
		/// <param name="resolver"></param>
		public void SetFromResolver(IVariableResolver resolver) {
			Variable v = Variable;
			EvalResult = resolver.Resolve(v);
		}

		/// <summary>
		/// Gets or sets the value this correlated variable evaluates to.
		/// </summary>
		public TObject EvalResult {
			get { return eval_result; }
			set { eval_result = value; }
		}

		/// <summary>
		/// Returns the TType this correlated variable evaluates to.
		/// </summary>
		public TType ReturnTType {
			get { return eval_result.TType; }
		}

		/// <inheritdoc/>
		public object Clone() {
			CorrelatedVariable v = (CorrelatedVariable)MemberwiseClone();
			v.variable = (Variable)variable.Clone();
			return v;
		}

		/// <inheritdoc/>
		public override String ToString() {
			return "CORRELATED: " + Variable + " = " + EvalResult;
		}

	}
}