// 
//  Copyright 2010  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

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
		/// The <see cref="VariableName"/> reference itself.
		/// </summary>
		private VariableName variable;

		/// <summary>
		/// The number of sub-query branches back that the reference for this
		/// variable can be found.
		/// </summary>
		private readonly int query_level_offset;

		/// <summary>
		/// The temporary value this variable has been set to evaluate to.
		/// </summary>
		private TObject eval_result;


		public CorrelatedVariable(VariableName variable, int level_offset) {
			this.variable = variable;
			this.query_level_offset = level_offset;
		}

		/// <summary>
		/// Returns the wrapped Variable.
		/// </summary>
		public VariableName VariableName {
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
			VariableName v = VariableName;
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
			v.variable = (VariableName)variable.Clone();
			return v;
		}

		/// <inheritdoc/>
		public override String ToString() {
			return "CORRELATED: " + VariableName + " = " + EvalResult;
		}

	}
}