// 
//  Copyright 2010-2014 Deveel
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

using Deveel.Data.Types;

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
	public class CorrelatedVariable {
		/// <summary>
		/// The <see cref="VariableName"/> reference itself.
		/// </summary>
		private ObjectName variable;

		public CorrelatedVariable(ObjectName variable, int levelOffset) {
			this.variable = variable;
			QueryLevelOffset = levelOffset;
		}

		/// <summary>
		/// Returns the wrapped Variable.
		/// </summary>
		public ObjectName VariableName {
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
		public int QueryLevelOffset { get; private set; }

		/// <summary>
		/// Given a <see cref="IVariableResolver"/> this will set the value 
		/// of the correlated variable.
		/// </summary>
		/// <param name="resolver"></param>
		public void SetFromResolver(IVariableResolver resolver) {
			var v = VariableName;
			EvalResult = resolver.Resolve(v);
		}

		/// <summary>
		/// Gets or sets the value this correlated variable evaluates to.
		/// </summary>
		public DataObject EvalResult { get; set; }

		/// <summary>
		/// Returns the TType this correlated variable evaluates to.
		/// </summary>
		public DataType ReturnType {
			get { return EvalResult.Type; }
		}

		/// <inheritdoc/>
		public override String ToString() {
			return String.Format("CORRELATED: {0} = {1}",  VariableName, EvalResult);
		}
	}
}