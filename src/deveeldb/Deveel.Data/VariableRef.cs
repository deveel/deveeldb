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
	/// The object that references a variable declared within
	/// an opened session.
	/// </summary>
	[Serializable]
	public sealed class VariableRef {
		/// <summary>
		/// Constructs the reference to the given variable name.
		/// </summary>
		/// <param name="varName">The name of the variable to reference.</param>
		public VariableRef(string varName) {
			if (varName == null)
				throw new ArgumentNullException("varName");

			this.varName = varName;
		}

		/// <summary>
		/// The name of the variable to reference.
		/// </summary>
		private readonly string varName;

		/// <summary>
		/// Gets the name of the variable referenced.
		/// </summary>
		public string Variable {
			get { return varName; }
		}

		public override bool Equals(object obj) {
			VariableRef varRef = obj as VariableRef;
			return varRef == null ? false : varName.Equals(varRef.varName);
		}

		public override int GetHashCode() {
			return varName.GetHashCode();
		}

		public override string ToString() {
			return ":" + varName;
		}
	}
}