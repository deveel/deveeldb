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

using Deveel.Data.Client;

namespace Deveel.Data {
	/// <summary>
	/// Represents a constant value that is to be lately binded to a constant 
	/// value in an <see cref="Expression"/>.
	/// </summary>
	/// <remarks>
	/// This is used when we have "?" style prepared statement values.
	/// This object is used as a marker in the elements of a expression.
	/// </remarks>
	[Serializable]
	public class ParameterSubstitution : IExpressionElement {
		/// <summary>
		/// The zero-based numerical number of this parameter substitution.
		/// </summary>
		private readonly int parameter_id;

		/// <summary>
		/// The name of the parameter for the substitution.
		/// </summary>
		private readonly string name;

		///<summary>
		/// Constructs a <see cref="ParameterSubstitution"/> to employ in a command
		/// which uses a <see cref="ParameterStyle.Marker"/> style.
		///</summary>
		///<param name="parameter_id">The unique identifier of the parameter.</param>
		public ParameterSubstitution(int parameter_id) {
			this.parameter_id = parameter_id;
		}

		/// <summary>
		/// Constructs a <see cref="ParameterSubstitution"/> to employ in a command
		/// which uses a <see cref="ParameterStyle.Named"/> style.
		/// </summary>
		/// <param name="name">The name of the parameter.</param>
		public ParameterSubstitution(string name) {
			this.name = name;
		}

		/// <summary>
		/// Returns the number of this parameter id.
		/// </summary>
		public int Id {
			get { return parameter_id; }
		}

		/// <summary>
		/// Gets the name of the parameter to substitue.
		/// </summary>
		public string Name {
			get { return name; }
		}

		/// <inheritdoc/>
		public override bool Equals(Object ob) {
			ParameterSubstitution sub = (ParameterSubstitution)ob;
			return (name == null || String.Compare(name, sub.name, false) != 0) && parameter_id == sub.parameter_id;
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			if (name != null)
				return name.GetHashCode();
			return parameter_id.GetHashCode();
		}
	}
}