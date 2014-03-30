// 
//  Copyright 2010-2011  Deveel
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
using System.Collections.Generic;

using Deveel.Data.Types;

namespace Deveel.Data {
	/// <summary>
	/// The default implementation of <see cref="IVariableResolver"/> that 
	/// takes a set of key/value pair to be resolved within an expression
	/// context.
	/// </summary>
	public class VariableResolver : IVariableResolver {
		private readonly int setId;
		private readonly Dictionary<VariableName, TObject> variables;

		/// <summary>
		/// Constructs an empty <see cref="VariableResolver"/> having
		/// the given identifier.
		/// </summary>
		/// <param name="setId"></param>
		public VariableResolver(int setId) {
			this.setId = setId;
			variables = new Dictionary<VariableName, TObject>();
		}

		/// <summary>
		/// Constructs an empty <see cref="VariableResolver"/>.
		/// </summary>
		public VariableResolver()
			: this(0) {
		}

		public int SetId {
			get { return setId; }
		}

		/// <summary>
		/// Adds a variable to the set.
		/// </summary>
		/// <param name="name">The name of the variable to add.</param>
		/// <param name="value">The value of the variable.</param>
		/// <remarks>
		/// The given <paramref name="value"/> must be compliant to
		/// <see cref="TObject"/>: if it is not an instance of
		/// <see cref="TObject"/>, this method will try to convert it.
		/// </remarks>
		/// <seealso cref="AddVariable(string,Deveel.Data.TObject)"/>
		public void AddVariable(string name, object value) {
			if (!(value is TObject))
				value = TObject.CreateObject(value);

			AddVariable(name, (TObject)value);
		}

		/// <summary>
		/// Adds a variable to the set.
		/// </summary>
		/// <param name="name">The name of the variable to add.</param>
		/// <param name="value">The value of the variable.</param>
		/// <exception cref="ArgumentNullException">
		/// If the <paramref name="name"/> given is <c>null</c> or empty.
		/// </exception>
		public void AddVariable(string name, TObject value) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			if (value == null)
				value = TObject.Null;

			VariableName varName = new VariableName(name);
			variables[varName] = value;
		}

		/// <inheritdoc/>
		public TObject Resolve(VariableName variable) {
			TObject value;
			if (!variables.TryGetValue(variable, out value))
				return TObject.Null;

			return value;
		}

		/// <inheritdoc/>
		public TType ReturnTType(VariableName variable) {
			TObject value;
			if (!variables.TryGetValue(variable, out value))
				return PrimitiveTypes.Null;

			return value.TType;
		}
	}
}