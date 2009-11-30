//  
//  VariableResolver.cs
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
using System.Collections;

namespace Deveel.Data {
	/// <summary>
	/// The default implementation of <see cref="IVariableResolver"/> that 
	/// takes a set of key/value pair to be resolved withing an expression
	/// context.
	/// </summary>
	public class VariableResolver : IVariableResolver {
		/// <summary>
		/// Constructs an empty <see cref="VariableResolver"/> having
		/// the given identifier.
		/// </summary>
		/// <param name="setId"></param>
		public VariableResolver(int setId) {
			this.setId = setId;
			variables = new Hashtable();
		}

		/// <summary>
		/// Constructs an empty <see cref="VariableResolver"/>.
		/// </summary>
		public VariableResolver()
			: this(0) {
		}

		private readonly int setId;
		private readonly Hashtable variables;

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
				value = TObject.GetObject(value);

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
			if (name == null || name.Length == 0)
				throw new ArgumentNullException("name");

			if (value == null)
				value = TObject.Null;

			VariableName varName = new VariableName(name);
			variables[varName] = value;
		}

		/// <inheritdoc/>
		public TObject Resolve(VariableName variable) {
			TObject value = variables[variable] as TObject;
			return (value == null ? TObject.Null : value);
		}

		/// <inheritdoc/>
		public TType ReturnTType(VariableName variable) {
			TObject var = variables[variable] as TObject;
			return (var == null ? TType.NullType : var.TType);
		}
	}
}