// 
//  Copyright 2010-2018 Deveel
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
//

using System;
using System.Threading.Tasks;

using Deveel.Data.Query;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Variables {
	/// <summary>
	/// A volatile variable that is defined in the scope of a command 
	/// </summary>
	/// <remarks>
	/// <para>
	/// Variables are named and strongly typed, defining also some attributes
	/// that rule the behavior to their access: a variable can be <i>constant</i>,
	/// meaning its value cannot be altered after the assignment
	/// </para>
	/// <para>
	/// A variable is intended to live only within a certain scope of execution
	/// of a command to the system
	/// </para>
	/// </remarks>
	public sealed class Variable : IDbObject, ISqlFormattable {
		private static readonly char[] InvalidChars = " $.|\\:/#'".ToCharArray();

		/// <summary>
		/// Constructs a new <see cref="Variable"/> from the given
		/// information object that describes it
		/// </summary>
		/// <param name="variableInfo">The <see cref="VariableInfo"/> information
		/// object that describes the metadata of a variable</param>
		/// <seealso cref="Variables.VariableInfo"/>
		/// <seealso cref="VariableInfo"/>
		public Variable(VariableInfo variableInfo) {
			VariableInfo = variableInfo ?? throw new ArgumentNullException(nameof(variableInfo));
		}

		/// <summary>
		/// Constructs a variable with the given name, type and value
		/// </summary>
		/// <param name="name">The name of the variable</param>
		/// <param name="type">The <see cref="SqlType"/> of the variable</param>
		/// <param name="constant">The flag that indicates if the value of the variable
		/// is constant</param>
		/// <param name="defaultValue">The default value of the variable. In case the
		/// variable is set to be constant, this is the constant value of the variable</param>
		public Variable(string name, SqlType type, bool constant, SqlExpression defaultValue)
			: this(new VariableInfo(name, type, constant, defaultValue)) {
		}

		/// <summary>
		/// Constructs a variable with the given name, type and value
		/// </summary>
		/// <param name="name">The name of the variable</param>
		/// <param name="type">The <see cref="SqlType"/> of the variable</param>
		public Variable(string name, SqlType type)
			: this(name, type, null) {
		}

		/// <summary>
		/// Constructs a variable with the given name, type and value
		/// </summary>
		/// <param name="name">The name of the variable</param>
		/// <param name="type">The <see cref="SqlType"/> of the variable</param>
		/// <param name="defaultValue">The default value of the variable, returned if
		/// the value was not set explicitly.</param>
		public Variable(string name, SqlType type, SqlExpression defaultValue)
			: this(name, type, false, defaultValue) {
		}

		/// <summary>
		/// Gets the <see cref="Variables.VariableInfo"/> object that describes
		/// the characteristics of the variable
		/// </summary>
		public VariableInfo VariableInfo { get; }

		/// <summary>
		/// Gets the name of the variable
		/// </summary>
		/// <seealso cref="VariableInfo"/>
		public string Name => VariableInfo.Name;

		/// <summary>
		/// Gets the indication if the value of this variable
		/// is constant
		/// </summary>
		/// <seealso cref="VariableInfo"/>
		public bool Constant => VariableInfo.Constant;

		/// <summary>
		/// Gets the <see cref="SqlType"/> of the variable
		/// </summary>
		public SqlType Type => VariableInfo.Type;

		/// <summary>
		/// Gets the value of the variable
		/// </summary>
		public SqlExpression Value { get; private set; }

		IDbObjectInfo IDbObject.ObjectInfo => VariableInfo;

		/// <summary>
		/// Assigns a given value of the variable
		/// </summary>
		/// <param name="value">The value to be set to the variable</param>
		/// <param name="context">The <see cref="QueryContext"/> used to
		/// evaluate the destination type of the value to assign</param>
		/// <returns>
		/// Return the instance of <see cref="SqlExpression"/> that is assigned
		/// to the variable.
		/// </returns>
		/// <seealso cref="Value"/>
		/// <exception cref="VariableException">
		/// If the variable is set to have a constant value
		/// </exception>
		/// <exception cref="ArgumentException">
		/// If the <see cref="SqlType"/> of the value to be set is not compatible
		/// with the <see cref="SqlType"/> of the variable
		/// </exception>
		public SqlExpression SetValue(SqlExpression value, IContext context) {
			if (value == null)
				throw new ArgumentNullException(nameof(value));

			if (VariableInfo.Constant)
				throw new VariableException($"Cannot set constant variable {VariableInfo.Name}");

			var valueType = value.GetSqlType(context);
			if (!valueType.Equals(VariableInfo.Type) &&
				!valueType.IsComparable(VariableInfo.Type))
				throw new ArgumentException($"The type {valueType} of the value is not compatible with the variable type '{VariableInfo.Type}'");

			Value = value;
			return Value;
		}

		/// <summary>
		/// Evaluates the current value of the variable and returns
		/// a reduced version of the value at its most elementary form
		/// </summary>
		/// <param name="context">The context used to resolve the value of
		/// the variable</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlExpression"/> that represents
		/// the version of the value of the variable
		/// </returns>
		public Task<SqlExpression> Evaluate(IContext context) {
			var value = Value;
			if (value == null)
				value = VariableInfo.DefaultValue;

			if (value == null)
				throw new VariableException($"Variable {VariableInfo.Name} has no value set");

			return value.ReduceAsync(context);
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			builder.AppendFormat(":{0}", VariableInfo.Name);
			builder.Append(" ");
			if (VariableInfo.Constant)
				builder.Append("CONSTANT ");

			VariableInfo.Type.AppendTo(builder);

			if (VariableInfo.HasDefaultValue) {
				builder.Append(" := ");
				VariableInfo.DefaultValue.AppendTo(builder);
			}
		}

		public override string ToString() {
			return this.ToSqlString();
		}

		/// <summary>
		/// Validates that the string provided can be a valid variable name
		/// </summary>
		/// <param name="name">The name to validate</param>
		/// <returns>
		/// Returns a boolean value indicating if the string argument
		/// is a valid variable name.
		/// </returns>
		public static bool IsValidName(string name) {
			return name.IndexOfAny(InvalidChars) == -1;
		}
	}
}