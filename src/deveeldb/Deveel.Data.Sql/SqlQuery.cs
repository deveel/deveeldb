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
using System.IO;
using System.Text;

namespace Deveel.Data.Sql {
	///<summary>
	/// Represents an SQL query to the database.
	///</summary>
	/// <remarks>
	/// This includes the query string itself plus any data types 
	/// that are part of the query.
	/// <para>
	/// This could do some preliminary parsing of the query string 
	/// for faster translation by the database.
	/// </para>
	/// </remarks>
	public sealed class SqlQuery : ICloneable {
		/// <summary>
		/// The SQL String.  For example, "select * from Part".
		/// </summary>
		private String text;

		/// <summary>
		/// Set to true when this query is prepared via the <see cref="Prepare"/> method.
		/// </summary>
		private bool prepared;

		/// <summary>
		/// The list of all variable substitutions that are in the query.
		/// </summary>
		/// <remarks>
		/// A variable substitution is set up in a prepared statement.
		/// </remarks>
		private object[] parameters;
		private string[] parameters_names;
		private int parameters_index;
		private int parameter_count;

		private ParameterStyle parameterStyle;


		/// <summary>
		/// Empty constructor.
		/// </summary>
		private SqlQuery() {
		}

		/// <summary>
		/// Constructs the command.
		/// </summary>
		/// <param name="text"></param>
		public SqlQuery(String text)
			: this(text, ParameterStyle.Marker) {
		}

		public SqlQuery(string text, ParameterStyle parameterStyle) {
			this.text = text;
			this.parameterStyle = parameterStyle;
			parameters = new Object[8];
			parameters_names = new string[8];
			parameters_index = 0;
			parameter_count = 0;
			prepared = false;
		}

		/// <summary>
		/// Grows the parameters list to the given size.
		/// </summary>
		/// <param name="new_size"></param>
		private void GrowParametersList(int new_size) {
			// Make new list
			Object[] new_list = new Object[new_size];
			// Copy everything to new list
			Array.Copy(parameters, 0, new_list, 0, parameters.Length);
			// Set the new list.
			parameters = new_list;

			if (parameterStyle == ParameterStyle.Named) {
				string[] new_names_list = new string[new_size];
				Array.Copy(parameters_names, 0, new_names_list, 0, parameters_names.Length);
				parameters_names = new_names_list;
			}
		}

		/// <summary>
		/// Translates the given object to a type the object can process.
		/// </summary>
		/// <param name="ob"></param>
		/// <returns></returns>
		private static Object TranslateObjectType(Object ob) {
			return ObjectTranslator.Translate(ob);
		}

		///<summary>
		/// Adds a variable to the query.
		///</summary>
		///<param name="ob"></param>
		/// <remarks>
		/// If the object is not a type that is a database <i>primitive</i> type 
		/// (<see cref="Deveel.Math.BigDecimal"/>, <see cref="ByteLongObject"/>, 
		/// <see cref="bool"/>, <see cref="DateTime"/>, <see cref="String"/>) then 
		/// it is serialized and the serialized form is wrapped in a <see cref="ByteLongObject"/>.
		/// </remarks>
		public void AddVariable(object ob) {
			if (parameterStyle != ParameterStyle.Marker)
				throw new NotSupportedException();

			AddVariable(null, ob);
		}

		public void AddVariable(string name, object value) {
			if ((name != null && name.Length > 0) &&
				parameterStyle != ParameterStyle.Named)
				throw new ArgumentException();
			if ((name == null || name.Length == 0) &&
				parameterStyle == ParameterStyle.Named)
				throw new ArgumentException();

			value = TranslateObjectType(value);

			if (parameterStyle == ParameterStyle.Named)
				parameters_names[parameters_index] = name;
			parameters[parameters_index] = value;
			++parameters_index;
			++parameter_count;
			if (parameters_index >= parameters.Length)
				GrowParametersList(parameters_index + 8);
		}

		///<summary>
		/// Sets a variable at the given index, growing the list of
		/// variables if necessary.
		///</summary>
		///<param name="i"></param>
		///<param name="ob"></param>
		/// <remarks>
		/// If the object is not a type that is a database <i>primitive</i> type 
		/// (<see cref="Deveel.Math.BigDecimal"/>, <see cref="ByteLongObject"/>, 
		/// <see cref="Boolean"/>, <see cref="DateTime"/>, <see cref="String"/>) 
		/// then it is serialized and the serialized form is wrapped in a 
		/// <see cref="ByteLongObject"/>.
		/// </remarks>
		public void SetVariable(int i, Object ob) {
			ob = TranslateObjectType(ob);
			if (i >= parameters.Length) {
				GrowParametersList(i + 8);
			}
			parameters[i] = ob;
			parameters_index = i + 1;
			parameter_count = System.Math.Max(parameters_index, parameter_count);
		}

		public object GetNamedVariable(string name) {
			if (parameterStyle != ParameterStyle.Named)
				throw new NotSupportedException();

			for (int i = 0; i < parameters_names.Length; i++) {
				string paramName = parameters_names[i];
				//TODO: should we do a case-insensitive comparison?
				if (String.Compare(paramName, name, false) == 0)
					return parameters[i];
			}

			return null;
		}

		public void SetNamedVariable(string name, object value) {
			if (parameterStyle != ParameterStyle.Named)
				throw new NotSupportedException();

			int toUpdate = -1;
			for (int i = 0; i < parameters_names.Length; i++) {
				string paramName = parameters_names[i];
				//TODO: should we do a case-insensitive comparison?
				if (String.Compare(paramName, name, false) == 0) {
					toUpdate = i;
					break;
				}
			}

			if (toUpdate == -1)
				throw new ArgumentException("Unable to find the variable named '" + name + "'.");

			SetVariable(toUpdate, value);
		}

		///<summary>
		/// Clears all the parameters.
		///</summary>
		public void Clear() {
			parameters_index = 0;
			parameter_count = 0;
			for (int i = 0; i < parameters.Length; ++i) {
				parameters[i] = null;
			}
			if (parameterStyle == ParameterStyle.Named) {
				for (int i = 0; i < parameters_names.Length; i++)
					parameters_names[i] = null;
			}
		}

		/// <summary>
		/// Returns the query string.
		/// </summary>
		public string Text {
			get { return text; }
		}

		/// <summary>
		/// Gets the parameter style used in this command.
		/// </summary>
		public ParameterStyle ParameterStyle {
			get { return parameterStyle; }
		}

		///<summary>
		/// Returns the array of all objects that are to be used as substitutions 
		/// for '?' in the query.
		///</summary>
		/// <remarks>
		/// The array returned references internal object[] here so don't change!
		/// </remarks>
		public object[] Variables {
			get { return parameters; }
		}

		public string[] VariableNames {
			get { return parameters_names; }
		}

		/// <inheritdoc/>
		public override bool Equals(Object ob) {
			SqlQuery q2 = (SqlQuery)ob;
			// NOTE: This could do syntax analysis on the query string to determine
			//   if it's the same or not.
			if (text.Equals(q2.text)) {
				if (parameter_count == q2.parameter_count) {
					for (int i = 0; i < parameter_count; ++i) {
						if (parameters[i] != q2.parameters[i]) {
							return false;
						}
					}
					return true;
				}
			}
			return false;
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return base.GetHashCode();
		}

		/// <inheritdoc/>
		public object Clone() {
			SqlQuery q = new SqlQuery();
			q.text = text;
			q.parameters = (Object[])parameters.Clone();
			q.parameters_index = parameters_index;
			q.parameter_count = parameter_count;
			q.prepared = prepared;
			return q;
		}

		/// <inheritdoc/>
		public override string ToString() {
			StringBuilder buf = new StringBuilder();
			buf.AppendLine("[ Command: ");
			buf.Append("[ ");
			buf.Append(Text);
			buf.AppendLine(" ]");
			if (parameter_count > 0) {
				buf.AppendLine();
				buf.AppendLine("Params: ");
				buf.Append("[ ");
				for (int i = 0; i < parameter_count; ++i) {
					if (parameterStyle == ParameterStyle.Named) {
						buf.Append(parameters_names[i]);
						buf.Append(" : ");
					}
					Object ob = parameters[i];
					if (ob == null) {
						buf.Append("NULL");
					} else {
						buf.Append(parameters[i].ToString());
					}
					buf.Append(", ");
				}
				buf.Append(" ]");
			}
			buf.AppendLine();
			buf.Append("]");
			return buf.ToString();
		}

		// ---------- Stream transfer methods ----------

		///<summary>
		/// Writes the SQL query to the data output stream.
		///</summary>
		///<param name="output"></param>
		public void WriteTo(BinaryWriter output) {
			output.Write(text);
			output.Write((byte)parameterStyle);
			output.Write(parameter_count);
			for (int i = 0; i < parameter_count; ++i) {
				if (parameterStyle == ParameterStyle.Named)
					output.Write(parameters_names[i]);

				ObjectTransfer.WriteTo(output, parameters[i]);
			}
		}

		/// <summary>
		/// Reads an <see cref="SqlQuery"/> object from the data input stream.
		/// </summary>
		/// <param name="input"></param>
		/// <returns></returns>
		public static SqlQuery ReadFrom(BinaryReader input) {
			String query_string = input.ReadString();
			ParameterStyle style = (ParameterStyle)input.ReadByte();
			SqlQuery query = new SqlQuery(query_string, style);

			int arg_length = input.ReadInt32();

			for (int i = 0; i < arg_length; ++i) {
				string name = null;
				if (style == ParameterStyle.Named)
					name = input.ReadString();

				object value = ObjectTransfer.ReadFrom(input);
				query.AddVariable(name, value);
			}
			return query;
		}

		///<summary>
		/// Serializes an <see cref="SqlQuery"/> object to a <see cref="ByteLongObject"/>.
		///</summary>
		///<returns></returns>
		///<exception cref="ApplicationException"></exception>
		public ByteLongObject SerializeToBlob() {
			MemoryStream bout = new MemoryStream();
			BinaryWriter output = new BinaryWriter(bout);
			try {
				WriteTo(output);
				output.Flush();
				return new ByteLongObject(bout.ToArray());
			} catch (IOException e) {
				throw new ApplicationException("IO Error: " + e.Message);
			}
		}

		///<summary>
		/// Deserializes an <see cref="SqlQuery"/> object from a <see cref="ByteLongObject"/>.
		///</summary>
		///<param name="ob"></param>
		///<returns></returns>
		///<exception cref="ApplicationException"></exception>
		public static SqlQuery DeserializeFromBlob(ByteLongObject ob) {
			BinaryReader input = new BinaryReader(new MemoryStream(ob.ToArray()), Encoding.Unicode);
			try {
				return ReadFrom(input);
			} catch (IOException e) {
				throw new ApplicationException("IO Error: " + e.Message);
			}
		}
	}
}