// 
//  SQLQuery.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Data;
using System.IO;
using System.Text;

namespace Deveel.Data.Client {
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
	public sealed class SQLQuery : ICloneable {
		/// <summary>
		/// The SQL String.  For example, "select * from Part".
		/// </summary>
		private String query;

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
		private Object[] parameters;
		private int parameters_index;
		private int parameter_count;


		/// <summary>
		/// Empty constructor.
		/// </summary>
		private SQLQuery() {
		}

		/// <summary>
		/// Constructs the query.
		/// </summary>
		/// <param name="query"></param>
		public SQLQuery(String query) {
			this.query = query;
			parameters = new Object[8];
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
		public void AddVariable(Object ob) {
			ob = TranslateObjectType(ob);
			parameters[parameters_index] = ob;
			++parameters_index;
			++parameter_count;
			if (parameters_index >= parameters.Length) {
				GrowParametersList(parameters_index + 8);
			}
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

		///<summary>
		/// Clears all the parameters.
		///</summary>
		public void Clear() {
			parameters_index = 0;
			parameter_count = 0;
			for (int i = 0; i < parameters.Length; ++i) {
				parameters[i] = null;
			}
		}

		/// <summary>
		/// Returns the query string.
		/// </summary>
		public string Query {
			get { return query; }
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

		/**
		 * Given a JDBC escape code of the form {keyword ... parameters ...} this
		 * will return the most optimal Mckoi SQL query for the code.
		 */
		private String escapeJDBCSubstitution(String jdbc_code) {
			String code = jdbc_code.Substring(1, jdbc_code.Length - 2);
			int kp_delim = code.IndexOf(' ');
			if (kp_delim != -1) {
				String keyword = code.Substring(0, kp_delim);
				String body = code.Substring(kp_delim).Trim();

				if (keyword.Equals("d")) {   // Process a date
					return "DATE " + body;
				}
				if (keyword.Equals("t")) {   // Process a time
					return "TIME " + body;
				}
				if (keyword.Equals("ts")) {  // Process a timestamp
					return "TIMESTAMP " + body;
				}
				if (keyword.Equals("fn")) {  // A function
					return body;
				}
				if (keyword.Equals("call") || keyword.Equals("?=")) {
					throw new DbDataException("Stored procedures not supported.");
				}
				if (keyword.Equals("oj")) {  // Outer join
					return body;
				}

				throw new DbDataException("Do not understand JDBC substitution keyword '" +
										keyword + "' of " + jdbc_code);
			} else {
				throw new DbDataException("Malformed JDBC escape code: " + jdbc_code);
			}

		}

		/**
		 * Performs any JDBC escape processing on the query.  For example, the
		 * code {d 'yyyy-mm-dd'} is converted to 'DATE 'yyyy-mm-dd'.
		 */
		private void doEscapeSubstitutions() {
			// This is a fast but primitive parser that scans the SQL string and
			// substitutes any {[code] ... } type escape sequences to the Mckoi
			// equivalent.  This will not make substitutions of anything inside a
			// quoted area of the query.

			// Exit early if no sign of an escape code
			if (query.IndexOf('{') == -1) {
				return;
			}

			StringBuilder buf = new StringBuilder();
			StringBuilder jdbc_escape = null;

			int i = 0;
			int sz = query.Length;
			char state = '\0';
			bool ignore_next = false;

			while (i < sz) {
				char c = query[i];

				if (state == '\0') {  // If currently processing SQL code
					if (c == '\'' || c == '\"') {
						state = c;     // Set state to quote
					} else if (c == '{') {
						jdbc_escape = new StringBuilder();
						state = '}';
					}
				} else if (state != 0) {  // If currently inside a quote or escape
					if (!ignore_next) {
						if (c == '\\') {
							ignore_next = true;
						} else {
							// If at the end of a quoted area
							if (c == (char)state) {
								state = '\0';
								if (c == '}') {
									jdbc_escape.Append('}');
									buf.Append(escapeJDBCSubstitution(jdbc_escape.ToString()));
									jdbc_escape = null;
									c = ' ';
								}
							}
						}
					} else {
						ignore_next = false;
					}
				}

				if (state != '}') {
					// Copy the character
					buf.Append(c);
				} else {
					jdbc_escape.Append(c);
				}

				++i;
			}

			if (state == '}') {
				throw new DataException("Unterminated JDBC escape code in query: " +
									   jdbc_escape);
			}

			query = buf.ToString();
		}

		///<summary>
		/// Prepares the query by parsing the query string and performing any updates
		/// that are required before being passed down to the lower layers of the
		/// database engine for processing. 
		///</summary>
		///<param name="do_escape_processing"></param>
		public void Prepare(bool do_escape_processing) {
			if (do_escape_processing) {
				doEscapeSubstitutions();
			}
			prepared = true;
		}

		/// <inheritdoc/>
		public override bool Equals(Object ob) {
			SQLQuery q2 = (SQLQuery)ob;
			// NOTE: This could do syntax analysis on the query string to determine
			//   if it's the same or not.
			if (query.Equals(q2.query)) {
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
			SQLQuery q = new SQLQuery();
			q.query = query;
			q.parameters = (Object[])parameters.Clone();
			q.parameters_index = parameters_index;
			q.parameter_count = parameter_count;
			q.prepared = prepared;
			return q;
		}

		/// <inheritdoc/>
		public override string ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append("[ Query:\n[ ");
			buf.Append(Query);
			buf.Append(" ]\n");
			if (parameter_count > 0) {
				buf.Append("\nParams:\n[ ");
				for (int i = 0; i < parameter_count; ++i) {
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
			buf.Append("\n]");
			return buf.ToString();
		}

		// ---------- Stream transfer methods ----------

		///<summary>
		/// Writes the SQL query to the data output stream.
		///</summary>
		///<param name="output"></param>
		public void WriteTo(BinaryWriter output) {
			output.Write(query);
			output.Write(parameter_count);
			for (int i = 0; i < parameter_count; ++i) {
				ObjectTransfer.WriteTo(output, parameters[i]);
			}
		}

		/// <summary>
		/// Reads an <see cref="SQLQuery"/> object from the data input stream.
		/// </summary>
		/// <param name="input"></param>
		/// <returns></returns>
		public static SQLQuery ReadFrom(BinaryReader input) {
			String query_string = input.ReadString();
			SQLQuery query = new SQLQuery(query_string);
			int arg_length = input.ReadInt32();
			for (int i = 0; i < arg_length; ++i) {
				query.AddVariable(ObjectTransfer.ReadFrom(input));
			}
			return query;
		}

		///<summary>
		/// Serializes an <see cref="SQLQuery"/> object to a <see cref="ByteLongObject"/>.
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
		/// Deserializes an <see cref="SQLQuery"/> object from a <see cref="ByteLongObject"/>.
		///</summary>
		///<param name="ob"></param>
		///<returns></returns>
		///<exception cref="ApplicationException"></exception>
		public static SQLQuery DeserializeFromBlob(ByteLongObject ob) {
			BinaryReader input = new BinaryReader(new MemoryStream(ob.ToArray()), Encoding.UTF8);
			try {
				return ReadFrom(input);
			} catch (IOException e) {
				throw new ApplicationException("IO Error: " + e.Message);
			}
		}
	}
}