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
using System.Collections;
using System.Collections.Generic;

using Deveel.Data.Types;
using Deveel.Math;

namespace Deveel.Data.Sql {
	/// <summary>
	/// A serializable container class for a parsed command language statement.
	/// </summary>
	/// <remarks>
	/// The structure of the tree is entirely dependant on the grammar that was used
	/// to create the tree. This object is a convenient form that can be cached and
	/// serialized to be stored.
	/// <para>
	/// Think of this as the model of a command after the grammar has been parsed
	/// and before it is evaluated.
	/// </para>
	/// </remarks>
	[Serializable]
	sealed class StatementTree : ICloneable, IEnumerable<KeyValuePair<string, object>> {
		/// <summary>
		/// The type of statement this is.
		/// </summary>
		/// <remarks>
		/// This is set to one of the command objects from the 
		/// <see cref="Deveel.Data.Sql"/> namespace. For 
		/// example, if this is a select statement then it points to 
		/// <see cref="SelectStatement"/>.
		/// </remarks>
		private readonly Type statementType;

		/// <summary>
		/// A map that maps from the name of the tree element to the object
		/// that contains information about.
		/// </summary>
		private Dictionary<string, object> values;

		///<summary>
		/// Constructs the <see cref="StatementTree"/>.
		///</summary>
		///<param name="statementTypeName">The string defining the <see cref="Type"/>
		/// of the statement.</param>
		public StatementTree(string statementTypeName)
			: this(Type.GetType(statementTypeName, true, true)) {
		}

		///<summary>
		/// Constructs the <see cref="StatementTree"/>.
		///</summary>
		///<param name="statementType">The <see cref="Type"/> of the statement.</param>
		public StatementTree(Type statementType) {
			if (statementType == null)
				throw new ArgumentNullException("statementType");

			if (!typeof(Statement).IsAssignableFrom(statementType))
				throw new ArgumentException("The type '" + statementType + "' is not derived from " + typeof(Statement) + ".");
			if (statementType.IsAbstract)
				throw new ArgumentException("The statement type '" + statementType + "' is not instantiable.");

			this.statementType = statementType;
			values = new Dictionary<string, object>();
		}

		///<summary>
		/// Sets a new entry into the statement tree map.
		///</summary>
		///<param name="key"></param>
		///<param name="value"></param>
		///<exception cref="ArgumentNullException"></exception>
		///<exception cref="ApplicationException"></exception>
		public void SetValue(string key, object value) {
			if (key == null)
				throw new ArgumentNullException("key");

			// Check on is derived from a known type
			if (value == null ||
			    value is bool ||
			    value is string ||
			    value is BigDecimal ||
			    value is VariableName ||
			    value is int ||
			    value is TObject ||
			    value is TType ||
			    value is Expression ||
			    value is Expression[] ||
			    value is IList ||
			    value is StatementTree ||
			    value is IStatementTreeObject) {
				if (values.ContainsKey(key))
					throw new ApplicationException("Entry '" + key + "' is already present in this tree.");

				values[key] = value;
			} else {
				throw new ApplicationException("value of entry '" + key + "' is not derived from a recognised type");
			}

		}

		///<summary>
		/// Gets an object entry from the statement tree.
		///</summary>
		///<param name="key"></param>
		///<returns></returns>
		public object GetValue(string key) {
			object value;
			if (!values.TryGetValue(key, out value))
				return null;
			return value;
		}


		/// <summary>
		/// Gets the interpreter type that services this tree.
		/// </summary>
		public Type StatementType {
			get { return statementType; }
		}

		/// <summary>
		/// This method will call the <see cref="Expression.Prepare"/> method in 
		/// each expression found in the underlying table.
		/// </summary>
		/// <param name="preparer"></param>
		/// <remarks>
		/// The prepare method is intended to mutate each expression so that 
		/// references can be qualified, sub-queries can be resolved, and variable 
		/// substitutions can be substituted.
		/// </remarks>
		public void PrepareAllExpressions(IExpressionPreparer preparer) {
			foreach (object v in values.Values) {
				if (v != null)
					PrepareExpressionsInObject(v, preparer);
			}
		}

		private static void PrepareExpressionsInObject(object v, IExpressionPreparer preparer) {
			// If expression
			if (v is Expression) {
				((Expression)v).Prepare(preparer);
			}
				// If an array of expression
			else if (v is Expression[]) {
				Expression[] exp_list = (Expression[])v;
				for (int n = 0; n < exp_list.Length; ++n) {
					exp_list[n].Prepare(preparer);
				}
			}
				// If a IStatementTreeObject then can use the 'PrepareExpressions' method.
			else if (v is IStatementTreeObject) {
				IStatementTreeObject stob = (IStatementTreeObject)v;
				stob.PrepareExpressions(preparer);
			}
				// If a StatementTree then can use the PrepareAllExpressions method.
			else if (v is StatementTree) {
				StatementTree st = (StatementTree)v;
				st.PrepareAllExpressions(preparer);
			}
				// If a list of objects,
			else if (v is IList) {
				IList list = (IList)v;
				for (int n = 0; n < list.Count; ++n) {
					Object ob = list[n];
					PrepareExpressionsInObject(ob, preparer);
				}
			}
		}

		///<summary>
		/// Clones a single object.
		///</summary>
		///<param name="entry"></param>
		///<returns></returns>
		///<exception cref="ApplicationException"></exception>
		internal static object CloneSingleObject(object entry) {
			// Immutable entries,
			if (entry == null ||
			    entry is TObject ||
			    entry is TType ||
			    entry is bool ||
			    entry is string ||
			    entry is BigDecimal ||
			    entry is int) {
				// Immutable entries
			} else if (entry is Expression) {
				entry = ((Expression) entry).Clone();
			} else if (entry is Expression[]) {
				Expression[] exps = (Expression[]) ((Expression[]) entry).Clone();
				// Clone each element of the array
				for (int n = 0; n < exps.Length; ++n) {
					exps[n] = (Expression) exps[n].Clone();
				}
				entry = exps;
			} else if (entry is VariableName) {
				entry = ((VariableName) entry).Clone();
			} else if (entry is IStatementTreeObject) {
				entry = ((IStatementTreeObject) entry).Clone();
			} else if (entry is StatementTree) {
				entry = ((StatementTree) entry).Clone();
			} else if (entry is IList) {
				IList list = (IList) entry;
				IList clonedList = (IList)Activator.CreateInstance(entry.GetType(), new object[] { list.Count });
				IEnumerator i = list.GetEnumerator();
				while (i.MoveNext()) {
					clonedList.Add(CloneSingleObject(i.Current));
				}
				entry = clonedList;
			} else {
				throw new ApplicationException("Can't clone the object: " + entry.GetType());
			}

			return entry;
		}

		/// <inheritdoc/>
		public object Clone() {
			// Shallow clone first
			StatementTree v = (StatementTree)MemberwiseClone();
			// Clone the map
			v.values = new Dictionary<string, object>();

			// For each key, clone the entry
			foreach (KeyValuePair<string, object> pair in values) {
				string key = pair.Key;
				object entry = pair.Value;

				entry = CloneSingleObject(entry);

				v.values[key] = entry;
			}

			return v;
		}

		public IEnumerator<KeyValuePair<string, object>> GetEnumerator() {
			return values.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}
	}
}