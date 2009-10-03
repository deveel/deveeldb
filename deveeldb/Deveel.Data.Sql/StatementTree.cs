// 
//  StatementTree.cs
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
using System.Collections;

using Deveel.Math;

namespace Deveel.Data.Sql {
	/// <summary>
	/// A serializable container class for a parsed query language statement.
	/// </summary>
	/// <remarks>
	/// The structure of the tree is entirely dependant on the grammar that was used
	/// to create the tree. This object is a convenient form that can be cached and
	/// serialized to be stored.
	/// <para>
	/// Think of this as the model of a query after the grammar has been parsed
	/// and before it is evaluated.
	/// </para>
	/// </remarks>
	[Serializable]
	public sealed class StatementTree : ICloneable {

		/// <summary>
		/// The type of statement this is.
		/// </summary>
		/// <remarks>
		/// This is set to one of the query objects from the 
		/// <see cref="Deveel.Data.Sql"/> namespace. For 
		/// example, if this is a select statement then it points to 
		/// <see cref="SelectStatement"/>.
		/// </remarks>
		private readonly Type statement_type;

		/// <summary>
		/// A map that maps from the name of the tree element to the object
		/// that contains information about.
		/// </summary>
		/// <remarks>
		/// For example, if this is an SQL SELECT statement then entries in this map may be:
		/// <code>
		///    "columns" -> sql.SelectColumn[]
		///    "distinct" -> new Boolean(true)
		/// </code>
		/// </remarks>
		private Hashtable map;

		///<summary>
		/// Constructs the <see cref="StatementTree"/>.
		///</summary>
		///<param name="statement_type">The string defining the <see cref="Type"/>
		/// of the statement.</param>
		public StatementTree(String statement_type)
			: this(Type.GetType(statement_type, true, true)) {
		}

		///<summary>
		/// Constructs the <see cref="StatementTree"/>.
		///</summary>
		///<param name="statement_type">The <see cref="Type"/> of the statement.</param>
		public StatementTree(Type statement_type) {
			if (!typeof(Statement).IsAssignableFrom(statement_type))
				throw new ArgumentException("The type '" + statement_type + "' is not derived from Statement.");
			this.statement_type = statement_type;
			map = new Hashtable();
		}

		///<summary>
		/// Sets a new entry into the statement tree map.
		///</summary>
		///<param name="entry_name"></param>
		///<param name="ob"></param>
		///<exception cref="ArgumentNullException"></exception>
		///<exception cref="ApplicationException"></exception>
		public void SetObject(String entry_name, Object ob) {
			if (entry_name == null) {
				throw new ArgumentNullException("entry_name");
			}
			// Check on is derived from a known type
			if (ob == null ||
			    ob is Boolean ||
			    ob is String ||
			    ob is BigDecimal ||
			    ob is Variable ||
			    ob is int ||
			    ob is TObject ||
			    ob is TType ||
			    ob is Expression ||
			    ob is Expression[] ||
			    ob is IList ||
			    ob is StatementTree ||
			    ob is IStatementTreeObject) {

				if (map.ContainsKey(entry_name)) {
					throw new ApplicationException("Entry '" + entry_name +
					                               "' is already present in this tree.");
				}

				map[entry_name] = ob;
			} else {
				throw new ApplicationException("ob of entry '" + entry_name +
				                               "' is not derived from a recognised class");
			}

		}

		///<summary>
		/// Sets a boolean into the statement tree map.
		///</summary>
		///<param name="entry_name"></param>
		///<param name="b"></param>
		public void SetBoolean(String entry_name, bool b) {
			SetObject(entry_name, b);
		}

		///<summary>
		/// Sets an integer into the statement tree map.
		///</summary>
		///<param name="entry_name"></param>
		///<param name="v"></param>
		public void SetInt(String entry_name, int v) {
			SetObject(entry_name, v);
		}


		///<summary>
		/// Gets an object entry from the statement tree.
		///</summary>
		///<param name="entry_name"></param>
		///<returns></returns>
		public Object GetObject(String entry_name) {
			return map[entry_name];
		}

		///<summary>
		/// Gets a boolean entry from the statement tree.
		///</summary>
		///<param name="entry_name"></param>
		///<returns></returns>
		public bool GetBoolean(String entry_name) {
			Object ob = map[entry_name];
			return (bool)ob;
		}

		/// <summary>
		/// Gets an integer entry from the statement tree.
		/// </summary>
		/// <param name="entry_name"></param>
		/// <returns></returns>
		public int GetInt(String entry_name) {
			Object ob = map[entry_name];
			return (int)ob;
		}


		/// <summary>
		/// Gets the interpreter type that services this tree.
		/// </summary>
		public Type StatementType {
			get { return statement_type; }
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
			IEnumerator i = map.Values.GetEnumerator();

			while (i.MoveNext()) {
				Object v = i.Current;
				if (v != null) {
					PrepareExpressionsInObject(v, preparer);
				}
			}

		}

		private static void PrepareExpressionsInObject(Object v, IExpressionPreparer preparer) {
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
		public static Object CloneSingleObject(Object entry) {

			// Immutable entries,
			if (entry == null ||
			    entry is TObject ||
			    entry is TType ||
			    entry is Boolean ||
			    entry is String ||
			    entry is BigDecimal ||
			    entry is int) {
				// Immutable entries
			} else if (entry is Expression) {
				entry = ((Expression)entry).Clone();
			} else if (entry is Expression[]) {
				Expression[] exps = (Expression[])((Expression[])entry).Clone();
				// Clone each element of the array
				for (int n = 0; n < exps.Length; ++n) {
					exps[n] = (Expression)exps[n].Clone();
				}
				entry = exps;
			} else if (entry is Variable) {
				entry = ((Variable)entry).Clone();
			} else if (entry is IStatementTreeObject) {
				entry = ((IStatementTreeObject)entry).Clone();
			} else if (entry is StatementTree) {
				entry = ((StatementTree)entry).Clone();
			} else if (entry is IList) {
				// Clone the list by making a new ArrayList and adding a cloned version
				// of each element into it.
				IList list = (IList)entry;
				ArrayList cloned_list = new ArrayList(list.Count);
				IEnumerator i = list.GetEnumerator();
				while (i.MoveNext()) {
					cloned_list.Add(CloneSingleObject(i.Current));
				}
				entry = cloned_list;
			} else {
				throw new ApplicationException("Can't clone the object: " + entry.GetType());
			}

			return entry;
		}

		/// <inheritdoc/>
		public Object Clone() {
			// Shallow clone first
			StatementTree v = (StatementTree)MemberwiseClone();
			// Clone the map
			Hashtable cloned_map = new Hashtable();
			v.map = cloned_map;

			// For each key, clone the entry
			IEnumerator i = map.Keys.GetEnumerator();
			while (i.MoveNext()) {
				Object key = i.Current;
				Object entry = map[key];

				entry = CloneSingleObject(entry);

				cloned_map[key] = entry;
			}

			return v;
		}

		/// <inheritdoc/>
		public override String ToString() {
			return "[ " + StatementType + " [ " + map + " ] ]";
		}

	}
}