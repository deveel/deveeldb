// 
//  Privileges.cs
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
using System.Text;

namespace Deveel.Data {
	///<summary>
	/// A set of privileges to grant a user for an object.
	///</summary>
	public class Privileges {
		/// <summary>
		/// The number of bits available to set.
		/// </summary>
		internal const int BitCount = 11;

		/// <summary>
		/// The bit mask.  There are currently 11 used bits, so this has all 11 bits set.
		/// </summary>
		private const int BitMask = (1 << BitCount) - 1;

		/// <summary>
		/// The priv to allow full access to the database object.
		/// </summary>
		/// <remarks>
		/// If this is used, it should be the only privilege added.
		/// </remarks>
		public const int All = BitMask;

		/// <summary>
		/// The privilege to alter objects.
		/// </summary>
		/// <remarks>
		/// Only applicable for SCHEMA grant objects.
		/// </remarks>
		public const int Alter = 0x0100;

		/// <summary>
		/// The priv to compact a database object.
		/// </summary>
		public const int Compact = 0x040;

		/// <summary>
		/// The privilege to create objects.
		/// </summary>
		/// <remarks>
		/// Only applicable for SCHEMA grant objects.
		/// </remarks>
		public const int Create = 0x080;

		/// <summary>
		/// The priv to DELETE from a database object.
		/// </summary>
		public const int Delete = 0x02;

		/// <summary>
		/// The privilege to drop objects.
		/// </summary>
		/// <remarks>
		/// Only applicable for SCHEMA grant objects.
		/// </remarks>
		public const int Drop = 0x0200;

		/// <summary>
		/// The priv to INSERT to a database object.
		/// </summary>
		public const int Insert = 0x08;

		/// <summary>
		/// The privilege to view objects in a schema.
		/// </summary>
		/// <remarks>
		/// Only applicable for SCHEMA grant objects.
		/// </remarks>
		public const int List = 0x0400;

		/// <summary>
		/// The priv to REFERENCE a database object.
		/// </summary>
		public const int References = 0x010;

		/// <summary>
		/// The priv to SELECT from a database object.
		/// </summary>
		public const int Select = 0x01;

		/// <summary>
		/// The priv to UPDATE a database object.
		/// </summary>
		public const int Update = 0x04;

		/// <summary>
		/// The priv to see statistics on a database object.
		/// </summary>
		public const int Usage = 0x020;

		/// <summary>
		/// No privileges.
		/// </summary>
		public static readonly Privileges Empty;

		/// <summary>
		/// All access (execute/update/delete/etc) privs for a procedure object.
		/// </summary>
		public static readonly Privileges ProcedureAll;

		/// <summary>
		/// Execute access privs for a procedure object.
		/// </summary>
		public static readonly Privileges ProcedureExecute;

		/// <summary>
		/// All access privs for a schema object.
		/// </summary>
		public static readonly Privileges SchemaAll;

		/// <summary>
		/// Read access privs for a schema object.
		/// </summary>
		public static readonly Privileges SchemaRead;

		/// <summary>
		/// Enable all privs for the object.
		/// </summary>
		public static readonly Privileges TableAll;

		/// <summary>
		/// Read privs for the object.
		/// </summary>
		public static readonly Privileges TableRead;


		// ---------- Members ----------

		/// <summary>
		/// The priv bit map.
		/// </summary>
		private readonly int privs;

		static Privileges() {
			Privileges p;

			Empty = new Privileges();

			p = Empty;
			p = p.Add(Select);
			p = p.Add(Delete);
			p = p.Add(Update);
			p = p.Add(Insert);
			p = p.Add(References);
			p = p.Add(Usage);
			p = p.Add(Compact);
			TableAll = p;

			p = Empty;
			p = p.Add(Select);
			p = p.Add(Usage);
			TableRead = p;

			p = Empty;
			p = p.Add(Create);
			p = p.Add(Alter);
			p = p.Add(Drop);
			p = p.Add(List);
			SchemaAll = p;

			p = Empty;
			p = p.Add(List);
			SchemaRead = p;

			p = Empty;
			p = p.Add(Select);
			p = p.Add(Delete);
			p = p.Add(Update);
			p = p.Add(Insert);
			ProcedureAll = p;

			p = Empty;
			p = p.Add(Select);
			ProcedureExecute = p;
		}

		private Privileges(int privs) {
			this.privs = privs & BitMask;
		}

		///<summary>
		///</summary>
		public Privileges()
			: this(0) {
		}

		/// <summary>
		/// Returns true if this Privileges object contains no priv entries.
		/// </summary>
		public bool IsEmpty {
			get { return privs == 0; }
		}

		/// <summary>
		/// Adds a privilege and returns a new Privileges object with the 
		/// new priv set.
		/// </summary>
		/// <param name="priv"></param>
		/// <returns></returns>
		public Privileges Add(int priv) {
			return new Privileges(privs | priv);
		}

		/// <summary>
		/// Removes a privilege with a column list parameter.
		/// </summary>
		/// <param name="priv"></param>
		/// <returns></returns>
		public Privileges Remove(int priv) {
			int and_priv = (privs & priv);
			return new Privileges(privs ^ and_priv);
		}

		/// <summary>
		/// Removes the given privileges from this privileges object and returns the
		/// new privileges object.
		/// </summary>
		/// <param name="privs"></param>
		/// <returns></returns>
		public Privileges Remove(Privileges privs) {
			return Remove(privs.privs);
		}

		/// <summary>
		/// Returns true if this privileges permits the given priv.
		/// </summary>
		/// <param name="priv"></param>
		/// <returns></returns>
		public bool Permits(int priv) {
			return (privs & priv) != 0;
		}

		///<summary>
		/// Merges privs from the given privilege object with this set of privs.
		///</summary>
		///<param name="in_privs"></param>
		/// <remarks>
		/// This performs an OR on all the attributes in the set.  If the entry
		/// does not exist in this set then it is added.
		/// </remarks>
		///<returns></returns>
		public Privileges Merge(Privileges in_privs) {
			return Add(in_privs.privs);
		}

		/// <summary>
		/// Returns a String that represents the given priv bit.
		/// </summary>
		/// <param name="priv"></param>
		/// <returns></returns>
		internal static String FormatPriv(int priv) {
			if ((priv & Select) != 0)
				return "SELECT";
			if ((priv & Delete) != 0)
				return "DELETE";
			if ((priv & Update) != 0)
				return "UPDATE";
			if ((priv & Insert) != 0)
				return "INSERT";
			if ((priv & References) != 0)
				return "REFERENCES";
			if ((priv & Usage) != 0)
				return "USAGE";
			if ((priv & Compact) != 0)
				return "COMPACT";
			if ((priv & Create) != 0)
				return "CREATE";
			if ((priv & Alter) != 0)
				return "ALTER";
			if ((priv & Drop) != 0)
				return "DROP";
			if ((priv & List) != 0)
				return "LIST";
			throw new ApplicationException("Not priv bit set.");
		}

		/// <summary>
		/// Given a string, returns the priv bit for it.
		/// </summary>
		/// <param name="priv"></param>
		/// <returns></returns>
		public static int ParseString(String priv) {
			if (priv.Equals("SELECT"))
				return Select;
			if (priv.Equals("DELETE"))
				return Delete;
			if (priv.Equals("UPDATE"))
				return Update;
			if (priv.Equals("INSERT"))
				return Insert;
			if (priv.Equals("REFERENCES"))
				return References;
			if (priv.Equals("USAGE"))
				return Usage;
			if (priv.Equals("COMPACT"))
				return Compact;
			if (priv.Equals("CREATE"))
				return Create;
			if (priv.Equals("ALTER"))
				return Alter;
			if (priv.Equals("DROP"))
				return Drop;
			if (priv.Equals("LIST"))
				return List;

			throw new ApplicationException("Priv not recognised.");
		}

		/// <summary>
		/// Returns this Privileges object as an encoded int bit array.
		/// </summary>
		/// <returns></returns>
		public int ToInt32() {
			return privs;
		}

		/// <summary>
		/// Converts this privilege to an encoded string.
		/// </summary>
		/// <returns></returns>
		public String ToEncodedString() {
			StringBuilder buf = new StringBuilder();
			buf.Append("||");
			int priv_bit = 1;
			for (int i = 0; i < 11; ++i) {
				if ((privs & priv_bit) != 0) {
					buf.Append(FormatPriv(priv_bit));
					buf.Append("||");
				}
				priv_bit = priv_bit << 1;
			}
			return buf.ToString();
		}

		/// <inheritdoc/>
		public override string ToString() {
			StringBuilder buf = new StringBuilder();
			int priv_bit = 1;
			for (int i = 0; i < 11; ++i) {
				if ((privs & priv_bit) != 0) {
					buf.Append(FormatPriv(priv_bit));
					buf.Append(' ');
				}
				priv_bit = priv_bit << 1;
			}
			return buf.ToString();
		}

		/// <inheritdoc/>
		public override bool Equals(Object ob) {
			return privs == ((Privileges) ob).privs;
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return base.GetHashCode();
		}
	}
}