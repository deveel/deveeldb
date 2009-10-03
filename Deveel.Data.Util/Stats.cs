// 
//  Stats.cs
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
using System.IO;
using System.Text;

namespace Deveel.Data {
	///<summary>
	/// An object that is used to store and update various stats.
	///</summary>
	/// <remarks>
	/// <b>Note</b>: This object is thread safe.
	/// </remarks>
	public sealed class Stats {

		/// <summary>
		/// Where the stat properties are held.
		/// </summary>
		private readonly Hashtable properties;

		///<summary>
		///</summary>
		public Stats() {
			// We need lookup on this hash to be really quick, so load factor is
			// low and initial capacity is high.
			properties = new Hashtable(250, 0.50f);
		}

		///<summary>
		/// Resets all stats that start with "{session}" to 0.
		///</summary>
		/// <remarks>
		/// This should be called when we are collecting stats 
		/// over a given session and a session has finished.
		/// </remarks>
		public void ResetSession() {
			lock (this) {
				ICollection key_set = properties.Keys;
				String[] keys = new String[key_set.Count];
				int index = 0;
				IEnumerator it = key_set.GetEnumerator();
				while (it.MoveNext()) {
					keys[index] = (String)it.Current;
					++index;
				}

				// If key starts with a "{session}" then reset it to 0.
				for (int i = 0; i < keys.Length; ++i) {
					if (keys[i].StartsWith("{session}")) {
						IntegerStat stat = (IntegerStat)properties[keys[i]];
						stat.value = 0;
					}
				}
			}
		}

		///<summary>
		/// Adds the given value to a stat property.
		///</summary>
		///<param name="value"></param>
		///<param name="stat_name"></param>
		public void Add(int value, String stat_name) {
			lock (this) {
				IntegerStat stat = (IntegerStat)properties[stat_name];
				if (stat != null) {
					stat.value += value;
				} else {
					stat = new IntegerStat();
					stat.value = value;
					properties[stat_name] = stat;
				}
			}
		}

		///<summary>
		/// Increments a stat property.
		///</summary>
		///<param name="stat_name"></param>
		public void Increment(String stat_name) {
			lock (this) {
				IntegerStat stat = (IntegerStat)properties[stat_name];
				if (stat != null) {
					++stat.value;
				} else {
					stat = new IntegerStat();
					stat.value = 1;
					properties[stat_name] = stat;
				}
			}
		}

		///<summary>
		/// Decrements a stat property.
		///</summary>
		///<param name="stat_name"></param>
		public void Decrement(String stat_name) {
			lock (this) {
				IntegerStat stat = (IntegerStat)properties[stat_name];
				if (stat != null) {
					--stat.value;
				} else {
					stat = new IntegerStat();
					stat.value = -1;
					properties[stat_name] = stat;
				}
			}
		}

		///<summary>
		/// Retrieves the current Object value of a stat property or null 
		/// if the stat wasn't found.
		///</summary>
		///<param name="stat_name"></param>
		///<returns></returns>
		public Object Get(String stat_name) {
			lock (this) {
				IntegerStat stat = (IntegerStat)properties[stat_name];
				if (stat != null) {
					return stat.value;
				}
				return null;
			}
		}

		///<summary>
		/// Sets the given stat name with the given value.
		///</summary>
		///<param name="value"></param>
		///<param name="stat_name"></param>
		public void Set(int value, String stat_name) {
			lock (this) {
				IntegerStat stat = (IntegerStat)properties[stat_name];
				if (stat != null) {
					stat.value = value;
				} else {
					stat = new IntegerStat();
					stat.value = value;
					properties[stat_name] = stat;
				}
			}
		}

		///<summary>
		/// Return a String array of all stat keys sorted in order from lowest to highest.
		///</summary>
		public string[] Keys {
			get {
				lock (this) {
					ICollection key_set = properties.Keys;

					String[] keys = new String[key_set.Count];
					int index = 0;
					IEnumerator it = key_set.GetEnumerator();
					while (it.MoveNext()) {
						keys[index] = (String) it.Current;
						++index;
					}

					// Sort the keys
					Array.Sort(keys, StringComparator);

					return keys;
				}
			}
		}

		/// <summary>
		/// Comparator for sorting the list of keys.
		/// </summary>
		private readonly static IComparer StringComparator = new StringComparatorImpl();

		internal class StringComparatorImpl : IComparer {
			public int Compare(Object ob1, Object ob2) {
				return ((String)ob1).CompareTo((String)ob2);
			}
		}



		///<summary>
		/// Returns a String representation of the stat with the given key name.
		///</summary>
		///<param name="key"></param>
		///<returns></returns>
		public String StatString(String key) {
			lock (this) {
				IntegerStat stat = (IntegerStat)properties[key];
				return stat.value.ToString();
			}
		}

		public override String ToString() {
			lock (this) {
				String[] keys = Keys;

				StringBuilder buf = new StringBuilder();
				for (int i = 0; i < keys.Length; ++i) {
					IntegerStat stat = (IntegerStat)properties[keys[i]];
					buf.Append(keys[i]);
					buf.Append(": ");
					buf.Append(stat.value);
					buf.Append('\n');
				}

				return buf.ToString();
			}
		}

		///<summary>
		/// Outputs the stats to a print stream.
		///</summary>
		///<param name="output"></param>
		public void PrintTo(TextWriter output) {
			lock (this) {
				String[] keys = Keys;

				for (int i = 0; i < keys.Length; ++i) {
					IntegerStat stat = (IntegerStat)properties[keys[i]];
					output.Write(keys[i]);
					output.Write(": ");
					output.WriteLine(stat.value);
				}
			}
		}

		// ---------- Inner class ----------

		private sealed class IntegerStat {
			internal long value;
		}
	}
}