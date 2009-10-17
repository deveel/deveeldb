//  
//  CommandLine.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
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

namespace Deveel.Data.Server {
	/// <summary>
	/// Used to parse a command-line.
	/// </summary>
	public class CommandLine {
		/// <summary>
		/// The command line arguments.
		/// </summary>
		private String[] args;

		/// <summary>
		/// Constructs the command line parser from the String[] array 
		/// passed as the argument to the application.
		/// </summary>
		/// <param name="args"></param>
		public CommandLine(String[] args) {
			if (args == null)
				args = new String[0];
			this.args = args;
		}

		/// <summary>
		/// Verifies if the command line arguments contain the given
		/// switch element.
		/// </summary>
		/// <param name="switch_str"></param>
		/// <returns>
		/// Returns true if the switch is in the command line.
		/// </returns>
		public bool containsSwitch(String switch_str) {
			for (int i = 0; i < args.Length; ++i) {
				if (args[i].Equals(switch_str)) {
					return true;
				}
			}
			return false;
		}

		/// <summary>
		/// Given a comma deliminated list, this scans for one of the switches in the list.
		/// </summary>
		/// <param name="switch_str"></param>
		/// <returns></returns>
		public bool containsSwitchFrom(String switch_str) {
			string[] tok = switch_str.Split(',');
			for (int i = 0; i < tok.Length; i++) {
				String elem = tok[i];
				if (containsSwitch(elem)) {
					return true;
				}
			}
			return false;
		}

		/// <summary>
		/// Verifies if the command line arguments contain a switch which
		/// starts with the given string.
		/// </summary>
		/// <param name="switch_str"></param>
		/// <returns>
		/// Returns true if the command line contains a switch starting with 
		/// the given string.
		/// </returns>
		public bool containsSwitchStart(String switch_str) {
			for (int i = 0; i < args.Length; ++i) {
				if (args[i].StartsWith(switch_str)) {
					return true;
				}
			}
			return false;
		}

		/// <summary>
		/// Gets all the switches in the command line arguments which start
		/// with the given string.
		/// </summary>
		/// <param name="switch_str"></param>
		/// <returns>
		/// Returns an array of <see cref="string">strings</see> of all switches on the 
		/// command line that start with the given string.
		/// </returns>
		public String[] allSwitchesStartingWith(String switch_str) {
			ArrayList list = new ArrayList();
			for (int i = 0; i < args.Length; ++i) {
				if (args[i].StartsWith(switch_str)) {
					list.Add(args[i]);
				}
			}
			return (String[])list.ToArray(typeof(string));
		}

		/// <summary>
		/// Returns the contents of a switch variable if the switch is found on the 
		/// command line.
		/// </summary>
		/// <param name="switch_str"></param>
		/// <remarks>
		/// A switch variable is of the form <c>-[switch] [variable]</c>.
		/// </remarks>
		/// <returns>
		/// Returns the <see cref="string"/> value of the given switch or <b>null</b> if 
		/// the argument was not found.
		/// </returns>
		public String switchArgument(String switch_str) {
			for (int i = 0; i < args.Length - 1; ++i) {
				if (args[i].Equals(switch_str)) {
					return args[i + 1];
				}
			}
			return null;
		}

		/// <summary>
		/// Returns the contents of a switch variable if the switch is found on the 
		/// command line.
		/// </summary>
		/// <param name="switch_str"></param>
		/// <param name="def"></param>
		/// <remarks>
		/// A switch variable is of the form <c>-[switch] [variable]</c>.
		/// </remarks>
		/// <returns>
		/// Returns the <see cref="string"/> value of the given switch or <paramref name="def"/> if 
		/// the argument was not found.
		/// </returns>
		public String switchArgument(String switch_str, String def) {
			String arg = switchArgument(switch_str);
			if (arg == null) {
				return def;
			}
			return arg;
		}

		/**
		 * Returns the contents of a set of arguments found after a switch on the
		 * command line.  For example, switchArguments("-create", 3) would try and
		 * find the '-create' switch and return the first 3 arguments after it if
		 * it can.
		 * <p>
		 * Returns null if no match is found.
		 */
		public String[] switchArguments(String switch_str, int arg_count) {
			for (int i = 0; i < args.Length - 1; ++i) {
				if (args[i].Equals(switch_str)) {
					if (i + arg_count < args.Length) {
						String[] ret_list = new String[arg_count];
						for (int n = 0; n < arg_count; ++n) {
							ret_list[n] = args[i + n + 1];
						}
						return ret_list;
					}
				}
			}
			return null;
		}

		public string [] switchArguments(string switch_str) {
			for (int i = 0; i < args.Length - 1; ++i) {
				if (args[i].Equals(switch_str)) {
					if (i < args.Length) {
						ArrayList ret_list = new ArrayList();
						for (int n = i + 1; n < args.Length; ++n) {
							ret_list[n] = args[i];
						}
						return (string[])ret_list.ToArray(typeof(string));
					}
				}
			}
			return null;
		}
	}
}